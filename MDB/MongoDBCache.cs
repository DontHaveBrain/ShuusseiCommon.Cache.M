using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
using ShuseiCommon.DependencyInjection;
using ShuseiCommon.HelpCommon.DBHelp;
using ShuseiCommon.HTTP.DefineHttpInteraction;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace ShuseiCommon.Common.MDB
{
    [Injection(type = typeof(MCache), serviceLifetime = ServiceLifetime.Singleton)]
    public class MongoDBCache : MCache
    {
        private readonly string _connStr;
        private readonly string _dbName;
        private readonly Lazy<MongoClient> _clientLazy;
        private readonly ConcurrentDictionary<string, IMongoDatabase> _dbCache = new();
        private readonly ConcurrentDictionary<string, object> _collectionCache = new();
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _indexLocks = new();
        private readonly ConcurrentDictionary<string, bool> _indexCache = new();
        private readonly ConcurrentDictionary<Type, MongoTypeMetadata> _metadataCache = new();

        // 可配置的批量操作大小
        private readonly int _batchSize;

        public MongoDBCache(IConfiguration configuration)
        {
            _connStr = configuration.GetConnectionString("mongoDB")!;
            _dbName = configuration.GetConnectionString("DBName")!;
            _clientLazy = new Lazy<MongoClient>(() => CreateClient(_connStr));
            _batchSize = configuration.GetValue("MongoDB:BatchSize", 1000);
        }

        public MongoDBCache(string connectionString, string dbName, int batchSize = 1000)
        {
            _connStr = connectionString;
            _dbName = dbName;
            _clientLazy = new Lazy<MongoClient>(() => CreateClient(_connStr));
            _batchSize = batchSize;
        }

        private static MongoClient CreateClient(string connStr)
        {
            var settings = MongoClientSettings.FromConnectionString(connStr);
            settings.MaxConnectionPoolSize = 200;
            settings.MinConnectionPoolSize = 10;
            settings.WaitQueueTimeout = TimeSpan.FromSeconds(30);
            settings.ConnectTimeout = TimeSpan.FromSeconds(10);
            settings.ServerSelectionTimeout = TimeSpan.FromSeconds(10);
            settings.SocketTimeout = TimeSpan.FromSeconds(30);
            settings.MaxConnectionIdleTime = TimeSpan.FromMinutes(10);
            return new MongoClient(settings);
        }

        private MongoClient Client => _clientLazy.Value;

        #region Metadata
        private MongoTypeMetadata GetMetadata<T>()
        {
            return _metadataCache.GetOrAdd(typeof(T), type =>
            {
                var metadata = new MongoTypeMetadata();

                // 解析 MongoCollectionAttribute
                var collectionAttr = type.GetCustomAttribute<MongoCollectionAttribute>();
                if (collectionAttr != null)
                {
                    metadata.CollectionName = collectionAttr.CollectionName;
                    metadata.DatabaseName = collectionAttr.DatabaseName;
                }

                // 解析属性特性
                foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    // MongoKeyAttribute
                    var keyAttr = prop.GetCustomAttribute<MongoKeyAttribute>();
                    if (keyAttr != null)
                    {
                        metadata.KeyProperty = prop;
                        metadata.KeyAttribute = keyAttr;
                    }

                    // MongoIndexAttribute
                    var indexAttr = prop.GetCustomAttribute<MongoIndexAttribute>();
                    if (indexAttr != null)
                    {
                        metadata.IndexedProperties.Add((prop, indexAttr));
                    }
                }

                return metadata;
            });
        }

        private class MongoTypeMetadata
        {
            public string? CollectionName { get; set; }
            public string? DatabaseName { get; set; }
            public PropertyInfo? KeyProperty { get; set; }
            public MongoKeyAttribute? KeyAttribute { get; set; }
            public List<(PropertyInfo Property, MongoIndexAttribute Attribute)> IndexedProperties { get; } = new();
        }
        #endregion

        #region Collection & Index
        private IMongoDatabase GetDatabase(string? dbName = null)
        {
            var name = dbName ?? _dbName;
            return _dbCache.GetOrAdd(name, n => Client.GetDatabase(n));
        }

        private IMongoCollection<T> GetCollection<T>(string? dbName = null, string? collectionName = null)
        {
            var metadata = GetMetadata<T>();
            var db = dbName ?? metadata.DatabaseName ?? _dbName;
            var col = collectionName ?? metadata.CollectionName ?? typeof(T).Name;
            var key = $"{db}.{col}";

            return (IMongoCollection<T>)_collectionCache.GetOrAdd(key, _ =>
            {
                return GetDatabase(db).GetCollection<T>(col);
            });
        }

        private async Task<IMongoCollection<T>> GetCollectionAsync<T>(string? dbName = null, string? collectionName = null)
        {
            var metadata = GetMetadata<T>();
            var db = dbName ?? metadata.DatabaseName ?? _dbName;
            var col = collectionName ?? metadata.CollectionName ?? typeof(T).Name;
            var key = $"{db}.{col}";

            var collection = (IMongoCollection<T>)_collectionCache.GetOrAdd(key, _ =>
            {
                return GetDatabase(db).GetCollection<T>(col);
            });

            await EnsureIndexesAsync(collection, key, metadata);
            return collection;
        }

        private async Task EnsureIndexesAsync<T>(IMongoCollection<T> collection, string cacheKey, MongoTypeMetadata metadata)
        {
            if (_indexCache.ContainsKey(cacheKey)) return;

            // 使用 SemaphoreSlim 避免并发创建索引
            var indexLock = _indexLocks.GetOrAdd(cacheKey, _ => new SemaphoreSlim(1, 1));
            await indexLock.WaitAsync();
            try
            {
                if (_indexCache.ContainsKey(cacheKey)) return;

                var indexes = new List<CreateIndexModel<T>>();

                // 主键索引
                if (metadata.KeyProperty != null && metadata.KeyAttribute?.AutoIndex == true)
                {
                    indexes.Add(new CreateIndexModel<T>(
                        Builders<T>.IndexKeys.Ascending(metadata.KeyProperty.Name),
                        new CreateIndexOptions
                        {
                            Background = true,
                            Unique = metadata.KeyAttribute.Unique,
                            Sparse = true
                        }));
                }

                // 标记了 MongoIndexAttribute 的字段
                foreach (var (prop, attr) in metadata.IndexedProperties)
                {
                    var keys = attr.Descending
                        ? Builders<T>.IndexKeys.Descending(prop.Name)
                        : Builders<T>.IndexKeys.Ascending(prop.Name);

                    indexes.Add(new CreateIndexModel<T>(keys, new CreateIndexOptions
                    {
                        Background = true,
                        Unique = attr.Unique,
                        Sparse = attr.Sparse
                    }));
                }

                if (indexes.Count == 0)
                {
                    _indexCache[cacheKey] = true;
                    return;
                }

                await collection.Indexes.CreateManyAsync(indexes);
                _indexCache[cacheKey] = true;
            }
            catch (MongoCommandException)
            {
                // 索引已存在，标记为完成
                _indexCache[cacheKey] = true;
            }
            finally
            {
                indexLock.Release();
            }
        }
        #endregion

        #region Key Helpers
        private string GetKeyFieldName<T>()
        {
            var metadata = GetMetadata<T>();
            return metadata.KeyProperty?.Name ?? throw new InvalidOperationException($"Type {typeof(T).Name} has no [MongoKey] attribute");
        }

        private object? GetKeyValue<T>(T entity)
        {
            var metadata = GetMetadata<T>();
            return metadata.KeyProperty?.GetValue(entity);
        }

        private FilterDefinition<T> BuildKeyFilter<T, TKey>(TKey key)
        {
            var keyField = GetKeyFieldName<T>();
            return Builders<T>.Filter.Eq(keyField, key);
        }
        #endregion

        #region Insert
        public async Task<Result<int>> Insert<T>(T data, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                await collection.InsertOneAsync(data);
                return Result<int>.Success(1);
            }
            catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
            {
                return Result<int>.Fail($"Insert failed: Duplicate key - {ex.WriteError.Message}");
            }
            catch (MongoConnectionException ex)
            {
                return Result<int>.Fail($"Insert failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<int>.Fail($"Insert failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<int>.Fail($"Insert failed: {ex.Message}");
            }
        }

        public async Task<Result<int>> Insert<T>(List<T> data, string? collectionName = null, string? dbName = null)
        {
            if (data == null || data.Count == 0)
                return Result<int>.Success(0);

            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);

                if (data.Count <= _batchSize)
                {
                    await collection.InsertManyAsync(data, new InsertManyOptions { IsOrdered = false });
                }
                else
                {
                    var tasks = new List<Task>();
                    for (int i = 0; i < data.Count; i += _batchSize)
                    {
                        var batch = data.Skip(i).Take(_batchSize).ToList();
                        tasks.Add(collection.InsertManyAsync(batch, new InsertManyOptions { IsOrdered = false }));
                    }
                    await Task.WhenAll(tasks);
                }

                return Result<int>.Success(data.Count);
            }
            catch (MongoBulkWriteException ex)
            {
                return Result<int>.Fail($"Batch insert failed: {ex.WriteErrors.Count} errors - {ex.Message}");
            }
            catch (MongoConnectionException ex)
            {
                return Result<int>.Fail($"Batch insert failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<int>.Fail($"Batch insert failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<int>.Fail($"Batch insert failed: {ex.Message}");
            }
        }
        #endregion

        #region Delete
        public async Task<Result<DeleteResult>> Delete<T>(FilterDefinition<T> filter, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.DeleteManyAsync(filter);
                return Result<DeleteResult>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: {ex.Message}");
            }
        }

        public async Task<Result<DeleteResult>> DeleteByKey<T, TKey>(TKey key, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var filter = BuildKeyFilter<T, TKey>(key);
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.DeleteOneAsync(filter);
                return Result<DeleteResult>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: {ex.Message}");
            }
        }

        public async Task<Result<DeleteResult>> Delete<T>(long reverseId, string? collectionName = null, string? dbName = null)
            => await DeleteByKey<T, long>(reverseId, collectionName, dbName);

        public async Task<Result<DeleteResult>> Delete<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.DeleteManyAsync(filter);
                return Result<DeleteResult>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<DeleteResult>.Fail($"Delete failed: {ex.Message}");
            }
        }
        #endregion

        #region Update
        public async Task<Result<UpdateResult>> Update<T>(Expression<Func<T, bool>> filter, UpdateDefinition<T>? update, string? collectionName = null, string? dbName = null)
        {
            if (update == null)
                return Result<UpdateResult>.Fail("Update definition is null");

            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.UpdateManyAsync(filter, update);
                return Result<UpdateResult>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<UpdateResult>.Fail($"Update failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<UpdateResult>.Fail($"Update failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<UpdateResult>.Fail($"Update failed: {ex.Message}");
            }
        }

        public async Task<Result<ReplaceOneResult>> Replace<T>(Expression<Func<T, bool>> filter, T replacement, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.ReplaceOneAsync(filter, replacement, new ReplaceOptions { IsUpsert = false });
                return Result<ReplaceOneResult>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<ReplaceOneResult>.Fail($"Replace failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<ReplaceOneResult>.Fail($"Replace failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<ReplaceOneResult>.Fail($"Replace failed: {ex.Message}");
            }
        }

        public async Task<Result<ReplaceOneResult>> Upsert<T>(Expression<Func<T, bool>> filter, T replacement, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.ReplaceOneAsync(filter, replacement, new ReplaceOptions { IsUpsert = true });
                return Result<ReplaceOneResult>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<ReplaceOneResult>.Fail($"Upsert failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<ReplaceOneResult>.Fail($"Upsert failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<ReplaceOneResult>.Fail($"Upsert failed: {ex.Message}");
            }
        }

        /// <summary>
        /// 根据主键 Upsert（需要实体类标记 [MongoKey]）
        /// </summary>
        public async Task<Result<ReplaceOneResult>> Save<T>(T entity, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var keyValue = GetKeyValue(entity);
                if (keyValue == null)
                    return Result<ReplaceOneResult>.Fail("Key value is null");

                var keyField = GetKeyFieldName<T>();
                var filter = Builders<T>.Filter.Eq(keyField, keyValue);
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.ReplaceOneAsync(filter, entity, new ReplaceOptions { IsUpsert = true });
                return Result<ReplaceOneResult>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<ReplaceOneResult>.Fail($"Save failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<ReplaceOneResult>.Fail($"Save failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<ReplaceOneResult>.Fail($"Save failed: {ex.Message}");
            }
        }
        #endregion

        #region Find
        public async Task<Result<T>> FindByKey<T, TKey>(TKey key, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var filter = BuildKeyFilter<T, TKey>(key);
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .Find(filter)
                    .Limit(1)
                    .FirstOrDefaultAsync();

                return result != null
                    ? Result<T>.Success(result)
                    : Result<T>.Fail($"Document with key {key} not found");
            }
            catch (MongoConnectionException ex)
            {
                return Result<T>.Fail($"Find failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<T>.Fail($"Find failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<T>.Fail($"Find failed: {ex.Message}");
            }
        }

        public async Task<Result<T>> Find<T>(long reverseId, string? collectionName = null, string? dbName = null)
            => await FindByKey<T, long>(reverseId, collectionName, dbName);

        public async Task<Result<List<T>>> Find<T>(string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .Find(FilterDefinition<T>.Empty)
                    .ToListAsync();
                return Result<List<T>>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<List<T>>.Fail($"Find failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<List<T>>.Fail($"Find failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<List<T>>.Fail($"Find failed: {ex.Message}");
            }
        }

        public async Task<Result<List<T>>> Find<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .Find(filter)
                    .ToListAsync();
                return Result<List<T>>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<List<T>>.Fail($"Find failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<List<T>>.Fail($"Find failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<List<T>>.Fail($"Find failed: {ex.Message}");
            }
        }

        public async Task<Result<List<T>>> Find<T>(FilterDefinition<T> filter, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .Find(filter)
                    .ToListAsync();
                return Result<List<T>>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<List<T>>.Fail($"Find failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<List<T>>.Fail($"Find failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<List<T>>.Fail($"Find failed: {ex.Message}");
            }
        }

        public async Task<Result<T?>> FindOne<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .Find(filter)
                    .Limit(1)
                    .FirstOrDefaultAsync();
                return Result<T?>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<T?>.Fail($"FindOne failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<T?>.Fail($"FindOne failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<T?>.Fail($"FindOne failed: {ex.Message}");
            }
        }

        public async Task<Result<long>> Count<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var count = await collection.CountDocumentsAsync(filter);
                return Result<long>.Success(count);
            }
            catch (MongoConnectionException ex)
            {
                return Result<long>.Fail($"Count failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<long>.Fail($"Count failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<long>.Fail($"Count failed: {ex.Message}");
            }
        }

        public async Task<Result<bool>> Exists<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var exists = await collection
                    .Find(filter)
                    .Limit(1)
                    .AnyAsync();
                return Result<bool>.Success(exists);
            }
            catch (MongoConnectionException ex)
            {
                return Result<bool>.Fail($"Exists check failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<bool>.Fail($"Exists check failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<bool>.Fail($"Exists check failed: {ex.Message}");
            }
        }

        public async Task<Result<bool>> ExistsByKey<T, TKey>(TKey key, string? collectionName = null, string? dbName = null)
        {
            try
            {
                var filter = BuildKeyFilter<T, TKey>(key);
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var exists = await collection
                    .Find(filter)
                    .Limit(1)
                    .AnyAsync();
                return Result<bool>.Success(exists);
            }
            catch (MongoConnectionException ex)
            {
                return Result<bool>.Fail($"Exists check failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<bool>.Fail($"Exists check failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<bool>.Fail($"Exists check failed: {ex.Message}");
            }
        }
        #endregion

        #region FindPage
        public async Task<PageResult<List<T>>> FindPage<T, TRequest>(
            Expression<Func<T, bool>> filter,
            PageRequest<TRequest>? pageRequest,
            string? collectionName = null,
            string? dbName = null) where TRequest : RequestBase, new()
        {
            if (pageRequest == null)
            {
                var data = await Find(filter, collectionName, dbName);
                return new PageResult<List<T>>(data);
            }

            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var filterBuilder = Builders<T>.Filter;
                var combinedFilter = filterBuilder.Where(filter);

                if (pageRequest.Request != null)
                {
                    if (pageRequest.Request.MinCreationTime.HasValue)
                        combinedFilter &= filterBuilder.Gte("CreationTime", pageRequest.Request.MinCreationTime.Value);
                    if (pageRequest.Request.MaxCreationTime.HasValue)
                        combinedFilter &= filterBuilder.Lte("CreationTime", pageRequest.Request.MaxCreationTime.Value);
                }

                if (!string.IsNullOrWhiteSpace(pageRequest.FilterText))
                    combinedFilter &= filterBuilder.Text(pageRequest.FilterText);

                var countTask = collection.CountDocumentsAsync(combinedFilter);
                var findFluent = collection.Find(combinedFilter);

                if (!string.IsNullOrWhiteSpace(pageRequest.Sorting))
                {
                    var parts = pageRequest.Sorting.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    var field = parts[0];
                    var desc = parts.Length > 1 && parts[1].Equals("DESC", StringComparison.OrdinalIgnoreCase);
                    findFluent = desc
                        ? findFluent.Sort(Builders<T>.Sort.Descending(field))
                        : findFluent.Sort(Builders<T>.Sort.Ascending(field));
                }

                var dataTask = findFluent
                    .Skip(pageRequest.SkipCount)
                    .Limit(pageRequest.MaxResultCount)
                    .ToListAsync();

                await Task.WhenAll(countTask, dataTask);

                return new PageResult<List<T>>((int)countTask.Result, dataTask.Result);
            }
            catch (MongoConnectionException ex)
            {
                return PageResult<List<T>>.Fail($"FindPage failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return PageResult<List<T>>.Fail($"FindPage failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return PageResult<List<T>>.Fail($"FindPage failed: {ex.Message}");
            }
        }

        public async Task<PageResult<List<T>>> FindPage<T>(
            Expression<Func<T, bool>> filter,
            int skip,
            int take,
            string? sortField = null,
            bool descending = true,
            string? collectionName = null,
            string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);

                var countTask = collection.CountDocumentsAsync(filter);
                var findFluent = collection.Find(filter);

                if (!string.IsNullOrEmpty(sortField))
                {
                    findFluent = descending
                        ? findFluent.Sort(Builders<T>.Sort.Descending(sortField))
                        : findFluent.Sort(Builders<T>.Sort.Ascending(sortField));
                }

                var dataTask = findFluent.Skip(skip).Limit(take).ToListAsync();

                await Task.WhenAll(countTask, dataTask);

                return new PageResult<List<T>>((int)countTask.Result, dataTask.Result);
            }
            catch (MongoConnectionException ex)
            {
                return PageResult<List<T>>.Fail($"FindPage failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return PageResult<List<T>>.Fail($"FindPage failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return PageResult<List<T>>.Fail($"FindPage failed: {ex.Message}");
            }
        }
        #endregion

        #region Aggregate
        public async Task<Result<List<TResult>>> Aggregate<T, TResult>(
            PipelineDefinition<T, TResult> pipeline,
            string? collectionName = null,
            string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .Aggregate(pipeline)
                    .ToListAsync();
                return Result<List<TResult>>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                return Result<List<TResult>>.Fail($"Aggregate failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<List<TResult>>.Fail($"Aggregate failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<List<TResult>>.Fail($"Aggregate failed: {ex.Message}");
            }
        }
        #endregion

        #region Bulk Operations
        public async Task<Result<BulkWriteResult<T>>> BulkWrite<T>(
            IEnumerable<WriteModel<T>> requests,
            string? collectionName = null,
            string? dbName = null)
        {
            try
            {
                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .BulkWriteAsync(requests, new BulkWriteOptions { IsOrdered = false });
                return Result<BulkWriteResult<T>>.Success(result);
            }
            catch (MongoBulkWriteException ex)
            {
                return Result<BulkWriteResult<T>>.Fail($"BulkWrite failed: {ex.WriteErrors.Count} errors - {ex.Message}");
            }
            catch (MongoConnectionException ex)
            {
                return Result<BulkWriteResult<T>>.Fail($"BulkWrite failed: Connection error - {ex.Message}");
            }
            catch (TimeoutException ex)
            {
                return Result<BulkWriteResult<T>>.Fail($"BulkWrite failed: Timeout - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<BulkWriteResult<T>>.Fail($"BulkWrite failed: {ex.Message}");
            }
        }

        public async Task<Result<int>> BulkUpsert<T>(
            List<T> items,
            Func<T, FilterDefinition<T>> filterSelector,
            string? collectionName = null,
            string? dbName = null)
        {
            if (items == null || items.Count == 0)
                return Result<int>.Success(0);

            try
            {
                var requests = items.Select(item =>
                    new ReplaceOneModel<T>(filterSelector(item), item) { IsUpsert = true });

                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .BulkWriteAsync(requests, new BulkWriteOptions { IsOrdered = false });

                return Result<int>.Success((int)(result.InsertedCount + result.ModifiedCount + result.Upserts.Count));
            }
            catch (MongoBulkWriteException ex)
            {
                return Result<int>.Fail($"BulkUpsert failed: {ex.WriteErrors.Count} errors - {ex.Message}");
            }
            catch (MongoConnectionException ex)
            {
                return Result<int>.Fail($"BulkUpsert failed: Connection error - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<int>.Fail($"BulkUpsert failed: {ex.Message}");
            }
        }

        /// <summary>
        /// 批量 Upsert（根据 [MongoKey] 标记的主键）
        /// </summary>
        public async Task<Result<int>> BulkSave<T>(List<T> items, string? collectionName = null, string? dbName = null)
        {
            if (items == null || items.Count == 0)
                return Result<int>.Success(0);

            try
            {
                var keyField = GetKeyFieldName<T>();
                var metadata = GetMetadata<T>();
                var keyProp = metadata.KeyProperty!;

                var requests = items.Select(item =>
                {
                    var keyValue = keyProp.GetValue(item);
                    var filter = Builders<T>.Filter.Eq(keyField, keyValue);
                    return new ReplaceOneModel<T>(filter, item) { IsUpsert = true };
                });

                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection
                    .BulkWriteAsync(requests, new BulkWriteOptions { IsOrdered = false });

                return Result<int>.Success((int)(result.InsertedCount + result.ModifiedCount + result.Upserts.Count));
            }
            catch (MongoBulkWriteException ex)
            {
                return Result<int>.Fail($"BulkSave failed: {ex.WriteErrors.Count} errors - {ex.Message}");
            }
            catch (MongoConnectionException ex)
            {
                return Result<int>.Fail($"BulkSave failed: Connection error - {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<int>.Fail($"BulkSave failed: {ex.Message}");
            }
        }
        #endregion

        #region Index Management
        public async Task<Result<string>> CreateIndex<T>(
            Expression<Func<T, object>> field,
            bool descending = false,
            bool unique = false,
            string? collectionName = null,
            string? dbName = null)
        {
            try
            {
                var keys = descending
                    ? Builders<T>.IndexKeys.Descending(field)
                    : Builders<T>.IndexKeys.Ascending(field);

                var options = new CreateIndexOptions { Background = true, Unique = unique };
                var model = new CreateIndexModel<T>(keys, options);

                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.Indexes.CreateOneAsync(model);
                return Result<string>.Success(result);
            }
            catch (MongoCommandException ex)
            {
                return Result<string>.Fail($"CreateIndex failed: {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<string>.Fail($"CreateIndex failed: {ex.Message}");
            }
        }

        public async Task<Result<string>> CreateCompoundIndex<T>(
            IndexKeysDefinition<T> keys,
            bool unique = false,
            string? collectionName = null,
            string? dbName = null)
        {
            try
            {
                var options = new CreateIndexOptions { Background = true, Unique = unique };
                var model = new CreateIndexModel<T>(keys, options);

                var collection = await GetCollectionAsync<T>(dbName, collectionName);
                var result = await collection.Indexes.CreateOneAsync(model);
                return Result<string>.Success(result);
            }
            catch (MongoCommandException ex)
            {
                return Result<string>.Fail($"CreateCompoundIndex failed: {ex.Message}");
            }
            catch (Exception ex)
            {
                return Result<string>.Fail($"CreateCompoundIndex failed: {ex.Message}");
            }
        }
        #endregion

        #region Transaction
        public Task<Result<bool>> ExecuteInTransaction(Func<IClientSessionHandle, Task> action)
            => ExecuteInTransaction(action, null);

        public async Task<Result<bool>> ExecuteInTransaction(
            Func<IClientSessionHandle, Task> action,
            TransactionOptions? options)
        {
            using var session = await Client.StartSessionAsync();
            try
            {
                session.StartTransaction(options);
                await action(session);
                await session.CommitTransactionAsync();
                return Result<bool>.Success(true);
            }
            catch (MongoConnectionException ex)
            {
                await session.AbortTransactionAsync();
                return Result<bool>.Fail($"Transaction failed: Connection error - {ex.Message}");
            }
            catch (MongoCommandException ex)
            {
                await session.AbortTransactionAsync();
                return Result<bool>.Fail($"Transaction failed: Command error - {ex.Message}");
            }
            catch (Exception ex)
            {
                await session.AbortTransactionAsync();
                return Result<bool>.Fail($"Transaction failed: {ex.Message}");
            }
        }

        public Task<Result<T>> ExecuteInTransaction<T>(Func<IClientSessionHandle, Task<T>> action)
            => ExecuteInTransaction(action, null);

        public async Task<Result<T>> ExecuteInTransaction<T>(
            Func<IClientSessionHandle, Task<T>> action,
            TransactionOptions? options)
        {
            using var session = await Client.StartSessionAsync();
            try
            {
                session.StartTransaction(options);
                var result = await action(session);
                await session.CommitTransactionAsync();
                return Result<T>.Success(result);
            }
            catch (MongoConnectionException ex)
            {
                await session.AbortTransactionAsync();
                return Result<T>.Fail($"Transaction failed: Connection error - {ex.Message}");
            }
            catch (MongoCommandException ex)
            {
                await session.AbortTransactionAsync();
                return Result<T>.Fail($"Transaction failed: Command error - {ex.Message}");
            }
            catch (Exception ex)
            {
                await session.AbortTransactionAsync();
                return Result<T>.Fail($"Transaction failed: {ex.Message}");
            }
        }
        #endregion

        #region Utilities
        public IMongoCollection<T> GetRawCollection<T>(string? collectionName = null, string? dbName = null)
            => GetCollection<T>(dbName, collectionName);

        public IMongoDatabase GetRawDatabase(string? dbName = null)
            => GetDatabase(dbName);
        #endregion
    }
}
