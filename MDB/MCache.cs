using MongoDB.Driver;
using ShuseiCommon.HelpCommon.DBHelp;
using ShuseiCommon.HTTP.DefineHttpInteraction;
using System.Linq.Expressions;

namespace ShuseiCommon.Common.MDB
{
    public interface MCache
    {
        // Find
        Task<Result<List<T>>> Find<T>(string? collectionName = null, string? dbName = null);
        Task<Result<List<T>>> Find<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null);
        Task<Result<List<T>>> Find<T>(FilterDefinition<T> filter, string? collectionName = null, string? dbName = null);
        Task<Result<T>> Find<T>(long reverseId, string? collectionName = null, string? dbName = null);
        Task<Result<T?>> FindOne<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null);

        /// <summary>
        /// 根据主键查询（需要实体类标记 [MongoKey]）
        /// </summary>
        Task<Result<T>> FindByKey<T, TKey>(TKey key, string? collectionName = null, string? dbName = null);

        // Page
        Task<PageResult<List<T>>> FindPage<T, TRequest>(Expression<Func<T, bool>> filter, PageRequest<TRequest>? pageRequest, string? collectionName = null, string? dbName = null) where TRequest : RequestBase, new();
        Task<PageResult<List<T>>> FindPage<T>(Expression<Func<T, bool>> filter, int skip, int take, string? sortField = null, bool descending = true, string? collectionName = null, string? dbName = null);

        // Insert
        Task<Result<int>> Insert<T>(T data, string? collectionName = null, string? dbName = null);
        Task<Result<int>> Insert<T>(List<T> data, string? collectionName = null, string? dbName = null);

        // Delete
        Task<Result<DeleteResult>> Delete<T>(FilterDefinition<T> filter, string? collectionName = null, string? dbName = null);
        Task<Result<DeleteResult>> Delete<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null);
        Task<Result<DeleteResult>> Delete<T>(long reverseId, string? collectionName = null, string? dbName = null);

        /// <summary>
        /// 根据主键删除（需要实体类标记 [MongoKey]）
        /// </summary>
        Task<Result<DeleteResult>> DeleteByKey<T, TKey>(TKey key, string? collectionName = null, string? dbName = null);

        // Update
        Task<Result<UpdateResult>> Update<T>(Expression<Func<T, bool>> filter, UpdateDefinition<T>? update, string? collectionName = null, string? dbName = null);
        Task<Result<ReplaceOneResult>> Replace<T>(Expression<Func<T, bool>> filter, T replacement, string? collectionName = null, string? dbName = null);
        Task<Result<ReplaceOneResult>> Upsert<T>(Expression<Func<T, bool>> filter, T replacement, string? collectionName = null, string? dbName = null);

        /// <summary>
        /// 根据主键 Upsert（需要实体类标记 [MongoKey]）
        /// </summary>
        Task<Result<ReplaceOneResult>> Save<T>(T entity, string? collectionName = null, string? dbName = null);

        // Count & Exists
        Task<Result<long>> Count<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null);
        Task<Result<bool>> Exists<T>(Expression<Func<T, bool>> filter, string? collectionName = null, string? dbName = null);

        /// <summary>
        /// 根据主键检查是否存在（需要实体类标记 [MongoKey]）
        /// </summary>
        Task<Result<bool>> ExistsByKey<T, TKey>(TKey key, string? collectionName = null, string? dbName = null);

        // Bulk
        Task<Result<BulkWriteResult<T>>> BulkWrite<T>(IEnumerable<WriteModel<T>> requests, string? collectionName = null, string? dbName = null);
        Task<Result<int>> BulkUpsert<T>(List<T> items, Func<T, FilterDefinition<T>> filterSelector, string? collectionName = null, string? dbName = null);

        /// <summary>
        /// 批量 Upsert（根据 [MongoKey] 标记的主键）
        /// </summary>
        Task<Result<int>> BulkSave<T>(List<T> items, string? collectionName = null, string? dbName = null);

        // Index
        Task<Result<string>> CreateIndex<T>(Expression<Func<T, object>> field, bool descending = false, bool unique = false, string? collectionName = null, string? dbName = null);
        Task<Result<string>> CreateCompoundIndex<T>(IndexKeysDefinition<T> keys, bool unique = false, string? collectionName = null, string? dbName = null);

        // Aggregate
        Task<Result<List<TResult>>> Aggregate<T, TResult>(PipelineDefinition<T, TResult> pipeline, string? collectionName = null, string? dbName = null);

        // Transaction
        Task<Result<bool>> ExecuteInTransaction(Func<IClientSessionHandle, Task> action);
        Task<Result<T>> ExecuteInTransaction<T>(Func<IClientSessionHandle, Task<T>> action);

        // Raw Access
        IMongoCollection<T> GetRawCollection<T>(string? collectionName = null, string? dbName = null);
        IMongoDatabase GetRawDatabase(string? dbName = null);
    }
}
