using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using ShuseiCommon.Common.MDB;

namespace ShuusseiCommon.Cache.M.Example
{
    #region 实体定义示例

    /// <summary>
    /// 用户实体 - 使用 string 类型主键
    /// </summary>
    [MongoCollection("users", DatabaseName = "example_db")]
    public class User
    {
        [MongoKey]
        public string UserId { get; set; } = null!;

        public string Name { get; set; } = null!;

        [MongoIndex(Unique = true)]
        public string Email { get; set; } = null!;

        public int Age { get; set; }

        [MongoIndex(Descending = true)]
        public DateTime CreatedAt { get; set; } = DateTime.Now;

        public List<string> Tags { get; set; } = new();

        public Address? Address { get; set; }
    }

    /// <summary>
    /// 嵌套对象
    /// </summary>
    public class Address
    {
        public string City { get; set; } = null!;
        public string Street { get; set; } = null!;
        public string ZipCode { get; set; } = null!;
    }

    /// <summary>
    /// 订单实体 - 使用 long 类型主键
    /// </summary>
    [MongoCollection("orders")]
    public class Order
    {
        [MongoKey]
        public long OrderId { get; set; }

        public string UserId { get; set; } = null!;

        [MongoIndex]
        public string Status { get; set; } = null!;

        public decimal TotalAmount { get; set; }

        public List<OrderItem> Items { get; set; } = new();

        [MongoIndex(Descending = true)]
        public DateTime OrderTime { get; set; } = DateTime.Now;
    }

    public class OrderItem
    {
        public string ProductId { get; set; } = null!;
        public string ProductName { get; set; } = null!;
        public int Quantity { get; set; }
        public decimal Price { get; set; }
    }

    /// <summary>
    /// 产品实体 - 使用 Guid 类型主键
    /// </summary>
    public class Product
    {
        [MongoKey]
        public Guid ProductId { get; set; } = Guid.NewGuid();

        public string Name { get; set; } = null!;

        [MongoIndex]
        public string Category { get; set; } = null!;

        public decimal Price { get; set; }

        public int Stock { get; set; }

        [BsonIgnore]  // 不存入 MongoDB
        public string TempData { get; set; } = null!;
    }

    /// <summary>
    /// 日志实体 - 使用 MongoDB ObjectId
    /// </summary>
    [MongoCollection("logs")]
    public class LogEntry
    {
        [MongoKey]
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = ObjectId.GenerateNewId().ToString();

        public string Level { get; set; } = null!;

        public string Message { get; set; } = null!;

        [MongoIndex(Descending = true)]
        public DateTime Timestamp { get; set; } = DateTime.Now;

        public Dictionary<string, object> Properties { get; set; } = new();
    }

    #endregion

    /// <summary>
    /// MongoDB CRUD 完整示例
    /// </summary>
    public class MongoExampleUsage
    {
        private readonly MCache _cache;

        public MongoExampleUsage(MCache cache)
        {
            _cache = cache;
        }

        #region Insert 插入

        /// <summary>
        /// 单条插入
        /// </summary>
        public async Task InsertSingleExample()
        {
            var user = new User
            {
                UserId = "U001",
                Name = "张三",
                Email = "zhangsan@example.com",
                Age = 28,
                Tags = new List<string> { "VIP", "Developer" },
                Address = new Address
                {
                    City = "上海",
                    Street = "南京路100号",
                    ZipCode = "200000"
                }
            };

            var result = await _cache.Insert(user);
            if (result.IsSuccess())
            {
                Console.WriteLine($"插入成功: {result.Data} 条");
            }
        }

        /// <summary>
        /// 批量插入
        /// </summary>
        public async Task InsertBatchExample()
        {
            var users = Enumerable.Range(1, 100).Select(i => new User
            {
                UserId = $"U{i:D4}",
                Name = $"用户{i}",
                Email = $"user{i}@example.com",
                Age = 20 + i % 30,
                CreatedAt = DateTime.Now.AddDays(-i)
            }).ToList();

            var result = await _cache.Insert(users);
            Console.WriteLine($"批量插入: {result.Data} 条");
        }

        #endregion

        #region Find 查询

        /// <summary>
        /// 根据主键查询
        /// </summary>
        public async Task FindByKeyExample()
        {
            // string 类型主键
            var userResult = await _cache.FindByKey<User, string>("U001");
            if (userResult.IsSuccess())
            {
                Console.WriteLine($"找到用户: {userResult.Data.Name}");
            }

            // long 类型主键
            var orderResult = await _cache.FindByKey<Order, long>(1001);

            // Guid 类型主键
            var productId = Guid.Parse("...");
            var productResult = await _cache.FindByKey<Product, Guid>(productId);
        }

        /// <summary>
        /// 条件查询 - Lambda 表达式
        /// </summary>
        public async Task FindByConditionExample()
        {
            // 简单条件
            var adults = await _cache.Find<User>(u => u.Age >= 18);

            // 多条件
            var result = await _cache.Find<User>(u =>
                u.Age >= 20 &&
                u.Age <= 30 &&
                u.Tags.Contains("VIP"));

            // 字符串匹配
            var shanghaiUsers = await _cache.Find<User>(u =>
                u.Address != null && u.Address.City == "上海");

            foreach (var user in result.Data ?? new List<User>())
            {
                Console.WriteLine($"{user.Name} - {user.Age}岁");
            }
        }

        /// <summary>
        /// 查询单条
        /// </summary>
        public async Task FindOneExample()
        {
            var result = await _cache.FindOne<User>(u => u.Email == "zhangsan@example.com");
            if (result.Data != null)
            {
                Console.WriteLine($"找到: {result.Data.Name}");
            }
        }

        /// <summary>
        /// 查询全部
        /// </summary>
        public async Task FindAllExample()
        {
            var allUsers = await _cache.Find<User>();
            Console.WriteLine($"总用户数: {allUsers.Data?.Count}");
        }

        /// <summary>
        /// 使用 FilterDefinition 高级查询
        /// </summary>
        public async Task FindWithFilterDefinitionExample()
        {
            var filter = Builders<User>.Filter.And(
                Builders<User>.Filter.Gte(u => u.Age, 20),
                Builders<User>.Filter.Regex(u => u.Name, new BsonRegularExpression("张")),
                Builders<User>.Filter.In(u => u.Address!.City, new[] { "上海", "北京" })
            );

            var result = await _cache.Find(filter);
        }

        /// <summary>
        /// 分页查询
        /// </summary>
        public async Task FindPageExample()
        {
            // 简单分页
            var page1 = await _cache.FindPage<User>(
                filter: u => u.Age >= 18,
                skip: 0,
                take: 10,
                sortField: "CreatedAt",
                descending: true
            );

            Console.WriteLine($"总数: {page1.TotalCount}, 当前页: {page1.Data?.Count}");

            // 第二页
            var page2 = await _cache.FindPage<User>(
                filter: u => u.Age >= 18,
                skip: 10,
                take: 10,
                sortField: "CreatedAt",
                descending: true
            );
        }

        /// <summary>
        /// 统计和存在性检查
        /// </summary>
        public async Task CountAndExistsExample()
        {
            // 统计数量
            var count = await _cache.Count<User>(u => u.Age >= 18);
            Console.WriteLine($"成年用户数: {count.Data}");

            // 检查是否存在 - 条件
            var exists = await _cache.Exists<User>(u => u.Email == "zhangsan@example.com");
            Console.WriteLine($"邮箱是否已注册: {exists.Data}");

            // 检查是否存在 - 主键
            var userExists = await _cache.ExistsByKey<User, string>("U001");
            Console.WriteLine($"用户U001是否存在: {userExists.Data}");
        }

        #endregion

        #region Update 更新

        /// <summary>
        /// 部分字段更新
        /// </summary>
        public async Task UpdatePartialExample()
        {
            // 更新单个字段
            var update = Builders<User>.Update.Set(u => u.Age, 30);
            var result = await _cache.Update<User>(u => u.UserId == "U001", update);

            // 更新多个字段
            var multiUpdate = Builders<User>.Update
                .Set(u => u.Name, "张三丰")
                .Set(u => u.Age, 100)
                .AddToSet(u => u.Tags, "Master");

            await _cache.Update<User>(u => u.UserId == "U001", multiUpdate);

            // 数值增减
            var incUpdate = Builders<User>.Update.Inc(u => u.Age, 1);
            await _cache.Update<User>(u => u.UserId == "U001", incUpdate);

            // 批量更新
            var batchUpdate = Builders<User>.Update.Set(u => u.Tags, new List<string> { "Updated" });
            await _cache.Update<User>(u => u.Age < 20, batchUpdate);
        }

        /// <summary>
        /// 整体替换
        /// </summary>
        public async Task ReplaceExample()
        {
            var newUser = new User
            {
                UserId = "U001",
                Name = "张三新",
                Email = "zhangsan_new@example.com",
                Age = 29
            };

            var result = await _cache.Replace<User>(u => u.UserId == "U001", newUser);
        }

        /// <summary>
        /// Upsert - 存在则更新，不存在则插入
        /// </summary>
        public async Task UpsertExample()
        {
            var user = new User
            {
                UserId = "U999",
                Name = "新用户",
                Email = "new@example.com",
                Age = 25
            };

            // 使用条件
            await _cache.Upsert<User>(u => u.UserId == "U999", user);

            // 使用 Save（根据主键自动 Upsert）
            await _cache.Save(user);
        }

        #endregion

        #region Delete 删除

        /// <summary>
        /// 根据主键删除
        /// </summary>
        public async Task DeleteByKeyExample()
        {
            // string 主键
            var result = await _cache.DeleteByKey<User, string>("U001");
            Console.WriteLine($"删除了 {result.Data?.DeletedCount} 条");

            // long 主键
            await _cache.DeleteByKey<Order, long>(1001);

            // Guid 主键
            await _cache.DeleteByKey<Product, Guid>(Guid.Parse("..."));
        }

        /// <summary>
        /// 条件删除
        /// </summary>
        public async Task DeleteByConditionExample()
        {
            // Lambda 条件
            var result = await _cache.Delete<User>(u => u.Age < 18);
            Console.WriteLine($"删除了 {result.Data?.DeletedCount} 条");

            // 删除7天前的日志
            var oneWeekAgo = DateTime.Now.AddDays(-7);
            await _cache.Delete<LogEntry>(l => l.Timestamp < oneWeekAgo);
        }

        /// <summary>
        /// 使用 FilterDefinition 删除
        /// </summary>
        public async Task DeleteWithFilterExample()
        {
            var filter = Builders<User>.Filter.And(
                Builders<User>.Filter.Eq(u => u.Tags, new List<string>()),
                Builders<User>.Filter.Lt(u => u.CreatedAt, DateTime.Now.AddYears(-1))
            );

            await _cache.Delete(filter);
        }

        #endregion

        #region Bulk 批量操作

        /// <summary>
        /// 批量 Save（根据主键 Upsert）
        /// </summary>
        public async Task BulkSaveExample()
        {
            var users = new List<User>
            {
                new() { UserId = "U001", Name = "用户1更新", Email = "u1@test.com", Age = 20 },
                new() { UserId = "U002", Name = "用户2更新", Email = "u2@test.com", Age = 21 },
                new() { UserId = "U003", Name = "用户3新增", Email = "u3@test.com", Age = 22 },
            };

            var result = await _cache.BulkSave(users);
            Console.WriteLine($"批量保存: {result.Data} 条");
        }

        /// <summary>
        /// 自定义批量 Upsert
        /// </summary>
        public async Task BulkUpsertExample()
        {
            var orders = new List<Order>
            {
                new() { OrderId = 1001, UserId = "U001", Status = "Pending", TotalAmount = 100 },
                new() { OrderId = 1002, UserId = "U002", Status = "Completed", TotalAmount = 200 },
            };

            var result = await _cache.BulkUpsert(
                orders,
                o => Builders<Order>.Filter.Eq(x => x.OrderId, o.OrderId)
            );
        }

        /// <summary>
        /// 复杂批量操作
        /// </summary>
        public async Task BulkWriteExample()
        {
            var requests = new List<WriteModel<User>>
            {
                new InsertOneModel<User>(new User { UserId = "NEW1", Name = "新用户1", Email = "new1@test.com", Age = 20 }),
                new UpdateOneModel<User>(
                    Builders<User>.Filter.Eq(u => u.UserId, "U001"),
                    Builders<User>.Update.Set(u => u.Name, "更新的名字")
                ),
                new DeleteOneModel<User>(Builders<User>.Filter.Eq(u => u.UserId, "U999")),
                new ReplaceOneModel<User>(
                    Builders<User>.Filter.Eq(u => u.UserId, "U002"),
                    new User { UserId = "U002", Name = "替换用户", Email = "replace@test.com", Age = 30 }
                )
            };

            var result = await _cache.BulkWrite(requests);
            Console.WriteLine($"插入: {result.Data?.InsertedCount}, 修改: {result.Data?.ModifiedCount}, 删除: {result.Data?.DeletedCount}");
        }

        #endregion

        #region Aggregate 聚合查询

        /// <summary>
        /// 聚合管道示例
        /// </summary>
        public async Task AggregateExample()
        {
            // 按城市统计用户数
            var pipeline = new BsonDocument[]
            {
                new("$match", new BsonDocument("Age", new BsonDocument("$gte", 18))),
                new("$group", new BsonDocument
                {
                    { "_id", "$Address.City" },
                    { "count", new BsonDocument("$sum", 1) },
                    { "avgAge", new BsonDocument("$avg", "$Age") }
                }),
                new("$sort", new BsonDocument("count", -1))
            };

            var result = await _cache.Aggregate<User, BsonDocument>(pipeline);
            foreach (var doc in result.Data ?? new List<BsonDocument>())
            {
                Console.WriteLine($"城市: {doc["_id"]}, 人数: {doc["count"]}, 平均年龄: {doc["avgAge"]}");
            }
        }

        #endregion

        #region Transaction 事务

        /// <summary>
        /// 事务示例（需要 MongoDB 副本集）
        /// </summary>
        public async Task TransactionExample()
        {
            var result = await _cache.ExecuteInTransaction(async session =>
            {
                // 事务内的操作需要使用 GetRawCollection
                var userCollection = _cache.GetRawCollection<User>();
                var orderCollection = _cache.GetRawCollection<Order>();

                // 创建订单
                var order = new Order
                {
                    OrderId = 2001,
                    UserId = "U001",
                    Status = "Pending",
                    TotalAmount = 500
                };
                await orderCollection.InsertOneAsync(session, order);

                // 更新用户标签
                await userCollection.UpdateOneAsync(
                    session,
                    Builders<User>.Filter.Eq(u => u.UserId, "U001"),
                    Builders<User>.Update.AddToSet(u => u.Tags, "HasOrder")
                );
            });

            if (result.IsSuccess())
            {
                Console.WriteLine("事务执行成功");
            }
        }

        /// <summary>
        /// 带返回值的事务
        /// </summary>
        public async Task TransactionWithResultExample()
        {
            var result = await _cache.ExecuteInTransaction<Order>(async session =>
            {
                var orderCollection = _cache.GetRawCollection<Order>();

                var order = new Order
                {
                    OrderId = 2002,
                    UserId = "U001",
                    Status = "Created",
                    TotalAmount = 1000
                };

                await orderCollection.InsertOneAsync(session, order);
                return order;
            });

            if (result.IsSuccess())
            {
                Console.WriteLine($"创建订单成功: {result.Data.OrderId}");
            }
        }

        #endregion

        #region Index 索引管理

        /// <summary>
        /// 手动创建索引
        /// </summary>
        public async Task CreateIndexExample()
        {
            // 单字段索引
            await _cache.CreateIndex<User>(u => u.Name);

            // 降序索引
            await _cache.CreateIndex<User>(u => u.CreatedAt, descending: true);

            // 唯一索引
            await _cache.CreateIndex<User>(u => u.Email, unique: true);

            // 复合索引
            var keys = Builders<User>.IndexKeys
                .Ascending(u => u.Age)
                .Descending(u => u.CreatedAt);
            await _cache.CreateCompoundIndex<User>(keys);
        }

        #endregion

        #region Raw Access 原生访问

        /// <summary>
        /// 获取原生 Collection 进行高级操作
        /// </summary>
        public async Task RawAccessExample()
        {
            var collection = _cache.GetRawCollection<User>();

            // 使用原生 API
            var cursor = await collection.FindAsync(
                Builders<User>.Filter.Empty,
                new FindOptions<User>
                {
                    BatchSize = 100,
                    NoCursorTimeout = true
                }
            );

            await cursor.ForEachAsync(user =>
            {
                Console.WriteLine(user.Name);
            });

            // 获取原生 Database
            var database = _cache.GetRawDatabase();
            var collections = await database.ListCollectionNamesAsync();
            await collections.ForEachAsync(name => Console.WriteLine($"Collection: {name}"));
        }

        #endregion
    }
}
