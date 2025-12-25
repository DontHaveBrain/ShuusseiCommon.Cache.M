namespace ShuseiCommon.Common.MDB
{
    /// <summary>
    /// 标记 MongoDB 主键字段
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class MongoKeyAttribute : Attribute
    {
        /// <summary>
        /// 是否自动创建索引（默认 true）
        /// </summary>
        public bool AutoIndex { get; set; } = true;

        /// <summary>
        /// 是否唯一索引（默认 true）
        /// </summary>
        public bool Unique { get; set; } = true;
    }

    /// <summary>
    /// 标记需要创建索引的字段
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class MongoIndexAttribute : Attribute
    {
        /// <summary>
        /// 是否降序（默认 false）
        /// </summary>
        public bool Descending { get; set; } = false;

        /// <summary>
        /// 是否唯一索引（默认 false）
        /// </summary>
        public bool Unique { get; set; } = false;

        /// <summary>
        /// 是否稀疏索引（默认 true）
        /// </summary>
        public bool Sparse { get; set; } = true;
    }

    /// <summary>
    /// 标记 MongoDB 集合名称
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    public class MongoCollectionAttribute : Attribute
    {
        public string CollectionName { get; }
        public string? DatabaseName { get; set; }

        public MongoCollectionAttribute(string collectionName)
        {
            CollectionName = collectionName;
        }
    }
}
