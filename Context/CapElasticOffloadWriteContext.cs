namespace Adi.Cap.SqlServer.Es.Context;

/// <summary>
/// ES 外置写入上下文：标记当前 CAP 持久化操作所属 Topic 与消息 Id，供序列化器在出入口替换 Content。
/// </summary>
internal static class CapElasticOffloadWriteContext
{
    private static readonly AsyncLocal<WriteScope?> CurrentScope = new();

    /// <summary>
    /// 当前异步上下文中的写入作用域。
    /// </summary>
    internal static WriteScope? Current => CurrentScope.Value;

    /// <summary>
    /// 进入 ES 外置写入作用域；Dispose 时恢复外层作用域。
    /// </summary>
    /// <param name="topicName">CAP Topic 名称。</param>
    /// <param name="capMessageId">可选，显式指定 CAP 消息 Id（状态变更时与 DbId 对齐）。</param>
    internal static IDisposable Enter(string topicName, string? capMessageId = null)
    {
        var previous = CurrentScope.Value;
        CurrentScope.Value = new WriteScope(topicName, capMessageId);
        return new ScopeDisposable(previous);
    }

    /// <summary>
    /// 写入作用域数据。
    /// </summary>
    /// <param name="TopicName">CAP Topic 名称。</param>
    /// <param name="CapMessageId">CAP 消息 Id；Received 入库时由 Snowflake 装饰器填充。</param>
    internal sealed class WriteScope(string topicName, string? capMessageId)
    {
        /// <summary>
        /// CAP Topic 名称。
        /// </summary>
        internal string TopicName { get; } = topicName;

        /// <summary>
        /// CAP 消息 Id，与 DB Id 及 ES 文档 _id 一致。
        /// </summary>
        internal string? CapMessageId { get; set; } = capMessageId;
    }

    /// <summary>
    /// 作用域恢复器。
    /// </summary>
    private sealed class ScopeDisposable(WriteScope? previous) : IDisposable
    {
        /// <inheritdoc />
        public void Dispose()
        {
            CurrentScope.Value = previous;
        }
    }
}
