using Adi.Cap.SqlServer.Es.Envelope;

namespace Adi.Cap.SqlServer.Es.Storage;

/// <summary>
/// CAP 消息正文 ES 读写服务。
/// </summary>
public interface ICapElasticMessageStore
{
    /// <summary>
    /// 写入正文；返回 indexName（用于 envelope）。失败抛异常（fail-fast）。
    /// </summary>
    /// <param name="capMessageId">CAP 消息 Id。</param>
    /// <param name="topicName">CAP Topic 名称。</param>
    /// <param name="fullContent">序列化后的完整 Content。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>写入的 ES 索引名。</returns>
    Task<string> WriteAsync(
        string capMessageId,
        string topicName,
        string fullContent,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 按引用读取正文；不存在或失败抛异常。
    /// </summary>
    /// <param name="reference">ES 引用。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>完整 Content 字符串。</returns>
    Task<string> ReadAsync(
        CapEsReference reference,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 按引用删除；文档不存在视为成功。
    /// </summary>
    /// <param name="reference">ES 引用。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    Task DeleteAsync(
        CapEsReference reference,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量删除；单条失败记录 Warning 并继续。
    /// </summary>
    /// <param name="references">ES 引用列表。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>成功删除的文档数。</returns>
    Task<int> DeleteManyAsync(
        IReadOnlyList<CapEsReference> references,
        CancellationToken cancellationToken = default);
}
