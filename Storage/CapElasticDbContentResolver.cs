using Adi.Cap.SqlServer.Es.Envelope;
using Microsoft.Extensions.Logging;

namespace Adi.Cap.SqlServer.Es.Storage;

/// <summary>
/// DB Content 列与 ES 正文之间的还原工具（用于不经过 ISerializer 的路径，如 Dashboard 列表）。
/// </summary>
internal static class CapElasticDbContentResolver
{
    /// <summary>
    /// 将 DB Content 还原为 ES 正文；非 envelope 或读取失败时原样返回。
    /// </summary>
    /// <param name="dbContent">数据库 Content 列。</param>
    /// <param name="elasticStore">ES 正文读写。</param>
    /// <param name="logger">日志。</param>
    /// <param name="logCategory">日志分类描述。</param>
    internal static async Task<string?> ResolveAsync(
        string? dbContent,
        ICapElasticMessageStore elasticStore,
        ILogger logger,
        string logCategory)
    {
        if (!CapEsContentEnvelope.TryParse(dbContent, CapElasticMessageDocument.EnvelopeIndexPrefix, out var reference))
        {
            return dbContent;
        }

        try
        {
            return await elasticStore.ReadAsync(reference).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "{LogCategory} ES 还原失败 CapMessageId={CapMessageId} IndexName={IndexName}",
                logCategory,
                reference.CapMessageId,
                reference.IndexName);
            return dbContent;
        }
    }

    /// <summary>
    /// 解析 envelope 并删除 ES 文档；非 envelope 时忽略。
    /// </summary>
    /// <param name="dbContent">数据库 Content 列。</param>
    /// <param name="elasticStore">ES 正文读写。</param>
    internal static async Task TryDeleteAsync(string? dbContent, ICapElasticMessageStore elasticStore)
    {
        if (!CapEsContentEnvelope.TryParse(dbContent, CapElasticMessageDocument.EnvelopeIndexPrefix, out var reference))
        {
            return;
        }

        await elasticStore.DeleteAsync(reference).ConfigureAwait(false);
    }
}
