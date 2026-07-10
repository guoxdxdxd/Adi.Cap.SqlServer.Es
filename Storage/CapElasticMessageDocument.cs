using Adi.ElasticSugar.Core.Index;
using Adi.ElasticSugar.Core.Models;

namespace Adi.Cap.SqlServer.Es.Storage;

/// <summary>
/// CAP 外置 ES 文档模型；索引名由 <see cref="EsIndexAttribute"/> 与 <see cref="EsDateTime"/> 生成。
/// </summary>
[EsIndex(CapElasticMessageDocument.IndexPrefix, IndexFormat.YearMonth)]
public sealed class CapElasticMessageDocument : BaseEsModel
{
    /// <summary>
    /// ES 索引前缀（年月后缀由 ElasticSugar 生成，形如 cap-message-2026-06）。
    /// </summary>
    public const string IndexPrefix = "cap-message";

    /// <summary>
    /// envelope 解析时用于校验 indexName 前缀（含尾部连字符）。
    /// </summary>
    public const string EnvelopeIndexPrefix = IndexPrefix + "-";

    /// <summary>
    /// CAP Topic / message name。
    /// </summary>
    public string TopicName { get; set; } = string.Empty;

    /// <summary>
    /// CAP 原 DB Content 全文（序列化后）。
    /// 仅写入 _source、不参与索引：外置场景按 _id 读写即可；默认 text + keyword 映射会在正文超过 32766 字节时触发 mapper_parsing_exception。
    /// </summary>
    [EsField(Index = false, NeedKeyword = false)]
    public string Content { get; set; } = string.Empty;
}
