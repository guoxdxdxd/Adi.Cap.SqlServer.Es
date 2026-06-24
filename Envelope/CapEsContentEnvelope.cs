using System.Text.Json;

namespace Adi.Cap.SqlServer.Es.Envelope;

/// <summary>
/// 解析成功的 ES 引用。
/// </summary>
/// <param name="IndexName">ES 索引名。</param>
/// <param name="CapMessageId">CAP 消息 Id，与 ES 文档 _id 一致。</param>
public readonly record struct CapEsReference(string IndexName, string CapMessageId);

/// <summary>
/// CAP DB Content 列 ES 引用 envelope 解析与格式化。
/// </summary>
public static class CapEsContentEnvelope
{
    /// <summary>
    /// envelope JSON 属性名。
    /// </summary>
    public const string PropertyName = "$capEs";

    /// <summary>
    /// 格式化为 DB 存储 JSON（紧凑，无多余空白）。
    /// </summary>
    /// <param name="indexName">ES 索引名。</param>
    /// <param name="capMessageId">CAP 消息 Id。</param>
    /// <returns>envelope JSON 字符串。</returns>
    public static string Format(string indexName, string capMessageId)
    {
        return JsonSerializer.Serialize(new Dictionary<string, string>
        {
            [PropertyName] = $"{indexName}/{capMessageId}"
        });
    }

    /// <summary>
    /// 尝试从 DB Content 解析 ES 引用；非引用格式返回 false。
    /// </summary>
    /// <param name="dbContent">数据库 Content 列原文。</param>
    /// <param name="indexNamePrefix">索引前缀，用于校验引用合法性。</param>
    /// <param name="reference">解析成功时输出引用。</param>
    /// <returns>识别为 envelope 时返回 true。</returns>
    public static bool TryParse(string? dbContent, string indexNamePrefix, out CapEsReference reference)
    {
        reference = default;

        if (string.IsNullOrWhiteSpace(dbContent))
        {
            return false;
        }

        // 全量 Content 通常远大于 envelope，快速跳过。
        if (!dbContent.StartsWith('{') || dbContent.Length > 512)
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(dbContent);
            if (!document.RootElement.TryGetProperty(PropertyName, out var capEsElement))
            {
                return false;
            }

            if (capEsElement.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            var value = capEsElement.GetString();
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            var slashIndex = value.LastIndexOf('/');
            if (slashIndex <= 0 || slashIndex >= value.Length - 1)
            {
                return false;
            }

            var indexName = value[..slashIndex];
            var capMessageId = value[(slashIndex + 1)..];

            if (!indexName.StartsWith(indexNamePrefix, StringComparison.OrdinalIgnoreCase)
                || string.IsNullOrWhiteSpace(capMessageId))
            {
                return false;
            }

            reference = new CapEsReference(indexName, capMessageId);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
