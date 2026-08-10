namespace CapOffloadProbe.Shared;

/// <summary>
/// 探测载荷：正常消息 Body 很短；超长消息 Body 刻意超过 2MiB 上限。
/// </summary>
public sealed class ProbeMessage
{
    /// <summary>消息种类：normal / oversized。</summary>
    public string Kind { get; set; } = ProbeConstants.KindNormal;

    /// <summary>本轮序号。</summary>
    public long Sequence { get; set; }

    /// <summary>发送端生成的唯一 Id。</summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>发送时间（本地）。</summary>
    public DateTime SentAt { get; set; }

    /// <summary>正文；超长场景填充大量字符。</summary>
    public string Body { get; set; } = string.Empty;
}
