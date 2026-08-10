namespace CapOffloadProbe.Shared;

/// <summary>
/// 探测 Topic / Group 常量（与 Kafka 物理 Topic 名一致）。
/// </summary>
public static class ProbeConstants
{
    /// <summary>CAP 消息名 / Kafka Topic。</summary>
    public const string TopicName = "cap.offload.probe";

    /// <summary>独立消费组，避免与 OMS BackgroundJob 抢消息。</summary>
    public const string ConsumerGroup = "cap.offload.probe.consumer";

    /// <summary>CAP SqlServer 独立 Schema，不污染 OMS 表。</summary>
    public const string CapSchema = "cap_offload_probe";

    /// <summary>正常消息 Kind。</summary>
    public const string KindNormal = "normal";

    /// <summary>超长消息 Kind。</summary>
    public const string KindOversized = "oversized";
}
