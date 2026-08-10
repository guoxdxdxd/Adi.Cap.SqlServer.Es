namespace Adi.Cap.SqlServer.Es.Options;

/// <summary>
/// CAP ES 外置写入安全上限。
/// </summary>
public static class CapElasticOffloadLimits
{
    /// <summary>
    /// 单条 Content UTF-8 字节上限；超过则拒绝正文（不写 ES、清空 Value），CAP 流程不抛异常（2 MiB）。
    /// </summary>
    public const int MaxContentLengthBytes = 2 * 1024 * 1024;
}
