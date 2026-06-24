namespace Adi.Cap.SqlServer.Es.Options;

/// <summary>
/// CAP 大报文 ES 外置存储配置；所有业务值须由宿主在注册时显式传入，库内不提供默认值。
/// </summary>
public sealed class CapElasticOffloadOptions
{
    /// <summary>
    /// 需外置 Content 的 CAP Topic 名称集合（CAP 消息 name）。
    /// 大小写不敏感；仅启动期通过 Configure 委托写入。
    /// </summary>
    public HashSet<string> OffloadedTopicNames { get; } =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// 宿主 ES 客户端工厂中的客户端名称（如 B2B <c>ElasticSearchFactory</c> 注册名）。
    /// </summary>
    public string? ElasticSearchClientName { get; set; }
}
