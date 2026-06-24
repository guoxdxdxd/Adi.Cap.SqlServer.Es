using Adi.Cap.SqlServer.Es.DependencyInjection;
using Adi.Cap.SqlServer.Es.Options;

namespace DotNetCore.CAP;

/// <summary>
/// CAP ES 外置存储 Options 扩展方法。
/// </summary>
public static class CapElasticOffloadOptionsExtensions
{
    /// <summary>
    /// 在 UseSqlServer 之后调用，将 IDataStorage 替换为 ES 外置 Decorator。
    /// </summary>
    /// <param name="options">CAP 配置。</param>
    /// <param name="configure">外置 Topic、ES 客户端与索引配置（必填，库内无默认值）。</param>
    /// <returns>当前 CapOptions。</returns>
    public static CapOptions UseCapElasticOffloadStorage(
        this CapOptions options,
        Action<CapElasticOffloadOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        options.RegisterExtension(new CapElasticOffloadCapOptionsExtension(configure));
        return options;
    }
}
