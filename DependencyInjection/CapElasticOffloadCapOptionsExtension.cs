using Adi.Cap.SqlServer.Es.Internal;
using Adi.Cap.SqlServer.Es.Monitoring;
using Adi.Cap.SqlServer.Es.Options;
using Adi.Cap.SqlServer.Es.Serialization;
using Adi.Cap.SqlServer.Es.Storage;
using DotNetCore.CAP;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using DotNetCore.CAP.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Adi.Cap.SqlServer.Es.DependencyInjection;

/// <summary>
/// CAP Options 扩展：注册 ES 外置装饰器（ISerializer / ISnowflakeId / IDataStorage）。
/// </summary>
internal sealed class CapElasticOffloadCapOptionsExtension : ICapOptionsExtension
{
    private readonly Action<CapElasticOffloadOptions> _configure;

    /// <summary>
    /// 创建扩展实例。
    /// </summary>
    /// <param name="configure">Options 配置委托（必填）。</param>
    public CapElasticOffloadCapOptionsExtension(Action<CapElasticOffloadOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configure = configure;
    }

    /// <inheritdoc />
    public void AddServices(IServiceCollection services)
    {
        services.AddOptions<CapElasticOffloadOptions>()
            .Configure(_configure);

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<CapElasticOffloadOptions>, CapElasticOffloadOptionsValidator>());

        services.TryAddSingleton<ICapElasticMessageStore, CapElasticMessageStore>();

        // 内层具体类型：装饰器构造与 Autofac/ABP GetRequiredService 解析。
        services.TryAddSingleton<JsonUtf8Serializer>();
        services.TryAddSingleton<SnowflakeId>();

        services.Replace(ServiceDescriptor.Singleton<ISerializer>(sp =>
            new CapElasticOffloadSerializer(
                sp.GetRequiredService<JsonUtf8Serializer>(),
                sp.GetRequiredService<ICapElasticMessageStore>(),
                sp.GetRequiredService<IOptions<CapElasticOffloadOptions>>(),
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<CapElasticOffloadSerializer>>())));

        services.Replace(ServiceDescriptor.Singleton<ISnowflakeId>(sp =>
            new CapElasticOffloadSnowflakeId(sp.GetRequiredService<SnowflakeId>())));

        services.TryAddSingleton<SqlServerDataStorage>();

        services.Replace(ServiceDescriptor.Singleton<IDataStorage>(sp =>
            new CapElasticOffloadDataStorage(
                sp.GetRequiredService<SqlServerDataStorage>(),
                sp.GetRequiredService<ICapElasticMessageStore>(),
                sp.GetRequiredService<IOptions<CapElasticOffloadOptions>>(),
                sp.GetRequiredService<ISnowflakeId>(),
                sp.GetRequiredService<ISerializer>(),
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<CapElasticOffloadDataStorage>>(),
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<CapElasticOffloadMonitoringApi>>())));
    }
}
