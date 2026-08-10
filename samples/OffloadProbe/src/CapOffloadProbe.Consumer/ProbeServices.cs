using Adi.Cap.SqlServer.Es.Elastic;
using CapOffloadProbe.Shared;
using DotNetCore.CAP;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Microsoft.Extensions.Options;

namespace CapOffloadProbe.Consumer;

public sealed class ProbeOptions
{
    public const string SectionName = "Probe";

    public string KafkaBootstrapServers { get; set; } = string.Empty;
    public string CapSqlConnection { get; set; } = string.Empty;
    public string CapSchema { get; set; } = ProbeConstants.CapSchema;
    public string ConsumerGroup { get; set; } = ProbeConstants.ConsumerGroup;
    public string TopicName { get; set; } = ProbeConstants.TopicName;
    public ElasticOptions Elastic { get; set; } = new();
}

public sealed class ElasticOptions
{
    public string Uri { get; set; } = string.Empty;
    public string UserName { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string ClientName { get; set; } = "Data";
}

/// <summary>
/// 固定返回探测用 ES 客户端。
/// </summary>
public sealed class ProbeElasticClientProvider : ICapElasticClientProvider
{
    private readonly ElasticsearchClient _client;

    public ProbeElasticClientProvider(IOptions<ProbeOptions> options)
    {
        var elastic = options.Value.Elastic;
        if (string.IsNullOrWhiteSpace(elastic.Uri))
        {
            throw new InvalidOperationException("Probe:Elastic:Uri 未配置。");
        }

        var settings = new ElasticsearchClientSettings(new Uri(elastic.Uri, UriKind.Absolute))
            .DisableAutomaticProxyDetection()
            .DisableDirectStreaming()
            .ServerCertificateValidationCallback(static (_, _, _, _) => true)
            .Authentication(new BasicAuthentication(elastic.UserName, elastic.Password));

        _client = new ElasticsearchClient(settings);
    }

    public ElasticsearchClient GetClient() => _client;
}

/// <summary>
/// 仅订阅探测 Topic，日志前缀统一便于 grep。
/// </summary>
public sealed class ProbeSubscriber : ICapSubscribe
{
    private readonly ILogger<ProbeSubscriber> _logger;
    private long _normalCount;
    private long _ignoredCount;

    public ProbeSubscriber(ILogger<ProbeSubscriber> logger)
    {
        _logger = logger;
    }

    [CapSubscribe(ProbeConstants.TopicName)]
    public void Handle(ProbeMessage? message)
    {
        // 超限丢弃后 Value 被清空，CAP 反序列化到类型参数即为 null。
        if (message is null)
        {
            var ignored = Interlocked.Increment(ref _ignoredCount);
            _logger.LogWarning(
                "[PROBE-OVERSIZE-IGNORED] Value=null（正文已按 2MiB 上限丢弃，Kafka 应已 Commit）。IgnoredTotal={IgnoredTotal} NormalTotal={NormalTotal}",
                ignored,
                Interlocked.Read(ref _normalCount));
            return;
        }

        var bodyLen = message.Body?.Length ?? 0;
        if (string.Equals(message.Kind, ProbeConstants.KindOversized, StringComparison.OrdinalIgnoreCase)
            || bodyLen > 1024 * 1024)
        {
            _logger.LogError(
                "[PROBE-OVERSIZE-UNEXPECTED] 超长消息仍进入订阅方法，说明未丢弃。Seq={Seq} Id={Id} BodyChars={BodyChars}",
                message.Sequence,
                message.MessageId,
                bodyLen);
            return;
        }

        var normal = Interlocked.Increment(ref _normalCount);
        _logger.LogInformation(
            "[PROBE-NORMAL-OK] Seq={Seq} Id={Id} Body={Body} BodyChars={BodyChars} NormalTotal={NormalTotal} IgnoredTotal={IgnoredTotal} Thread={Thread}",
            message.Sequence,
            message.MessageId,
            message.Body,
            bodyLen,
            normal,
            Interlocked.Read(ref _ignoredCount),
            Environment.CurrentManagedThreadId);
    }
}
