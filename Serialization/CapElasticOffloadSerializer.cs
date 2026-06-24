using Adi.Cap.SqlServer.Es.Context;
using Adi.Cap.SqlServer.Es.Envelope;
using Adi.Cap.SqlServer.Es.Options;
using Adi.Cap.SqlServer.Es.Storage;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Adi.Cap.SqlServer.Es.Serialization;

/// <summary>
/// CAP 序列化器装饰器：写入时将正文外置 ES 并返回 envelope；读取时将 envelope 还原为正文后再反序列化。
/// </summary>
public sealed class CapElasticOffloadSerializer : ISerializer
{
    private readonly ISerializer _inner;
    private readonly ICapElasticMessageStore _elasticStore;
    private readonly IOptions<CapElasticOffloadOptions> _options;
    private readonly ILogger<CapElasticOffloadSerializer> _logger;

    /// <summary>
    /// 创建序列化器装饰器。
    /// </summary>
    /// <param name="inner">CAP 原生序列化器（如 JsonUtf8Serializer）。</param>
    /// <param name="elasticStore">ES 正文读写。</param>
    /// <param name="options">外置 Topic 配置。</param>
    /// <param name="logger">日志。</param>
    public CapElasticOffloadSerializer(
        ISerializer inner,
        ICapElasticMessageStore elasticStore,
        IOptions<CapElasticOffloadOptions> options,
        ILogger<CapElasticOffloadSerializer> logger)
    {
        _inner = inner;
        _elasticStore = elasticStore;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    /// <remarks>写入作用域内且 Topic 已配置外置时，正文写入 ES 并返回 envelope JSON。</remarks>
    public string Serialize(Message message)
    {
        var scope = CapElasticOffloadWriteContext.Current;
        if (scope == null || !IsOffloadedTopic(scope.TopicName))
        {
            return _inner.Serialize(message);
        }

        var fullContent = _inner.Serialize(message);
        var capMessageId = scope.CapMessageId ?? message.GetId();

        try
        {
            var indexName = _elasticStore
                .WriteAsync(capMessageId, scope.TopicName, fullContent)
                .GetAwaiter()
                .GetResult();
            return CapEsContentEnvelope.Format(indexName, capMessageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "CapElasticOffloadSerializer 写入 ES 失败 Topic={Topic} CapMessageId={CapMessageId}",
                scope.TopicName,
                capMessageId);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask<TransportMessage> SerializeAsync(Message message)
    {
        return await _inner.SerializeAsync(message).ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>识别 envelope 时从 ES 拉取正文后再反序列化，供 SqlServerDataStorage 各读路径使用。</remarks>
    public Message? Deserialize(string json)
    {
        if (CapEsContentEnvelope.TryParse(json, CapElasticMessageDocument.EnvelopeIndexPrefix, out var reference))
        {
            try
            {
                var fullContent = _elasticStore.ReadAsync(reference).GetAwaiter().GetResult();
                return _inner.Deserialize(fullContent);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "CapElasticOffloadSerializer ES 还原失败 CapMessageId={CapMessageId} IndexName={IndexName}",
                    reference.CapMessageId,
                    reference.IndexName);
                throw;
            }
        }

        return _inner.Deserialize(json);
    }

    /// <inheritdoc />
    public ValueTask<Message> DeserializeAsync(TransportMessage transportMessage, Type? valueType)
        => _inner.DeserializeAsync(transportMessage, valueType);

    /// <inheritdoc />
    public object? Deserialize(object value, Type valueType)
        => _inner.Deserialize(value, valueType);

    /// <inheritdoc />
    public bool IsJsonType(object jsonObject)
        => _inner.IsJsonType(jsonObject);

    /// <summary>
    /// 判断 Topic 是否配置为 ES 外置。
    /// </summary>
    /// <param name="topicName">CAP Topic 名称。</param>
    private bool IsOffloadedTopic(string topicName)
    {
        return !string.IsNullOrWhiteSpace(topicName)
               && _options.Value.OffloadedTopicNames.Contains(topicName);
    }
}
