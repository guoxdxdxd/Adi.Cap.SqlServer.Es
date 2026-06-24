using Adi.Cap.SqlServer.Es.Context;
using Adi.Cap.SqlServer.Es.Envelope;
using Adi.Cap.SqlServer.Es.Monitoring;
using Adi.Cap.SqlServer.Es.Options;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using DotNetCore.CAP.SqlServer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Adi.Cap.SqlServer.Es.Storage;

/// <summary>
/// CAP IDataStorage 装饰器：数据库读写全部委托 <see cref="SqlServerDataStorage"/>，仅在出入口处理 ES 外置。
/// </summary>
public sealed class CapElasticOffloadDataStorage : IDataStorage
{
    private readonly SqlServerDataStorage _inner;
    private readonly ICapElasticMessageStore _elasticStore;
    private readonly IOptions<CapElasticOffloadOptions> _options;
    private readonly ISnowflakeId _snowflakeId;
    private readonly ISerializer _serializer;
    private readonly ILogger<CapElasticOffloadDataStorage> _logger;
    private readonly ILogger<CapElasticOffloadMonitoringApi> _monitoringLogger;
    private CapElasticOffloadMonitoringApi? _monitoringApi;

    /// <summary>
    /// 创建 CAP ES 外置存储装饰器。
    /// </summary>
    public CapElasticOffloadDataStorage(
        SqlServerDataStorage inner,
        ICapElasticMessageStore elasticStore,
        IOptions<CapElasticOffloadOptions> options,
        ISnowflakeId snowflakeId,
        ISerializer serializer,
        ILogger<CapElasticOffloadDataStorage> logger,
        ILogger<CapElasticOffloadMonitoringApi> monitoringLogger)
    {
        _inner = inner;
        _elasticStore = elasticStore;
        _options = options;
        _snowflakeId = snowflakeId;
        _serializer = serializer;
        _logger = logger;
        _monitoringLogger = monitoringLogger;
    }

    /// <inheritdoc />
    public Task<bool> AcquireLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
        => _inner.AcquireLockAsync(key, ttl, instance, token);

    /// <inheritdoc />
    public Task ReleaseLockAsync(string key, string instance, CancellationToken cancellationToken = default)
        => _inner.ReleaseLockAsync(key, instance, cancellationToken);

    /// <inheritdoc />
    public Task RenewLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
        => _inner.RenewLockAsync(key, ttl, instance, token);

    /// <inheritdoc />
    public Task ChangePublishStateToDelayedAsync(string[] ids)
        => _inner.ChangePublishStateToDelayedAsync(ids);

    /// <inheritdoc />
    public async Task ChangePublishStateAsync(MediumMessage message, StatusName state, object? transaction = null)
    {
        var topicName = message.Origin.GetName();
        if (!IsOffloadedTopic(topicName))
        {
            await _inner.ChangePublishStateAsync(message, state, transaction).ConfigureAwait(false);
            return;
        }

        using (CapElasticOffloadWriteContext.Enter(topicName, message.DbId))
        {
            await _inner.ChangePublishStateAsync(message, state, transaction).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task ChangeReceiveStateAsync(MediumMessage message, StatusName state)
    {
        var topicName = message.Origin.GetName();
        if (!IsOffloadedTopic(topicName))
        {
            await _inner.ChangeReceiveStateAsync(message, state).ConfigureAwait(false);
            return;
        }

        using (CapElasticOffloadWriteContext.Enter(topicName, message.DbId))
        {
            await _inner.ChangeReceiveStateAsync(message, state).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<MediumMessage> StoreMessageAsync(string name, Message content, object? transaction = null)
    {
        if (!IsOffloadedTopic(name))
        {
            return await _inner.StoreMessageAsync(name, content, transaction).ConfigureAwait(false);
        }

        using (CapElasticOffloadWriteContext.Enter(name))
        {
            return await _inner.StoreMessageAsync(name, content, transaction).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    /// <remarks>异常消息 Content 为原始字符串，不经 ISerializer，此处先写 ES 再以 envelope 交给内层。</remarks>
    public async Task StoreReceivedExceptionMessageAsync(string name, string group, string content)
    {
        if (!IsOffloadedTopic(name))
        {
            await _inner.StoreReceivedExceptionMessageAsync(name, group, content).ConfigureAwait(false);
            return;
        }

        using (CapElasticOffloadWriteContext.Enter(name))
        {
            var capId = _snowflakeId.NextId().ToString();
            var indexName = await _elasticStore.WriteAsync(capId, name, content).ConfigureAwait(false);
            var envelope = CapEsContentEnvelope.Format(indexName, capId);
            await _inner.StoreReceivedExceptionMessageAsync(name, group, envelope).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<MediumMessage> StoreReceivedMessageAsync(string name, string group, Message message)
    {
        if (!IsOffloadedTopic(name))
        {
            return await _inner.StoreReceivedMessageAsync(name, group, message).ConfigureAwait(false);
        }

        using (CapElasticOffloadWriteContext.Enter(name))
        {
            return await _inner.StoreReceivedMessageAsync(name, group, message).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    /// <remarks>过期清理委托原版；ES 文档需依赖索引生命周期或另行补偿，避免复制 CAP 批量 DELETE SQL。</remarks>
    public Task<int> DeleteExpiresAsync(
        string table,
        DateTime timeout,
        int batchCount = 1000,
        CancellationToken token = default)
        => _inner.DeleteExpiresAsync(table, timeout, batchCount, token);

    /// <inheritdoc />
    public Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
        => _inner.GetPublishedMessagesOfNeedRetry(lookbackSeconds);

    /// <inheritdoc />
    public Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
        => _inner.GetReceivedMessagesOfNeedRetry(lookbackSeconds);

    /// <inheritdoc />
    public Task ScheduleMessagesOfDelayedAsync(
        Func<object, IEnumerable<MediumMessage>, Task> scheduleTask,
        CancellationToken token = default)
        => _inner.ScheduleMessagesOfDelayedAsync(scheduleTask, token);

    /// <inheritdoc />
    public async Task<int> DeleteReceivedMessageAsync(long id)
    {
        await TryDeleteEsByMonitoringMessageAsync(id, isPublished: false).ConfigureAwait(false);
        return await _inner.DeleteReceivedMessageAsync(id).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<int> DeletePublishedMessageAsync(long id)
    {
        await TryDeleteEsByMonitoringMessageAsync(id, isPublished: true).ConfigureAwait(false);
        return await _inner.DeletePublishedMessageAsync(id).ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>Dashboard 列表 Content 不经 ISerializer，单独包装 MonitoringApi。</remarks>
    public IMonitoringApi GetMonitoringApi()
    {
        return _monitoringApi ??= new CapElasticOffloadMonitoringApi(
            _inner.GetMonitoringApi(),
            _elasticStore,
            _serializer,
            _monitoringLogger);
    }

    /// <summary>
    /// 判断 Topic 是否配置为 ES 外置。
    /// </summary>
    /// <param name="name">CAP Topic 名称。</param>
    private bool IsOffloadedTopic(string? name)
    {
        return !string.IsNullOrWhiteSpace(name)
               && _options.Value.OffloadedTopicNames.Contains(name);
    }

    /// <summary>
    /// 删除前通过原版 MonitoringApi 读取 DB Content（envelope），再删 ES 文档。
    /// </summary>
    /// <param name="id">CAP 消息 Id。</param>
    /// <param name="isPublished">是否为 Published 表。</param>
    private async Task TryDeleteEsByMonitoringMessageAsync(long id, bool isPublished)
    {
        var monitoringApi = _inner.GetMonitoringApi();
        var message = isPublished
            ? await monitoringApi.GetPublishedMessageAsync(id).ConfigureAwait(false)
            : await monitoringApi.GetReceivedMessageAsync(id).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(message?.Content))
        {
            return;
        }

        if (CapEsContentEnvelope.TryParse(
                message.Content,
                CapElasticMessageDocument.EnvelopeIndexPrefix,
                out var reference))
        {
            await _elasticStore.DeleteAsync(reference).ConfigureAwait(false);
            return;
        }

        _logger.LogWarning(
            "CapElasticOffloadEnvelopeParseSkipped CapId={CapId} ContentSnippet={ContentSnippet}",
            id,
            message.Content.Length > 64 ? message.Content[..64] : message.Content);
    }
}
