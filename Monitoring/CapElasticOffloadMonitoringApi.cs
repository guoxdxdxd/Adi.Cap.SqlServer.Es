using Adi.Cap.SqlServer.Es.Storage;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using Microsoft.Extensions.Logging;

namespace Adi.Cap.SqlServer.Es.Monitoring;

/// <summary>
/// CAP Dashboard <see cref="IMonitoringApi"/> 装饰器：列表与详情返回前将 envelope Content 从 ES 还原为正文。
/// </summary>
public sealed class CapElasticOffloadMonitoringApi : IMonitoringApi
{
    private readonly IMonitoringApi _inner;
    private readonly ICapElasticMessageStore _elasticStore;
    private readonly ISerializer _serializer;
    private readonly ILogger<CapElasticOffloadMonitoringApi> _logger;

    /// <summary>
    /// 创建 Dashboard Monitoring 装饰器。
    /// </summary>
    /// <param name="inner">SqlServer 原生 MonitoringApi。</param>
    /// <param name="elasticStore">ES 正文读写。</param>
    /// <param name="serializer">CAP 消息序列化器，用于详情 Origin 反序列化。</param>
    /// <param name="logger">日志。</param>
    public CapElasticOffloadMonitoringApi(
        IMonitoringApi inner,
        ICapElasticMessageStore elasticStore,
        ISerializer serializer,
        ILogger<CapElasticOffloadMonitoringApi> logger)
    {
        _inner = inner;
        _elasticStore = elasticStore;
        _serializer = serializer;
        _logger = logger;
    }

    /// <inheritdoc />
    /// <remarks>分页列表逐条还原 Content；按 content 关键字搜索仍只匹配 DB 中的 envelope。</remarks>
    public async Task<PagedQueryResult<MessageDto>> GetMessagesAsync(MessageQueryDto queryDto)
    {
        var result = await _inner.GetMessagesAsync(queryDto).ConfigureAwait(false);
        if (result.Items == null || result.Items.Count == 0)
        {
            return result;
        }

        var hydrateTasks = result.Items.Select(async item =>
        {
            item.Content = await CapElasticDbContentResolver.ResolveAsync(
                item.Content,
                _elasticStore,
                _logger,
                nameof(CapElasticOffloadMonitoringApi)).ConfigureAwait(false);
        });

        await Task.WhenAll(hydrateTasks).ConfigureAwait(false);
        return result;
    }

    /// <inheritdoc />
    /// <remarks>详情与 Requeue 均依赖此方法，须同时还原 Content 与 Origin。</remarks>
    public async Task<MediumMessage?> GetPublishedMessageAsync(long id)
    {
        var message = await _inner.GetPublishedMessageAsync(id).ConfigureAwait(false);
        return await HydrateMediumMessageAsync(message).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<MediumMessage?> GetReceivedMessageAsync(long id)
    {
        var message = await _inner.GetReceivedMessageAsync(id).ConfigureAwait(false);
        return await HydrateMediumMessageAsync(message).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<StatisticsDto> GetStatisticsAsync() => _inner.GetStatisticsAsync();

    /// <inheritdoc />
    public ValueTask<int> PublishedFailedCount() => _inner.PublishedFailedCount();

    /// <inheritdoc />
    public ValueTask<int> PublishedSucceededCount() => _inner.PublishedSucceededCount();

    /// <inheritdoc />
    public ValueTask<int> ReceivedFailedCount() => _inner.ReceivedFailedCount();

    /// <inheritdoc />
    public ValueTask<int> ReceivedSucceededCount() => _inner.ReceivedSucceededCount();

    /// <inheritdoc />
    public Task<IDictionary<DateTime, int>> HourlySucceededJobs(MessageType type)
        => _inner.HourlySucceededJobs(type);

    /// <inheritdoc />
    public Task<IDictionary<DateTime, int>> HourlyFailedJobs(MessageType type)
        => _inner.HourlyFailedJobs(type);

    /// <summary>
    /// 详情消息：还原 Content 并重新反序列化 Origin。
    /// </summary>
    /// <param name="message">内层 MonitoringApi 返回的消息。</param>
    private async Task<MediumMessage?> HydrateMediumMessageAsync(MediumMessage? message)
    {
        if (message == null)
        {
            return null;
        }

        var fullContent = await CapElasticDbContentResolver.ResolveAsync(
            message.Content,
            _elasticStore,
            _logger,
            nameof(CapElasticOffloadMonitoringApi)).ConfigureAwait(false);

        if (string.IsNullOrEmpty(fullContent) || fullContent == message.Content)
        {
            return message;
        }

        message.Content = fullContent;
        message.Origin = _serializer.Deserialize(fullContent)!;
        return message;
    }
}
