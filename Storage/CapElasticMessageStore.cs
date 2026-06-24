using Adi.Cap.SqlServer.Es.Elastic;
using Adi.Cap.SqlServer.Es.Envelope;
using Adi.ElasticSugar.Core.Document;
using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Logging;

namespace Adi.Cap.SqlServer.Es.Storage;

/// <summary>
/// 基于 Elasticsearch 的 CAP 消息正文存储实现（写入走 Adi.ElasticSugar.Core）。
/// </summary>
public sealed class CapElasticMessageStore : ICapElasticMessageStore
{
    private readonly ICapElasticClientProvider _clientProvider;
    private readonly ILogger<CapElasticMessageStore> _logger;

    /// <summary>
    /// 创建 ES 消息存储服务。
    /// </summary>
    /// <param name="clientProvider">宿主提供的 ES 客户端来源。</param>
    /// <param name="logger">日志。</param>
    public CapElasticMessageStore(
        ICapElasticClientProvider clientProvider,
        ILogger<CapElasticMessageStore> logger)
    {
        _clientProvider = clientProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<string> WriteAsync(
        string capMessageId,
        string topicName,
        string fullContent,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var document = new CapElasticMessageDocument
        {
            Id = capMessageId,
            EsDateTime = DateTime.UtcNow,
            TopicName = topicName,
            Content = fullContent
        };

        var indexName = document.GetIndexNameFromAttribute();

        try
        {
            var client = _clientProvider.GetClient();
            await client.PushDocumentAsync(document).ConfigureAwait(false);

            _logger.LogInformation(
                "CapElasticOffload ES 写入成功 Topic={Topic} CapId={CapId} IndexName={IndexName}",
                topicName,
                capMessageId,
                indexName);

            return indexName;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                ex,
                "CapElasticOffloadWriteFailed Topic={Topic} CapId={CapId} IndexName={IndexName}",
                topicName,
                capMessageId,
                indexName);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<string> ReadAsync(
        CapEsReference reference,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _clientProvider.GetClient();
            var response = await client.GetAsync<CapElasticMessageDocument>(
                reference.IndexName,
                reference.CapMessageId,
                cancellationToken).ConfigureAwait(false);

            if (!response.Found || response.Source == null || string.IsNullOrEmpty(response.Source.Content))
            {
                throw new InvalidOperationException(
                    $"ES 文档不存在或 content 为空: {reference.IndexName}/{reference.CapMessageId}");
            }

            return response.Source.Content;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                ex,
                "CapElasticOffloadReadFailed CapId={CapId} IndexName={IndexName}",
                reference.CapMessageId,
                reference.IndexName);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task DeleteAsync(
        CapEsReference reference,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _clientProvider.GetClient();
            var response = await client.DeleteAsync<CapElasticMessageDocument>(
                reference.CapMessageId,
                d => d.Index(reference.IndexName),
                cancellationToken).ConfigureAwait(false);

            if (!response.IsValidResponse && response.Result != Result.NotFound)
            {
                _logger.LogWarning(
                    "CapElasticOffloadDeleteFailed CapId={CapId} IndexName={IndexName} Reason={Reason}",
                    reference.CapMessageId,
                    reference.IndexName,
                    response.ElasticsearchServerError?.Error?.Reason ?? response.DebugInformation);
                return;
            }

            _logger.LogDebug(
                "CapElasticOffload ES 删除成功 CapId={CapId} IndexName={IndexName}",
                reference.CapMessageId,
                reference.IndexName);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "CapElasticOffloadDeleteFailed CapId={CapId} IndexName={IndexName}",
                reference.CapMessageId,
                reference.IndexName);
        }
    }

    /// <inheritdoc />
    public async Task<int> DeleteManyAsync(
        IReadOnlyList<CapEsReference> references,
        CancellationToken cancellationToken = default)
    {
        if (references.Count == 0)
        {
            return 0;
        }

        var deletedCount = 0;
        foreach (var reference in references)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var client = _clientProvider.GetClient();
                var response = await client.DeleteAsync<CapElasticMessageDocument>(
                    reference.CapMessageId,
                    d => d.Index(reference.IndexName),
                    cancellationToken).ConfigureAwait(false);

                if (response.IsValidResponse || response.Result == Result.NotFound)
                {
                    deletedCount++;
                }
                else
                {
                    _logger.LogWarning(
                        "CapElasticOffloadDeleteFailed CapId={CapId} IndexName={IndexName} Reason={Reason}",
                        reference.CapMessageId,
                        reference.IndexName,
                        response.ElasticsearchServerError?.Error?.Reason ?? response.DebugInformation);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(
                    ex,
                    "CapElasticOffloadDeleteFailed CapId={CapId} IndexName={IndexName}",
                    reference.CapMessageId,
                    reference.IndexName);
            }
        }

        return deletedCount;
    }
}
