using CapOffloadProbe.Shared;
using DotNetCore.CAP;
using Microsoft.Extensions.Options;

namespace CapOffloadProbe.Producer;

public sealed class ProbeOptions
{
    public const string SectionName = "Probe";

    public string KafkaBootstrapServers { get; set; } = string.Empty;
    public string CapSqlConnection { get; set; } = string.Empty;
    public string CapSchema { get; set; } = ProbeConstants.CapSchema + "_pub";
    public string TopicName { get; set; } = ProbeConstants.TopicName;

    /// <summary>正常消息投递间隔（秒）。</summary>
    public int NormalIntervalSeconds { get; set; } = 2;

    /// <summary>超长消息投递间隔（秒）。</summary>
    public int OversizedIntervalSeconds { get; set; } = 8;

    /// <summary>持续运行秒数；0 表示一直跑直到 Ctrl+C。</summary>
    public int DurationSeconds { get; set; } = 90;

    /// <summary>超长 Body 字符数（默认约 20MiB）。</summary>
    public int OversizedBodyChars { get; set; } = 20 * 1024 * 1024;
}

/// <summary>
/// 双通道并行投递：正常消息与超长消息各自独立循环，互不等待。
/// Producer 故意不启用 ES Offload，确保超长正文完整进入 Kafka。
/// </summary>
public sealed class ProbePublishWorker : BackgroundService
{
    private readonly ICapPublisher _publisher;
    private readonly IOptions<ProbeOptions> _options;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<ProbePublishWorker> _logger;
    private long _normalSeq;
    private long _oversizedSeq;

    public ProbePublishWorker(
        ICapPublisher publisher,
        IOptions<ProbeOptions> options,
        IHostApplicationLifetime lifetime,
        ILogger<ProbePublishWorker> logger)
    {
        _publisher = publisher;
        _options = options;
        _lifetime = lifetime;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = _options.Value;
        _logger.LogInformation(
            "ProbePublishWorker 并行启动 NormalEvery={Normal}s OversizedEvery={Over}s OversizeChars={Chars}(~{Mb:F1}MiB) Duration={Duration}s Topic={Topic}",
            opts.NormalIntervalSeconds,
            opts.OversizedIntervalSeconds,
            opts.OversizedBodyChars,
            opts.OversizedBodyChars / (1024.0 * 1024.0),
            opts.DurationSeconds,
            opts.TopicName);

        await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken).ConfigureAwait(false);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        if (opts.DurationSeconds > 0)
        {
            linked.CancelAfter(TimeSpan.FromSeconds(opts.DurationSeconds));
        }

        var token = linked.Token;
        var normalTask = RunNormalLoopAsync(opts, token);
        var oversizedTask = RunOversizedLoopAsync(opts, token);

        try
        {
            await Task.WhenAll(normalTask, oversizedTask).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "[PROBE-PUBLISH-DONE] 达到持续时长 Duration={Duration}s NormalSent={Normal} OversizedSent={Over}",
                opts.DurationSeconds,
                Interlocked.Read(ref _normalSeq),
                Interlocked.Read(ref _oversizedSeq));
            _lifetime.StopApplication();
        }
    }

    private async Task RunNormalLoopAsync(ProbeOptions opts, CancellationToken token)
    {
        var delay = TimeSpan.FromSeconds(Math.Max(1, opts.NormalIntervalSeconds));
        while (!token.IsCancellationRequested)
        {
            var seq = Interlocked.Increment(ref _normalSeq);
            try
            {
                var msg = new ProbeMessage
                {
                    Kind = ProbeConstants.KindNormal,
                    Sequence = seq,
                    MessageId = $"N-{seq}-{Guid.NewGuid():N}"[..20],
                    SentAt = DateTime.Now,
                    Body = $"hello-probe-{seq}-{DateTime.Now:HH:mm:ss.fff}"
                };

                await _publisher.PublishAsync(ProbeConstants.TopicName, msg, cancellationToken: token)
                    .ConfigureAwait(false);

                _logger.LogInformation(
                    "[PROBE-PUBLISH-NORMAL] Seq={Seq} Id={Id} BodyChars={BodyChars}",
                    msg.Sequence,
                    msg.MessageId,
                    msg.Body.Length);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "[PROBE-PUBLISH-NORMAL-FAIL] Seq={Seq}", seq);
            }

            await Task.Delay(delay, token).ConfigureAwait(false);
        }
    }

    private async Task RunOversizedLoopAsync(ProbeOptions opts, CancellationToken token)
    {
        var delay = TimeSpan.FromSeconds(Math.Max(3, opts.OversizedIntervalSeconds));
        var chars = Math.Max(opts.OversizedBodyChars, 20 * 1024 * 1024);

        // 预分配一次，避免每轮 new string 20MB 造成 GC 抖动
        var sharedBody = new string('X', chars);

        while (!token.IsCancellationRequested)
        {
            var seq = Interlocked.Increment(ref _oversizedSeq);
            try
            {
                var msg = new ProbeMessage
                {
                    Kind = ProbeConstants.KindOversized,
                    Sequence = seq,
                    MessageId = $"O-{seq}-{Guid.NewGuid():N}"[..20],
                    SentAt = DateTime.Now,
                    Body = sharedBody
                };

                var started = DateTime.UtcNow;
                await _publisher.PublishAsync(ProbeConstants.TopicName, msg, cancellationToken: token)
                    .ConfigureAwait(false);

                _logger.LogWarning(
                    "[PROBE-PUBLISH-OVERSIZE] Seq={Seq} Id={Id} BodyChars={BodyChars}(~{Mb:F1}MiB) ElapsedMs={ElapsedMs}",
                    msg.Sequence,
                    msg.MessageId,
                    msg.Body.Length,
                    msg.Body.Length / (1024.0 * 1024.0),
                    (DateTime.UtcNow - started).TotalMilliseconds);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "[PROBE-PUBLISH-OVERSIZE-FAIL] Seq={Seq}", seq);
            }

            await Task.Delay(delay, token).ConfigureAwait(false);
        }
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Configuration
            .AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true);

        builder.Services.Configure<ProbeOptions>(builder.Configuration.GetSection(ProbeOptions.SectionName));
        var probe = builder.Configuration.GetSection(ProbeOptions.SectionName).Get<ProbeOptions>()
                    ?? throw new InvalidOperationException("缺少 Probe 配置，请复制 appsettings.Local.example.json → appsettings.Local.json");

        if (string.IsNullOrWhiteSpace(probe.CapSqlConnection))
        {
            throw new InvalidOperationException("Probe:CapSqlConnection 未配置。");
        }

        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddFilter("Microsoft", LogLevel.Warning);

        builder.Services.AddHostedService<ProbePublishWorker>();

        builder.Services.AddCap(options =>
        {
            options.DefaultGroupName = "cap.offload.probe.publisher";
            options.FailedRetryCount = 2;
            options.ConsumerThreadCount = 2;

            options.UseSqlServer(sql =>
            {
                sql.ConnectionString = probe.CapSqlConnection;
                sql.Schema = probe.CapSchema;
            });

            // 故意不启用 UseCapElasticOffloadStorage：超长正文必须完整进 Kafka
            options.UseKafka(kafka =>
            {
                kafka.Servers = probe.KafkaBootstrapServers;
                // 20MB 正文 + CAP Headers，留余量到 25MB
                kafka.MainConfig["message.max.bytes"] = "26214400";
                kafka.MainConfig["socket.send.buffer.bytes"] = "26214400";
                kafka.MainConfig["queue.buffering.max.kbytes"] = "102400";
            });
        });

        var host = builder.Build();
        var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Probe.Producer");
        logger.LogInformation(
            "CapOffloadProbe.Producer 启动 Topic={Topic} Schema={Schema} Kafka={Kafka}（无 ES Offload，双通道并行）",
            probe.TopicName,
            probe.CapSchema,
            probe.KafkaBootstrapServers);

        await host.RunAsync().ConfigureAwait(false);
    }
}
