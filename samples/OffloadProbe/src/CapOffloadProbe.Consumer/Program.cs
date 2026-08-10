using Adi.Cap.SqlServer.Es.Elastic;
using CapOffloadProbe.Shared;
using DotNetCore.CAP;
using Microsoft.Extensions.Options;

namespace CapOffloadProbe.Consumer;

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

        Validate(probe);

        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
        builder.Logging.AddFilter("DotNetCore.CAP.Internal.ConsumerRegister", LogLevel.Information);
        builder.Logging.AddFilter("Adi.Cap.SqlServer.Es", LogLevel.Information);

        builder.Services.AddSingleton<ICapElasticClientProvider, ProbeElasticClientProvider>();
        builder.Services.AddSingleton<ProbeSubscriber>();

        builder.Services.AddCap(options =>
        {
            options.DefaultGroupName = probe.ConsumerGroup;
            options.FailedRetryCount = 3;
            options.FailedRetryInterval = 30;
            options.SucceedMessageExpiredAfter = 3600;
            options.FailedMessageExpiredAfter = 3600;
            // 多分区并行消费：拒绝超长时正常消息仍可由其它线程处理
            options.ConsumerThreadCount = 4;

            options.UseSqlServer(sql =>
            {
                sql.ConnectionString = probe.CapSqlConnection;
                sql.Schema = probe.CapSchema;
            });

            // 顺序：先 SqlServer，再 ES Offload（只外置探测 Topic）
            options.UseCapElasticOffloadStorage(offload =>
            {
                offload.OffloadedTopicNames.Add(probe.TopicName);
                offload.ElasticSearchClientName = probe.Elastic.ClientName;
            });

            options.UseKafka(kafka =>
            {
                kafka.Servers = probe.KafkaBootstrapServers;
                // 约 20MB 正文；receive 必须 >= fetch.max.bytes + 512
                kafka.MainConfig["fetch.max.bytes"] = "26214400";
                kafka.MainConfig["max.partition.fetch.bytes"] = "26214400";
                kafka.MainConfig["fetch.message.max.bytes"] = "26214400";
                kafka.MainConfig["receive.message.max.bytes"] = "27262976";
            });
        });

        var host = builder.Build();
        var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Probe.Consumer");
        logger.LogInformation(
            "CapOffloadProbe.Consumer 启动 Topic={Topic} Group={Group} Schema={Schema} Kafka={Kafka}",
            probe.TopicName,
            probe.ConsumerGroup,
            probe.CapSchema,
            probe.KafkaBootstrapServers);

        await host.RunAsync().ConfigureAwait(false);
    }

    private static void Validate(ProbeOptions probe)
    {
        if (string.IsNullOrWhiteSpace(probe.CapSqlConnection))
        {
            throw new InvalidOperationException("Probe:CapSqlConnection 未配置。");
        }

        if (string.IsNullOrWhiteSpace(probe.Elastic.Uri)
            || string.IsNullOrWhiteSpace(probe.Elastic.UserName))
        {
            throw new InvalidOperationException("Probe:Elastic 未配置完整。");
        }
    }
}
