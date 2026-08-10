#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOPIC="${1:-cap.offload.probe}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:?请设置环境变量 KAFKA_BOOTSTRAP，例如 export KAFKA_BOOTSTRAP=host:9092}"

cd /tmp
rm -rf CapOffloadProbeTopicTool
dotnet new console -n CapOffloadProbeTopicTool -o CapOffloadProbeTopicTool --force >/dev/null
cd CapOffloadProbeTopicTool
dotnet add package Confluent.Kafka --version 2.6.1 >/dev/null
cat > Program.cs <<'EOF'
using Confluent.Kafka;
using Confluent.Kafka.Admin;

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP")
    ?? throw new InvalidOperationException("请设置环境变量 KAFKA_BOOTSTRAP");
var topic = args.ElementAtOrDefault(0) ?? "cap.offload.probe";
using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
var meta = admin.GetMetadata(TimeSpan.FromSeconds(15));
if (meta.Topics.Any(t => t.Topic == topic && !t.Error.IsError))
{
    Console.WriteLine($"Topic exists: {topic}");
    return;
}
await admin.CreateTopicsAsync(new[]
{
    new TopicSpecification
    {
        Name = topic,
        NumPartitions = 1,
        ReplicationFactor = 1,
        Configs = new Dictionary<string, string> { ["max.message.bytes"] = "10485760" }
    }
});
Console.WriteLine($"Created topic: {topic}");
EOF

KAFKA_BOOTSTRAP="$BOOTSTRAP" dotnet run --no-restore -- "$TOPIC" 2>/dev/null || \
  KAFKA_BOOTSTRAP="$BOOTSTRAP" dotnet run -- "$TOPIC"
