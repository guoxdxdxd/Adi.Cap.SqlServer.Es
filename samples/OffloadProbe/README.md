## CapOffloadProbe — CAP ES Offload 超长消息探测

独立轻量 Producer / Consumer，Kafka / SQL / ES 地址仅写在本地
`appsettings.Local.json`（已 gitignore），**不跑 OMS BackgroundJob**，避免其它业务日志干扰。

### 架构

```
Producer（无 ES Offload）                Consumer（启用 Adi.Cap.SqlServer.Es）
  双通道并行：normal / oversized(~20MB) → Kafka topic: cap.offload.probe (6 分区)
  CAP Schema: cap_offload_probe_pub      CAP Schema: cap_offload_probe
                                         Group: cap.offload.probe.consumer
                                         ConsumerThreadCount=4
                                         超限：不抛异常，Value 清空，Commit
```

### 预期日志

| 标记 | 含义 |
|------|------|
| `[PROBE-PUBLISH-NORMAL]` | 发送端并行发出正常消息（默认每 2s） |
| `[PROBE-PUBLISH-OVERSIZE]` | 发送端并行发出约 20MiB 超长消息（默认每 8s） |
| `[PROBE-NORMAL-OK]` | 消费端成功处理正常消息（可与超长拒绝交错出现） |
| `[PROBE-OVERSIZE-IGNORED]` | 消费端收到 Value=null，判定为超限丢弃 |
| `CapElasticOffloadContentRejected` | 库内拒绝写 ES 的 Warning（ContentUtf8Bytes≈20MB） |
| `[PROBE-OVERSIZE-UNEXPECTED]` | 异常：超长仍进入业务方法 |
| `[PROBE-PUBLISH-DONE]` | 达到 `DurationSeconds` 后自动停开发送 |

默认持续 **90 秒**；可在 `appsettings.Local.json` 调 `DurationSeconds`（0=一直跑）。

### 一次性准备

1. Topic 已创建：`cap.offload.probe`（`max.message.bytes=10485760`）
2. 复制本地配置（已 gitignore）：

```bash
cp src/CapOffloadProbe.Consumer/appsettings.Local.example.json \
   src/CapOffloadProbe.Consumer/appsettings.Local.json
cp src/CapOffloadProbe.Producer/appsettings.Local.example.json \
   src/CapOffloadProbe.Producer/appsettings.Local.json
# 填入 SQL / ES 密码
```

3. 确保库内 `Adi.Cap.SqlServer.Es` 已是「超限不抛异常」工作区版本（ProjectReference）。

### 运行

```bash
# 终端 1 — 先起消费
./scripts/run-consumer.sh

# 终端 2 — 再起投递（每 15s：1 正常 + 1 超长）
./scripts/run-producer.sh

# 可选：重建 Topic
./scripts/create-topic.sh
```

观察 Consumer 控制台：应交替出现 `NORMAL-OK` 与 `OVERSIZE-IGNORED`，且 **不应** 出现 CAP Reject / Kafka 死循环。

### 与 OMS 隔离说明

- 独立 Topic / Group / CAP Schema，不订阅 SAP 业务 Topic
- Producer **不**启用 ES Offload，才能把超长正文完整打进 Kafka
- Consumer 才启用 Offload，验证「入库丢弃 + Commit」
