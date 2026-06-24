# Adi.Cap.SqlServer.Es

CAP SqlServer 存储的 **ES 外置扩展**：将指定 Topic 的大报文 `Content` 写入 Elasticsearch，数据库仅保留轻量引用，从而减轻 SQL Server 体积与 I/O 压力。

## 背景

[DotNetCore.CAP](https://github.com/dotnetcore/CAP) 默认将消息正文（`Content`）完整写入 SqlServer 的 `Published` / `Received` 表。当某些 Topic 携带大 JSON、批量数据或附件元信息时，会导致：

- 数据库文件持续膨胀，备份与恢复变慢
- 大字段读写增加磁盘 I/O，影响 CAP 调度与重试性能
- Dashboard 列表查询扫描大量 `nvarchar(max)` 列，拖慢监控页面

本库针对 **已选定的大报文 Topic**，在持久化层将正文外置到 ES，数据库只存一条 envelope 引用（通常不足 100 字节）。

## 功能

- **按 Topic 选择性外置**：仅 `OffloadedTopicNames` 中配置的 CAP Topic 走 ES，其余 Topic 行为与原版 CAP 完全一致
- **透明读写**：通过装饰 `IDataStorage` / `ISerializer` / `ISnowflakeId`，CAP 发布、消费、重试、Dashboard 详情/Requeue 无需改业务代码
- **按月分索引**：ES 文档写入 `cap-message-{yyyy-MM}`（UTC），便于按时间归档与 ILM 清理
- **Dashboard 兼容**：Monitoring API 装饰器会在列表/详情展示前从 ES 还原正文
- **删除联动**：通过 Dashboard 或 API 删除 CAP 消息时，尽力删除对应 ES 文档

## 优点

| 维度 | 说明 |
|------|------|
| **对 CAP 无侵入** | 不 fork、不 patch CAP 源码；通过 `ICapOptionsExtension` 注册装饰器，升级 CAP 版本只需保持包引用兼容 |
| **按需启用** | 只外置指定 Topic，小消息仍走 SQL，避免 ES 读写开销 |
| **宿主掌控 ES 连接** | 库内不创建 ES Client，由宿主实现 `ICapElasticClientProvider`，复用现有集群与鉴权 |
| **失败策略明确** | 写入/读取 ES 失败时快速失败，避免 silently 把大正文落回 SQL |

## 工作原理

```
发布/消费入库
    │
    ▼
CapElasticOffloadDataStorage（装饰 SqlServerDataStorage）
    │  进入 WriteContext，标记当前 Topic
    ▼
CapElasticOffloadSerializer（装饰 JsonUtf8Serializer）
    │  序列化完整 Content → 写入 ES → 返回 envelope JSON
    ▼
SqlServerDataStorage
    │  Content 列仅存 {"$capEs":"cap-message-2026-06/{id}"}
    ▼
读取 / 重试 / Dashboard
    │
    ▼
识别 envelope → 从 ES 拉取正文 → 再走 CAP 原生反序列化
```

数据库 envelope 示例：

```json
{"$capEs":"cap-message-2026-06/1234567890123456789"}
```

ES 文档字段（`CapElasticMessageDocument`）：

| 字段 | 说明 |
|------|------|
| `_id` | CAP 消息 Id（Snowflake） |
| `TopicName` | CAP Topic 名称 |
| `Content` | 原 CAP 序列化后的完整正文 |
| `EsDateTime` | 写入时间（UTC），用于生成按月索引名 |

## 环境要求

- .NET 10.0+
- [DotNetCore.CAP](https://www.nuget.org/packages/DotNetCore.CAP) **10.x**
- [DotNetCore.CAP.SqlServer](https://www.nuget.org/packages/DotNetCore.CAP.SqlServer) **10.x**
- 已部署并可访问的 **Elasticsearch** 集群
- 宿主项目已有 ES 客户端工厂（或等价能力）

## 安装

```xml
<PackageReference Include="AdiCapSqlServer.Es" Version="1.0.0" />
<PackageReference Include="AdiElasticSugar.Core" Version="1.1.6" />
```

> `AdiElasticSugar.Core` 提供文档推送（`PushDocumentAsync`）与索引自动创建；通常随本库传递引用，宿主若已引用可不必重复添加。

## 快速开始

### 1. 实现 ES 客户端提供者

在 **`AddCap` 之前** 注册 `ICapElasticClientProvider`，从宿主现有 ES 工厂解析 `ElasticsearchClient`：

```csharp
using Adi.Cap.SqlServer.Es.Elastic;
using Elastic.Clients.Elasticsearch;

public sealed class CapElasticClientProvider : ICapElasticClientProvider
{
    private readonly IElasticSearchFactory _factory; // 宿主自有工厂
    private readonly CapElasticOffloadOptions _options;

    public CapElasticClientProvider(
        IElasticSearchFactory factory,
        IOptions<CapElasticOffloadOptions> options)
    {
        _factory = factory;
        _options = options.Value;
    }

    public ElasticsearchClient GetClient()
    {
        // ElasticSearchClientName 须与下方 UseCapElasticOffloadStorage 配置一致
        return _factory.GetClient(_options.ElasticSearchClientName!);
    }
}

// DI 注册（Startup / Module）
services.AddSingleton<ICapElasticClientProvider, CapElasticClientProvider>();
```

### 2. 配置 CAP

**顺序很重要**：先 `UseSqlServer`，再 `UseCapElasticOffloadStorage`。

```csharp
using DotNetCore.CAP;

services.AddCap(options =>
{
    options.UseSqlServer(sql =>
    {
        sql.ConnectionString = configuration.GetConnectionString("CapDb")!;
        // 其他 SqlServer 配置...
    });

    options.UseCapElasticOffloadStorage(offload =>
    {
        // 必填：需要外置 Content 的 CAP Topic（消息 name），大小写不敏感
        offload.OffloadedTopicNames.Add("order.sync.large-payload");
        offload.OffloadedTopicNames.Add("report.export.completed");

        // 必填：宿主 ES 工厂中的客户端名称
        offload.ElasticSearchClientName = "Data";
    });

    options.UseKafka(kafka =>
    {
        // 消息队列配置...
    });
});
```

### 3. 启动校验

`CapElasticOffloadOptions` **不提供默认值**，启动时会校验：

- `OffloadedTopicNames` 不能为空
- `ElasticSearchClientName` 不能为空

未配置或 `ICapElasticClientProvider` 未注册时，应用启动即失败（fail-fast），避免运行时 silently 落回 SQL。

## 配置项

| 属性 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `OffloadedTopicNames` | `HashSet<string>` | 是 | 需外置 Content 的 CAP Topic 名称集合，与 `[CapSubscribe("topic")]` / `CapPublish` 的 name 一致 |
| `ElasticSearchClientName` | `string` | 是 | 传给 `ICapElasticClientProvider` 的 ES 客户端逻辑名 |

```csharp
offload.OffloadedTopicNames.Add("your-topic-name");
offload.ElasticSearchClientName = "Data";
```

## ES 索引

- 索引前缀：`cap-message`
- 分片策略：按 **UTC 年月** → `cap-message-2026-06`
- 由 `[EsIndex("cap-message", YearMonth)]` + `AdiElasticSugar.Core` 自动创建

### 索引生命周期建议

CAP 过期清理（`DeleteExpiresAsync`）**仅删除 SQL 行**，不会批量删除 ES 文档。请在 ES 侧配置 **ILM** 或定时任务，按 `cap-message-*` 前缀清理历史索引，避免 ES 存储无限增长。

手动删除单条 CAP 消息（Dashboard / Monitoring API）时，库会 **尽力** 删除对应 ES 文档；删除失败只记 Warning，SQL 行仍会被删除。

## 失败策略

| 场景 | 行为 |
|------|------|
| **ES 写入失败** | 存储操作失败，**完整 Content 不会写入 SQL** |
| **ES 读取失败**（消费重试、反序列化） | 抛出异常，CAP 按原有机制重试 |
| **ES 删除失败** | 记录 Warning，**SQL 删除继续** |
| **Dashboard 列表 ES 还原失败** | 记录 Warning，列表仍显示 envelope 原文 |

## 结构化日志

便于检索与告警的事件名：

| 事件名 | 含义 |
|--------|------|
| `CapElasticOffloadWriteFailed` | ES 写入失败 |
| `CapElasticOffloadReadFailed` | ES 读取失败 |
| `CapElasticOffloadDeleteFailed` | ES 删除失败 |

## 使用注意事项

1. **Topic 名称必须精确匹配**  
   配置的是 CAP 消息的 `name`（Topic 名），不是 Kafka/RabbitMQ 的物理队列名。

2. **仅支持 SqlServer 存储**  
   本库装饰 `SqlServerDataStorage`；使用 PostgreSQL、MySQL 等 CAP 存储时需选用对应方案。

3. **不要手动修改 DB Content 列**  
   外置消息的 Content 必须是 envelope 格式；手工写入会导致 ES 无法还原。

4. **Dashboard 按 Content 关键字搜索**  
   搜索仍匹配数据库中的 envelope，**无法**按 ES 内正文全文检索。如需搜正文，请走 ES 查询。

5. **与 CAP 升级**  
   保持 `DotNetCore.CAP` / `DotNetCore.CAP.SqlServer` 主版本与本库一致（当前为 10.x）。

## 常见问题

**Q：已有历史大消息在 SQL 里，启用后会自动迁移吗？**  
A：不会。仅 **启用之后新写入** 的指定 Topic 消息走 ES。历史数据需自行迁移或等待过期清理。

**Q：能否按消息大小而非 Topic 外置？**  
A：当前版本仅支持 **按 Topic 名单** 外置。若需按字节阈值，可在业务层拆分 Topic 或提 Issue。

**Q：ES 宕机时 CAP 还能工作吗？**  
A：外置 Topic 的写入/读取会失败并触发 CAP 重试；非外置 Topic 不受影响。请保证 ES 高可用。

**Q：多实例部署需要注意什么？**  
A：与原版 CAP 相同；各实例共享同一 ES 集群与 SqlServer 即可，无额外分布式锁。

## 许可证

MIT

## 相关链接

- [NuGet: AdiCapSqlServer.Es](https://www.nuget.org/packages/AdiCapSqlServer.Es)
- [NuGet: AdiElasticSugar.Core](https://www.nuget.org/packages/AdiElasticSugar.Core)
- [DotNetCore.CAP 文档](https://cap.dotnetcore.xyz/)
