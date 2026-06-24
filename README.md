# Adi.Cap.SqlServer.Es

CAP `IDataStorage` decorator for SqlServer storage: offload large message `Content` to Elasticsearch while keeping a reference envelope in the database.

Uses [Adi.ElasticSugar.Core](https://www.nuget.org/packages/AdiElasticSugar.Core) for document push (`PushDocumentAsync`) and index auto-creation.

## Reference

```xml
<PackageReference Include="AdiElasticSugar.Core" Version="1.1.6" />
<PackageReference Include="AdiCapSqlServer.Es" Version="1.0.0" />
```

## Host responsibilities

1. Register **`ICapElasticClientProvider`** before `AddCap` (resolve `ElasticsearchClient` from your existing ES factory).
2. Call **`UseSqlServer`**, then **`UseCapElasticOffloadStorage`** inside `AddCap`.

```csharp
// Example: B2B registers ElasticSearchFactory + ICapElasticClientProvider in Infrastructure module.

context.Services.AddCap(options =>
{
    options.UseSqlServer(sql => { /* connection & schema */ });

    options.UseCapElasticOffloadStorage(offload =>
    {
        offload.OffloadedTopicNames.Add("your-cap-topic-name");
        offload.ElasticSearchClientName = "Data";
    });

    options.UseKafka(/* ... */);
});
```

`CapElasticOffloadOptions` has **no defaults**. Only `OffloadedTopicNames` and `ElasticSearchClientName` are required.

## ES index

Documents use `[EsIndex("cap-message", YearMonth)]` → index `cap-message-{yyyy-MM}` (UTC).

DB envelope:

```json
{"$capEs":"cap-message-2026-06/{capMessageId}"}
```

## fail-fast

- **ES write failure**: storage fails; full Content is not written to SQL.
- **ES read failure**: throws; CAP retry may reschedule the message.
- **ES delete failure**: logs warning; SQL delete continues.

## Structured log event codes

`CapElasticOffloadWriteFailed`, `CapElasticOffloadReadFailed`, `CapElasticOffloadDeleteFailed`
