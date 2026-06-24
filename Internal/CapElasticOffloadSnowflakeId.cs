using Adi.Cap.SqlServer.Es.Context;
using DotNetCore.CAP.Internal;

namespace Adi.Cap.SqlServer.Es.Internal;

/// <summary>
/// CAP Snowflake 装饰器：写入作用域内多次 <see cref="NextId"/> 返回同一 Id，保证 Received/异常入库与 ES 引用一致。
/// </summary>
internal sealed class CapElasticOffloadSnowflakeId : ISnowflakeId
{
    private readonly ISnowflakeId _inner;

    /// <summary>
    /// 创建 Snowflake 装饰器。
    /// </summary>
    /// <param name="inner">CAP 原生 Snowflake 实现。</param>
    public CapElasticOffloadSnowflakeId(ISnowflakeId inner)
    {
        _inner = inner;
    }

    /// <inheritdoc />
    public long NextId()
    {
        var scope = CapElasticOffloadWriteContext.Current;
        if (scope == null)
        {
            return _inner.NextId();
        }

        if (scope.CapMessageId == null)
        {
            scope.CapMessageId = _inner.NextId().ToString();
        }

        return long.Parse(scope.CapMessageId);
    }
}
