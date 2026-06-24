using Elastic.Clients.Elasticsearch;

namespace Adi.Cap.SqlServer.Es.Elastic;

/// <summary>
/// CAP ES 外置存储的 Elasticsearch 客户端来源抽象。
/// 由宿主注册实现（通常包装现有 ES 客户端工厂），本库不创建 ES 连接。
/// </summary>
public interface ICapElasticClientProvider
{
    /// <summary>
    /// 获取已配置的 Elasticsearch 客户端。
    /// </summary>
    /// <returns>客户端实例。</returns>
    /// <exception cref="InvalidOperationException">客户端未注册或名称不匹配时抛出。</exception>
    ElasticsearchClient GetClient();
}
