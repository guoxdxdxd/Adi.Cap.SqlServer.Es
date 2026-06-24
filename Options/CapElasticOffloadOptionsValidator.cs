using Microsoft.Extensions.Options;

namespace Adi.Cap.SqlServer.Es.Options;

/// <summary>
/// 启动期校验 <see cref="CapElasticOffloadOptions"/> 必填项。
/// </summary>
public sealed class CapElasticOffloadOptionsValidator : IValidateOptions<CapElasticOffloadOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, CapElasticOffloadOptions options)
    {
        if (options.OffloadedTopicNames.Count == 0)
        {
            return ValidateOptionsResult.Fail(
                "CapElasticOffloadOptions.OffloadedTopicNames 不能为空，请在 UseCapElasticOffloadStorage 中显式注册 Topic。");
        }

        if (string.IsNullOrWhiteSpace(options.ElasticSearchClientName))
        {
            return ValidateOptionsResult.Fail(
                "CapElasticOffloadOptions.ElasticSearchClientName 未配置。");
        }

        return ValidateOptionsResult.Success;
    }
}
