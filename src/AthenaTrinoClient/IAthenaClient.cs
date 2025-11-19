using AthenaTrinoClient.Models;

namespace AthenaTrinoClient;

public interface IAthenaClient
{
    Task<List<T>> Query<T>(FormattableString query, CancellationToken cancellationToken = default);
    Task<UnloadResponse> Unload(
        FormattableString query,
        string s3RelativePath,
        CancellationToken cancellationToken = default
    );
}
