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
    /// <summary>
    /// Executes a parameterized query and returns a single scalar value.
    /// </summary>
    /// <typeparam name="T">The type of the scalar value to return.</typeparam>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The scalar value, or default(T) if no rows or null.</returns>
    Task<T?> QueryScalar<T>(FormattableString query, CancellationToken cancellationToken = default);
}
