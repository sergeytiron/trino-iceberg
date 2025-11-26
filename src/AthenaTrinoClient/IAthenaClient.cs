using AthenaTrinoClient.Models;

namespace AthenaTrinoClient;

public interface IAthenaClient
{
    Task<List<T>> QueryAsync<T>(FormattableString query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a query and unloads the results to S3 in Parquet format.
    /// Data files are placed directly at the specified path (no metadata subfolder).
    /// Requires an S3 client to be configured via the AthenaClient constructor.
    /// </summary>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="s3RelativePath">The relative S3 path within the warehouse bucket (e.g., "exports/data").</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>An UnloadResponse containing the row count and absolute S3 path.</returns>
    Task<UnloadResponse> UnloadAsync(
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
    Task<T?> QueryScalarAsync<T>(FormattableString query, CancellationToken cancellationToken = default);
}
