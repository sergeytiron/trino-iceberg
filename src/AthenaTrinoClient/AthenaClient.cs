using AthenaTrinoClient.Formatting;
using AthenaTrinoClient.Mapping;
using AthenaTrinoClient.Models;
using S3Client;
using Trino.Client;

namespace AthenaTrinoClient;

/// <summary>
/// Client for executing Trino queries with type-safe deserialization and parameterization support.
/// </summary>
public class AthenaClient : IAthenaClient
{
    private readonly ClientSession _session;
    private readonly SqlParameterFormatter _parameterFormatter;
    private readonly QueryResultMapper _resultMapper;
    private readonly IS3Client? _s3Client;

    /// <summary>
    /// Creates a new TrinoClient with the specified connection parameters.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URI.</param>
    /// <param name="catalog">The default catalog to use.</param>
    /// <param name="schema">The default schema to use.</param>
    public AthenaClient(Uri trinoEndpoint, string catalog, string schema)
        : this(trinoEndpoint, catalog, schema, null)
    {
    }

    /// <summary>
    /// Creates a new TrinoClient with the specified connection parameters and S3 client.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URI.</param>
    /// <param name="catalog">The default catalog to use.</param>
    /// <param name="schema">The default schema to use.</param>
    /// <param name="s3Client">The S3 client for UnloadAsync operations. Required for UnloadAsync to work.</param>
    public AthenaClient(Uri trinoEndpoint, string catalog, string schema, IS3Client? s3Client)
    {
        _session = new ClientSession(
            sessionProperties: new ClientSessionProperties
            {
                Server = trinoEndpoint,
                Catalog = catalog,
                Schema = schema
            },
            auth: null
        );
        _parameterFormatter = new SqlParameterFormatter();
        _resultMapper = new QueryResultMapper();
        _s3Client = s3Client;
    }

    /// <summary>
    /// Executes a parameterized query and returns results deserialized into a list of typed objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each row into.</typeparam>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A list of deserialized objects of type T.</returns>
    public async Task<List<T>> QueryAsync<T>(FormattableString query, CancellationToken cancellationToken = default)
    {
        var sql = _parameterFormatter.ConvertFormattableStringToParameterizedQuery(query);
        var executor = await ExecuteStatementAsync(sql, cancellationToken);
        return _resultMapper.DeserializeResults<T>(executor, executor.Records.Columns);
    }

    /// <summary>
    /// Executes a parameterized query and returns a single scalar value.
    /// </summary>
    /// <typeparam name="T">The type of the scalar value to return.</typeparam>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The scalar value, or default(T) if no rows or null.</returns>
    public async Task<T?> QueryScalarAsync<T>(FormattableString query, CancellationToken cancellationToken = default)
    {
        var sql = _parameterFormatter.ConvertFormattableStringToParameterizedQuery(query);
        var executor = await ExecuteStatementAsync(sql, cancellationToken);

        foreach (var row in executor)
        {
            if (row.Count > 0 && row[0] != null)
            {
                return ConvertScalarValue<T>(row[0]);
            }
            return default;
        }

        return default;
    }

    /// <summary>
    /// Converts a scalar value to the target type, handling nullable types and special types.
    /// </summary>
    private static T ConvertScalarValue<T>(object value)
    {
        var targetType = typeof(T);
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Handle Guid specially since Convert.ChangeType doesn't support it
        if (underlyingType == typeof(Guid))
        {
            return (T)(object)Guid.Parse(value.ToString()!);
        }

        return (T)Convert.ChangeType(value, underlyingType);
    }

    /// <summary>
    /// Executes a query and unloads the results to S3 in Parquet format.
    /// Creates a temporary Iceberg table, then copies only the data files (not metadata) to the target path.
    /// </summary>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="s3RelativePath">The relative S3 path within the warehouse bucket (e.g., "exports/data").</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>An UnloadResponse containing the row count and absolute S3 path.</returns>
    /// <exception cref="InvalidOperationException">Thrown when S3 client is not configured or operation fails.</exception>
    public async Task<UnloadResponse> UnloadAsync(
        FormattableString query,
        string s3RelativePath,
        CancellationToken cancellationToken = default
    )
    {
        if (_s3Client is null)
        {
            throw new InvalidOperationException(
                "S3 client is required for UnloadAsync. Use the constructor that accepts IS3Client."
            );
        }

        var sql = _parameterFormatter.ConvertFormattableStringToParameterizedQuery(query);

        // Generate a unique table name and temp path for the export
        var timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
        var guid = Guid.NewGuid().ToString("N")[..8];
        var exportTableName = $"unload_temp_{timestamp}_{guid}";
        var tempPath = $"_unload_temp/{exportTableName}";
        var absoluteTempPath = $"s3://warehouse/{tempPath}";
        var absoluteTargetPath = $"s3://warehouse/{s3RelativePath}";

        try
        {
            // Step 1: Create a temporary table with the query results in temp location
            var createTableSql =
                $"""
                CREATE TABLE {exportTableName}
                WITH (location = '{absoluteTempPath}', format = 'PARQUET')
                AS {sql}
                """;

            var createExecutor = await ExecuteStatementAsync(createTableSql, cancellationToken);

            // Extract row count from first row
            var rowCount = 0;
            foreach (var row in createExecutor)
            {
                if (row.Count > 0 && row[0] != null)
                {
                    rowCount = Convert.ToInt32(row[0]);
                    break;
                }
            }

            // Step 2: Copy only data files from temp/data/ to target path
            var dataPrefix = $"{tempPath}/data/";
            var dataFiles = await _s3Client.ListFilesAsync(dataPrefix, cancellationToken);

            foreach (var file in dataFiles)
            {
                // Get just the filename from the data folder
                var fileName = file.Key[(file.Key.LastIndexOf('/') + 1)..];
                var targetKey = s3RelativePath.TrimEnd('/') + "/" + fileName;
                await _s3Client.CopyObjectAsync(file.Key, targetKey, cancellationToken);
            }

            // Step 3: Drop the temporary table (keeps the data files in temp location)
            try
            {
                await ExecuteStatementAsync($"DROP TABLE {exportTableName}", cancellationToken);
            }
            catch
            {
                // If drop fails, the table will be cleaned up eventually
            }

            // Step 4: Clean up temp files
            try
            {
                var allTempFiles = await _s3Client.ListFilesAsync(tempPath, cancellationToken);
                if (allTempFiles.Count > 0)
                {
                    await _s3Client.DeleteObjectsAsync(allTempFiles.Select(f => f.Key), cancellationToken);
                }
            }
            catch
            {
                // If cleanup fails, temp files will remain but won't affect the result
            }

            return new UnloadResponse(rowCount, absoluteTargetPath);
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            throw new InvalidOperationException(
                $"Failed to unload query results to '{absoluteTargetPath}'. "
                    + "Ensure the S3 bucket is accessible and the query is valid.",
                ex
            );
        }
    }

    /// <summary>
    /// Executes a SQL statement and returns the record executor for processing results.
    /// </summary>
    private async Task<RecordExecutor> ExecuteStatementAsync(string sql, CancellationToken cancellationToken = default)
    {
        return await RecordExecutor.Execute(
            logger: null,
            queryStatusNotifications: null,
            session: _session,
            statement: sql,
            queryParameters: null,
            bufferSize: Constants.DefaultBufferSizeBytes,
            isQuery: true,
            cancellationToken: cancellationToken
        );
    }
}
