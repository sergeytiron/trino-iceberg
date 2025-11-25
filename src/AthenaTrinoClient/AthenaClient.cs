using AthenaTrinoClient.Formatting;
using AthenaTrinoClient.Mapping;
using AthenaTrinoClient.Models;
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

    /// <summary>
    /// Creates a new TrinoClient with the specified connection parameters.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URI.</param>
    /// <param name="catalog">The default catalog to use.</param>
    /// <param name="schema">The default schema to use.</param>
    public AthenaClient(Uri trinoEndpoint, string catalog, string schema)
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

    /// <summary>
    /// Executes a parameterized query and returns results deserialized into a list of typed objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each row into.</typeparam>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A list of deserialized objects of type T.</returns>
    public async Task<List<T>> Query<T>(FormattableString query, CancellationToken cancellationToken = default)
    {
        var sql = _parameterFormatter.ConvertFormattableStringToParameterizedQuery(query);
        var executor = await ExecuteStatementAsync(sql, cancellationToken);
        return _resultMapper.DeserializeResults<T>(executor, executor.Records.Columns);
    }

    /// <summary>
    /// Executes a query and unloads the results to S3 in Parquet format.
    /// Mimics AWS Athena UNLOAD by creating a temporary Iceberg table and using INSERT INTO.
    /// </summary>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="s3RelativePath">The relative S3 path within the warehouse bucket (e.g., "exports/data").</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>An UnloadResponse containing the row count and absolute S3 path.</returns>
    public async Task<UnloadResponse> Unload(
        FormattableString query,
        string s3RelativePath,
        CancellationToken cancellationToken = default
    )
    {
        var sql = _parameterFormatter.ConvertFormattableStringToParameterizedQuery(query);

        // Generate a unique table name for the export
        var timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
        var guid = Guid.NewGuid().ToString("N")[..8];
        var exportTableName = $"unload_temp_{timestamp}_{guid}";
        var absolutePath = $"s3://warehouse/{s3RelativePath}";

        try
        {
            // Step 1: Create a temporary table with the query results location
            var createTableSql =
                $"""
                CREATE TABLE {exportTableName}
                WITH (location = '{absolutePath}', format = 'PARQUET')
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

            // Step 2: Drop the temporary table (keeps the data files in S3)
            try
            {
                await ExecuteStatementAsync($"DROP TABLE {exportTableName}", cancellationToken);
            }
            catch
            {
                // If drop fails, the table will be cleaned up eventually
            }

            return new UnloadResponse(rowCount, absolutePath);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to unload query results to '{absolutePath}'. "
                    + "Ensure the S3 bucket is accessible and the query is valid.",
                ex
            );
        }
    }
}
