using AthenaTrinoClient.Execution;
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
    private readonly ISqlParameterFormatter _parameterFormatter;
    private readonly IQueryResultMapper _resultMapper;
    private readonly IQueryExecutor _queryExecutor;

    /// <summary>
    /// Creates a new TrinoClient with the specified connection parameters.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URI.</param>
    /// <param name="catalog">The default catalog to use.</param>
    /// <param name="schema">The default schema to use.</param>
    /// <param name="parameterFormatter">Optional SQL parameter formatter.</param>
    /// <param name="resultMapper">Optional query result mapper.</param>
    /// <param name="queryExecutor">Optional query executor.</param>
    public AthenaClient(
        Uri trinoEndpoint,
        string catalog,
        string schema,
        ISqlParameterFormatter? parameterFormatter = null,
        IQueryResultMapper? resultMapper = null,
        IQueryExecutor? queryExecutor = null
    )
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
        _parameterFormatter = parameterFormatter ?? new SqlParameterFormatter();
        _resultMapper = resultMapper ?? new QueryResultMapper();
        _queryExecutor = queryExecutor ?? new QueryExecutor();
    }

    /// <summary>
    /// Executes a SQL statement and returns the QueryResult for processing results.
    /// </summary>
    /// <param name="sql">The SQL statement to execute (with all parameters already inlined).</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A QueryResult for processing query results.</returns>
    private async Task<QueryResult> ExecuteStatement(
        string sql,
        CancellationToken cancellationToken = default
    )
    {
        return await _queryExecutor.Execute(_session, sql, cancellationToken);
    }

    /// <summary>
    /// Extracts the row count from a query result (typically from CREATE TABLE AS or similar statements).
    /// </summary>
    /// <param name="result">The QueryResult containing the result.</param>
    /// <returns>The row count, or 0 if no count was found.</returns>
    private int ExtractRowCountFromResult(QueryResult result)
    {
        foreach (var row in result.Rows)
        {
            if (row.Count > 0 && row[0] != null)
            {
                return Convert.ToInt32(row[0]);
            }
        }
        return 0;
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
        var result = await ExecuteStatement(sql, cancellationToken);
        return _resultMapper.DeserializeResults<T>(result.Rows, result.Columns);
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

            var createExecutor = await ExecuteStatement(createTableSql, cancellationToken);
            var rowCount = ExtractRowCountFromResult(createExecutor);

            // Step 2: Drop the temporary table (keeps the data files in S3)
            try
            {
                var dropTableSql = $"DROP TABLE {exportTableName}";
                var dropExecutor = await ExecuteStatement(dropTableSql, cancellationToken);

                // Consume the results
                foreach (var _ in dropExecutor.Rows) { }
            }
            catch
            {
                // If drop fails, log but don't fail the operation
                // The table will be cleaned up eventually
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
