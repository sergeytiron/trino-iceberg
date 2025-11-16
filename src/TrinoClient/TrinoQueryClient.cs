using Trino.Client;

namespace TrinoClient;

/// <summary>
/// Simplified wrapper around the official Trino C# client for executing queries against a Trino server.
/// This class provides a simple interface for common query operations using the official Trino.Client library.
/// See: https://github.com/trinodb/trino-csharp-client
/// </summary>
public class TrinoQueryClient : IDisposable
{
    private readonly ClientSession _session;

    /// <summary>
    /// Initializes a new instance of the TrinoQueryClient using the official Trino.Client library.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URL (e.g., http://localhost:8080)</param>
    /// <param name="catalog">The default catalog to use for queries (e.g., "iceberg")</param>
    /// <param name="schema">The default schema to use for queries (e.g., "default")</param>
    public TrinoQueryClient(string trinoEndpoint, string catalog = "iceberg", string schema = "default")
    {
        if (string.IsNullOrWhiteSpace(trinoEndpoint))
        {
            throw new ArgumentException("Trino endpoint cannot be null or empty", nameof(trinoEndpoint));
        }

        var serverUri = new Uri(trinoEndpoint.TrimEnd('/'));
        
        var sessionProperties = new ClientSessionProperties
        {
            Server = serverUri,
            Catalog = catalog,
            Schema = schema
        };

        _session = new ClientSession(sessionProperties: sessionProperties, auth: null);
    }

    /// <summary>
    /// Executes a SQL query against Trino and returns all results as a list of rows.
    /// </summary>
    /// <param name="sql">The SQL query to execute</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>A list of rows, where each row is a list of column values</returns>
    /// <exception cref="ArgumentException">Thrown when SQL is null or empty</exception>
    /// <exception cref="TrinoQueryException">Thrown when the query fails</exception>
    public async Task<List<List<object?>>> ExecuteQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL query cannot be null or empty", nameof(sql));
        }

        try
        {
            var recordExecutor = await RecordExecutor.Execute(
                session: _session,
                statement: sql)
                .ConfigureAwait(false);

            var results = new List<List<object?>>();
            
            foreach (var row in recordExecutor)
            {
                var rowData = new List<object?>();
                foreach (var value in row)
                {
                    rowData.Add(value);
                }
                results.Add(rowData);
            }

            return results;
        }
        catch (Exception ex) when (ex is not ArgumentException)
        {
            throw new TrinoQueryException($"Query execution failed: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Disposes the client. The client session does not require disposal.
    /// </summary>
    public void Dispose()
    {
        // ClientSession does not implement IDisposable, so nothing to dispose
    }
}

/// <summary>
/// Exception thrown when a Trino query fails.
/// </summary>
public class TrinoQueryException : Exception
{
    public TrinoQueryException(string message) : base(message)
    {
    }

    public TrinoQueryException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
