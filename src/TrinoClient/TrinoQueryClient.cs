using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace TrinoClient;

/// <summary>
/// HTTP-based client for executing queries against a Trino server.
/// Uses Trino's REST API for query submission and result retrieval.
/// </summary>
public class TrinoQueryClient : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _trinoEndpoint;
    private readonly string _catalog;
    private readonly string _schema;
    private readonly bool _disposeHttpClient;

    /// <summary>
    /// Initializes a new instance of the TrinoQueryClient.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URL (e.g., http://localhost:8080)</param>
    /// <param name="catalog">The default catalog to use for queries (e.g., "iceberg")</param>
    /// <param name="schema">The default schema to use for queries (e.g., "default")</param>
    /// <param name="httpClient">Optional HttpClient instance. If not provided, a new one will be created and managed by this class.</param>
    public TrinoQueryClient(string trinoEndpoint, string catalog = "iceberg", string schema = "default", HttpClient? httpClient = null)
    {
        if (string.IsNullOrWhiteSpace(trinoEndpoint))
        {
            throw new ArgumentException("Trino endpoint cannot be null or empty", nameof(trinoEndpoint));
        }

        _trinoEndpoint = trinoEndpoint.TrimEnd('/');
        _catalog = catalog;
        _schema = schema;
        
        if (httpClient != null)
        {
            _httpClient = httpClient;
            _disposeHttpClient = false;
        }
        else
        {
            _httpClient = new HttpClient();
            _disposeHttpClient = true;
        }
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

        var results = new List<List<object?>>();

        // Submit the query
        var queryUri = await SubmitQueryAsync(sql, cancellationToken);

        // Poll for results
        await PollQueryResultsAsync(queryUri, results, cancellationToken);

        return results;
    }

    /// <summary>
    /// Executes a SQL query and returns the raw JSON response as a string.
    /// Useful for debugging or when you need access to metadata.
    /// </summary>
    /// <param name="sql">The SQL query to execute</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>The final query response as a JSON string</returns>
    public async Task<string> ExecuteQueryRawAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL query cannot be null or empty", nameof(sql));
        }

        var queryUri = await SubmitQueryAsync(sql, cancellationToken);
        return await GetFinalQueryResponseAsync(queryUri, cancellationToken);
    }

    private async Task<string> SubmitQueryAsync(string sql, CancellationToken cancellationToken)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, $"{_trinoEndpoint}/v1/statement");
        request.Headers.Add("X-Trino-Catalog", _catalog);
        request.Headers.Add("X-Trino-Schema", _schema);
        request.Headers.Add("X-Trino-User", "trino-client");
        request.Content = new StringContent(sql, Encoding.UTF8, "text/plain");

        var response = await _httpClient.SendAsync(request, cancellationToken);
        
        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new TrinoQueryException($"Failed to submit query. Status: {response.StatusCode}, Response: {errorContent}");
        }

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        var responseJson = JsonDocument.Parse(responseContent);
        
        // Check for query errors
        if (responseJson.RootElement.TryGetProperty("error", out var errorElement))
        {
            var errorMessage = errorElement.TryGetProperty("message", out var msgElement) 
                ? msgElement.GetString() 
                : "Unknown error";
            throw new TrinoQueryException($"Query failed: {errorMessage}");
        }

        // Get the nextUri to poll for results
        if (responseJson.RootElement.TryGetProperty("nextUri", out var nextUriElement))
        {
            return nextUriElement.GetString() ?? throw new TrinoQueryException("nextUri is null");
        }

        // If there's no nextUri, the query might be complete already (e.g., DDL statements)
        return string.Empty;
    }

    private async Task PollQueryResultsAsync(string nextUri, List<List<object?>> results, CancellationToken cancellationToken)
    {
        var currentUri = nextUri;

        while (!string.IsNullOrEmpty(currentUri))
        {
            var request = new HttpRequestMessage(HttpMethod.Get, currentUri);
            request.Headers.Add("X-Trino-User", "trino-client");

            var response = await _httpClient.SendAsync(request, cancellationToken);
            
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
                throw new TrinoQueryException($"Failed to fetch query results. Status: {response.StatusCode}, Response: {errorContent}");
            }

            var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
            var responseJson = JsonDocument.Parse(responseContent);

            // Check for errors
            if (responseJson.RootElement.TryGetProperty("error", out var errorElement))
            {
                var errorMessage = errorElement.TryGetProperty("message", out var msgElement) 
                    ? msgElement.GetString() 
                    : "Unknown error";
                throw new TrinoQueryException($"Query failed: {errorMessage}");
            }

            // Extract data if present
            if (responseJson.RootElement.TryGetProperty("data", out var dataElement) && dataElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var row in dataElement.EnumerateArray())
                {
                    var rowData = new List<object?>();
                    foreach (var column in row.EnumerateArray())
                    {
                        rowData.Add(ExtractJsonValue(column));
                    }
                    results.Add(rowData);
                }
            }

            // Get next URI or break if query is complete
            if (responseJson.RootElement.TryGetProperty("nextUri", out var nextUriElement))
            {
                currentUri = nextUriElement.GetString() ?? string.Empty;
            }
            else
            {
                break;
            }

            // Small delay to avoid overwhelming the server
            await Task.Delay(100, cancellationToken);
        }
    }

    private async Task<string> GetFinalQueryResponseAsync(string nextUri, CancellationToken cancellationToken)
    {
        var currentUri = nextUri;
        string? lastResponse = null;

        while (!string.IsNullOrEmpty(currentUri))
        {
            var request = new HttpRequestMessage(HttpMethod.Get, currentUri);
            request.Headers.Add("X-Trino-User", "trino-client");

            var response = await _httpClient.SendAsync(request, cancellationToken);
            
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
                throw new TrinoQueryException($"Failed to fetch query results. Status: {response.StatusCode}, Response: {errorContent}");
            }

            lastResponse = await response.Content.ReadAsStringAsync(cancellationToken);
            var responseJson = JsonDocument.Parse(lastResponse);

            // Check for errors
            if (responseJson.RootElement.TryGetProperty("error", out var errorElement))
            {
                var errorMessage = errorElement.TryGetProperty("message", out var msgElement) 
                    ? msgElement.GetString() 
                    : "Unknown error";
                throw new TrinoQueryException($"Query failed: {errorMessage}");
            }

            // Get next URI or break if query is complete
            if (responseJson.RootElement.TryGetProperty("nextUri", out var nextUriElement))
            {
                currentUri = nextUriElement.GetString() ?? string.Empty;
            }
            else
            {
                break;
            }

            await Task.Delay(100, cancellationToken);
        }

        return lastResponse ?? string.Empty;
    }

    private static object? ExtractJsonValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var longValue) ? longValue : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => element.EnumerateArray().Select(ExtractJsonValue).ToList(),
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => ExtractJsonValue(p.Value)),
            _ => element.ToString()
        };
    }

    /// <summary>
    /// Disposes the HttpClient if it was created by this instance.
    /// </summary>
    public void Dispose()
    {
        if (_disposeHttpClient)
        {
            _httpClient.Dispose();
        }
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
