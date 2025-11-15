using Xunit.Abstractions;

namespace TrinoIcebergTests;

/// <summary>
/// Integration tests for the Trino + Nessie + MinIO stack
/// </summary>
public class TrinoIcebergStackTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private TrinoIcebergStack? _stack;

    public TrinoIcebergStackTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async Task InitializeAsync()
    {
        _stack = new TrinoIcebergStack();
        await _stack.StartAsync();
        
        _output.WriteLine($"Stack started successfully!");
        _output.WriteLine($"Trino: {_stack.TrinoEndpoint}");
        _output.WriteLine($"Nessie: {_stack.NessieEndpoint}");
        _output.WriteLine($"MinIO API: {_stack.MinioEndpoint}");
        _output.WriteLine($"MinIO Console: {_stack.MinioConsoleEndpoint}");
    }

    public async Task DisposeAsync()
    {
        if (_stack != null)
        {
            await _stack.DisposeAsync();
        }
    }

    [Fact]
    public async Task CanCreateSchemaInNessieCatalog()
    {
        // Act
        var result = await _stack!.ExecuteTrinoQueryAsync(
            "CREATE SCHEMA IF NOT EXISTS iceberg.test_schema WITH (location='s3://warehouse/test_schema/')");

        // Assert
        _output.WriteLine($"Result: {result}");
        Assert.Contains("CREATE SCHEMA", result);
    }

    [Fact]
    public async Task CanCreateAndQueryIcebergTable()
    {
        // Arrange - Create schema
        await _stack!.ExecuteTrinoQueryAsync(
            "CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/')");

        // Act - Create table
        await _stack.ExecuteTrinoQueryAsync(
            "CREATE TABLE IF NOT EXISTS iceberg.demo.test_numbers (id int, name varchar) WITH (format='PARQUET')");

        // Act - Insert data
        await _stack.ExecuteTrinoQueryAsync(
            "INSERT INTO iceberg.demo.test_numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')");

        // Act - Query data
        var result = await _stack.ExecuteTrinoQueryAsync(
            "SELECT * FROM iceberg.demo.test_numbers ORDER BY id");

        // Assert
        _output.WriteLine($"Query result:\n{result}");
        Assert.Contains("\"1\"", result);
        Assert.Contains("\"one\"", result);
        Assert.Contains("\"2\"", result);
        Assert.Contains("\"two\"", result);
        Assert.Contains("\"3\"", result);
        Assert.Contains("\"three\"", result);
    }

    [Fact]
    public async Task CanExecuteMultipleQueries()
    {
        // Arrange
        await _stack!.ExecuteTrinoQueryAsync(
            "CREATE SCHEMA IF NOT EXISTS iceberg.analytics WITH (location='s3://warehouse/analytics/')");

        await _stack.ExecuteTrinoQueryAsync(
            "CREATE TABLE IF NOT EXISTS iceberg.analytics.events (event_id bigint, event_type varchar, timestamp timestamp) WITH (format='PARQUET')");

        // Act
        await _stack.ExecuteTrinoQueryAsync(
            "INSERT INTO iceberg.analytics.events VALUES " +
            "(1, 'click', TIMESTAMP '2025-11-15 10:00:00'), " +
            "(2, 'view', TIMESTAMP '2025-11-15 10:05:00'), " +
            "(3, 'click', TIMESTAMP '2025-11-15 10:10:00')");

        var countResult = await _stack.ExecuteTrinoQueryAsync(
            "SELECT COUNT(*) as total FROM iceberg.analytics.events");

        var groupResult = await _stack.ExecuteTrinoQueryAsync(
            "SELECT event_type, COUNT(*) as count FROM iceberg.analytics.events GROUP BY event_type ORDER BY event_type");

        // Assert
        _output.WriteLine($"Count result: {countResult}");
        _output.WriteLine($"Group result: {groupResult}");
        
        Assert.Contains("\"3\"", countResult);
        Assert.Contains("\"click\"", groupResult);
        Assert.Contains("\"view\"", groupResult);
    }

    [Fact]
    public async Task TrinoHealthCheckPasses()
    {
        // Arrange
        using var client = new HttpClient();

        // Act
        var response = await client.GetAsync($"{_stack!.TrinoEndpoint}/v1/info");

        // Assert
        response.EnsureSuccessStatusCode();
        var content = await response.Content.ReadAsStringAsync();
        _output.WriteLine($"Trino info: {content}");
        Assert.Contains("\"starting\":false", content.ToLower());
    }

    [Fact]
    public async Task NessieHealthCheckPasses()
    {
        // Arrange
        using var client = new HttpClient();

        // Act
        var response = await client.GetAsync($"{_stack!.NessieEndpoint}/api/v2/config");

        // Assert
        response.EnsureSuccessStatusCode();
        var content = await response.Content.ReadAsStringAsync();
        _output.WriteLine($"Nessie config: {content}");
        Assert.NotEmpty(content);
    }
}
