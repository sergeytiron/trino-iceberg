using Xunit.Abstractions;

namespace TrinoIcebergTests;

/// <summary>
/// Integration tests for the Trino + Nessie + MinIO stack
/// </summary>
public class TrinoIcebergStackTests : IClassFixture<TrinoIcebergStackFixture>
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _fixture;
    private TrinoIcebergStack Stack => _fixture.Stack;

    public TrinoIcebergStackTests(ITestOutputHelper output, TrinoIcebergStackFixture fixture)
    {
        _output = output;
        _fixture = fixture;
    }

    /// <summary>
    /// Generates a unique schema name for test isolation
    /// </summary>
    private static string GetUniqueSchemaName(string baseName) =>
        $"{baseName}_{Guid.NewGuid():N}".ToLowerInvariant();

    [Fact]
    public async Task CanCreateSchemaInNessieCatalog()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("test_schema");

        // Act
        var result = await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");

        // Assert
        _output.WriteLine($"Result: {result}");
        Assert.Contains("CREATE SCHEMA", result);
    }

    [Fact]
    public async Task CanCreateAndQueryIcebergTable()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("demo");

        // Create schema
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");

        // Act - Create table
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE IF NOT EXISTS iceberg.{schemaName}.test_numbers (id int, name varchar) WITH (format='PARQUET')");

        // Act - Insert data
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.test_numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')");

        // Act - Query data
        var result = await Stack.ExecuteTrinoQueryAsync(
            $"SELECT * FROM iceberg.{schemaName}.test_numbers ORDER BY id");

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
        var schemaName = GetUniqueSchemaName("analytics");

        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");

        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE IF NOT EXISTS iceberg.{schemaName}.events (event_id bigint, event_type varchar, timestamp timestamp) WITH (format='PARQUET')");

        // Act
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.events VALUES " +
            "(1, 'click', TIMESTAMP '2025-11-15 10:00:00'), " +
            "(2, 'view', TIMESTAMP '2025-11-15 10:05:00'), " +
            "(3, 'click', TIMESTAMP '2025-11-15 10:10:00')");

        var countResult = await Stack.ExecuteTrinoQueryAsync(
            $"SELECT COUNT(*) as total FROM iceberg.{schemaName}.events");

        var groupResult = await Stack.ExecuteTrinoQueryAsync(
            $"SELECT event_type, COUNT(*) as count FROM iceberg.{schemaName}.events GROUP BY event_type ORDER BY event_type");

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
        var response = await client.GetAsync($"{Stack.TrinoEndpoint}/v1/info");

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
        var response = await client.GetAsync($"{Stack.NessieEndpoint}/api/v2/config");

        // Assert
        response.EnsureSuccessStatusCode();
        var content = await response.Content.ReadAsStringAsync();
        _output.WriteLine($"Nessie config: {content}");
        Assert.NotEmpty(content);
    }
}