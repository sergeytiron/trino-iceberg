using TrinoClient;
using Xunit.Abstractions;

namespace TrinoIcebergTests;

/// <summary>
/// Integration tests for TrinoQueryClient against a real Trino stack
/// </summary>
public class TrinoClientIntegrationTests : IClassFixture<TrinoIcebergStackFixture>
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _fixture;
    private TrinoIcebergStack Stack => _fixture.Stack;

    public TrinoClientIntegrationTests(ITestOutputHelper output, TrinoIcebergStackFixture fixture)
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
    public async Task TrinoClient_CanCreateSchema()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Act
        var results = await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");

        // Assert
        _output.WriteLine($"Create schema results: {results.Count} rows");
        Assert.NotNull(results);
    }

    [Fact]
    public async Task TrinoClient_CanCreateTableAndInsertData()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Create schema
        await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");

        // Act - Create table
        await client.ExecuteQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.test_data (id int, value varchar) WITH (format='PARQUET')");

        // Act - Insert data
        await client.ExecuteQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.test_data VALUES (100, 'test'), (200, 'data')");

        // Assert - Query to verify
        var results = await client.ExecuteQueryAsync(
            $"SELECT * FROM iceberg.{schemaName}.test_data ORDER BY id");

        _output.WriteLine($"Query returned {results.Count} rows");
        Assert.Equal(2, results.Count);
        
        // Verify first row
        Assert.Equal(2, results[0].Count);
        Assert.Equal(100, Convert.ToInt32(results[0][0]));
        Assert.Equal("test", results[0][1]);
        
        // Verify second row
        Assert.Equal(2, results[1].Count);
        Assert.Equal(200, Convert.ToInt32(results[1][0]));
        Assert.Equal("data", results[1][1]);
    }

    [Fact]
    public async Task TrinoClient_CanExecuteSelectQuery()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Setup test data
        await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
        await client.ExecuteQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.numbers (n int) WITH (format='PARQUET')");
        await client.ExecuteQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.numbers VALUES (1), (2), (3), (4), (5)");

        // Act
        var results = await client.ExecuteQueryAsync(
            $"SELECT n FROM iceberg.{schemaName}.numbers WHERE n > 2 ORDER BY n");

        // Assert
        _output.WriteLine($"Query returned {results.Count} rows");
        Assert.Equal(3, results.Count);
        Assert.Equal(3, Convert.ToInt32(results[0][0]));
        Assert.Equal(4, Convert.ToInt32(results[1][0]));
        Assert.Equal(5, Convert.ToInt32(results[2][0]));
    }

    [Fact]
    public async Task TrinoClient_CanExecuteAggregateQuery()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Setup test data
        await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
        await client.ExecuteQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.sales (amount bigint, category varchar) WITH (format='PARQUET')");
        await client.ExecuteQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.sales VALUES (100, 'A'), (200, 'B'), (150, 'A'), (300, 'B')");

        // Act
        var results = await client.ExecuteQueryAsync(
            $"SELECT category, SUM(amount) as total FROM iceberg.{schemaName}.sales GROUP BY category ORDER BY category");

        // Assert
        _output.WriteLine($"Aggregate query returned {results.Count} rows");
        Assert.Equal(2, results.Count);
        
        // Category A: 100 + 150 = 250
        Assert.Equal("A", results[0][0]);
        Assert.Equal(250, Convert.ToInt64(results[0][1]));
        
        // Category B: 200 + 300 = 500
        Assert.Equal("B", results[1][0]);
        Assert.Equal(500, Convert.ToInt64(results[1][1]));
    }

    [Fact]
    public async Task TrinoClient_CanExecuteCountQuery()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Setup test data
        await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
        await client.ExecuteQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.items (id int) WITH (format='PARQUET')");
        await client.ExecuteQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.items VALUES (1), (2), (3), (4), (5), (6), (7)");

        // Act
        var results = await client.ExecuteQueryAsync(
            $"SELECT COUNT(*) as total FROM iceberg.{schemaName}.items");

        // Assert
        _output.WriteLine($"Count query returned {results.Count} rows");
        Assert.Single(results);
        Assert.Single(results[0]);
        Assert.Equal(7, Convert.ToInt64(results[0][0]));
    }

    [Fact]
    public async Task TrinoClient_HandlesEmptyResultSet()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Setup test data
        await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
        await client.ExecuteQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.empty_table (id int) WITH (format='PARQUET')");

        // Act
        var results = await client.ExecuteQueryAsync(
            $"SELECT * FROM iceberg.{schemaName}.empty_table");

        // Assert
        _output.WriteLine($"Empty query returned {results.Count} rows");
        Assert.Empty(results);
    }

    [Fact]
    public async Task TrinoClient_CanExecuteMultipleQueriesSequentially()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Setup
        await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
        await client.ExecuteQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.counter (value int) WITH (format='PARQUET')");

        // Act - Execute multiple inserts
        await client.ExecuteQueryAsync($"INSERT INTO iceberg.{schemaName}.counter VALUES (1)");
        await client.ExecuteQueryAsync($"INSERT INTO iceberg.{schemaName}.counter VALUES (2)");
        await client.ExecuteQueryAsync($"INSERT INTO iceberg.{schemaName}.counter VALUES (3)");

        // Query final state
        var results = await client.ExecuteQueryAsync(
            $"SELECT SUM(value) as total FROM iceberg.{schemaName}.counter");

        // Assert
        _output.WriteLine($"Sequential queries result: {results.Count} rows");
        Assert.Single(results);
        Assert.Equal(6, Convert.ToInt32(results[0][0])); // 1 + 2 + 3 = 6
    }

    [Fact]
    public async Task TrinoClient_ThrowsExceptionForInvalidSQL()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");

        // Act & Assert
        await Assert.ThrowsAsync<TrinoQueryException>(async () =>
            await client.ExecuteQueryAsync("SELECT * FROM nonexistent.invalid.table"));
    }

    [Fact]
    public async Task TrinoClient_CanHandleComplexDataTypes()
    {
        // Arrange
        using var client = new TrinoQueryClient(Stack.TrinoEndpoint, "iceberg", "default");
        var schemaName = GetUniqueSchemaName("client_test");

        // Setup test data with various types
        await client.ExecuteQueryAsync(
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
        await client.ExecuteQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.mixed_types (id int, name varchar, amount bigint, active boolean) WITH (format='PARQUET')");
        await client.ExecuteQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.mixed_types VALUES (1, 'Alice', 1000, true), (2, 'Bob', 2000, false)");

        // Act
        var results = await client.ExecuteQueryAsync(
            $"SELECT * FROM iceberg.{schemaName}.mixed_types ORDER BY id");

        // Assert
        _output.WriteLine($"Mixed types query returned {results.Count} rows");
        Assert.Equal(2, results.Count);
        
        // First row
        Assert.Equal(1, Convert.ToInt32(results[0][0]));
        Assert.Equal("Alice", results[0][1]);
        Assert.Equal(1000, Convert.ToInt64(results[0][2]));
        Assert.True((bool)results[0][3]!);
        
        // Second row
        Assert.Equal(2, Convert.ToInt32(results[1][0]));
        Assert.Equal("Bob", results[1][1]);
        Assert.Equal(2000, Convert.ToInt64(results[1][2]));
        Assert.False((bool)results[1][3]!);
    }
}
