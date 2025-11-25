using Trino.Client;

namespace IntegrationTests;

/// <summary>
/// Class fixture for TrinoClientIntegrationTests - uses the shared common schema from assembly fixture.
/// No additional setup queries needed - reuses pre-populated test data.
/// </summary>
public sealed class TrinoClientIntegrationTestsClassFixture : IAsyncLifetime
{
    private readonly TrinoIcebergStackFixture _stackFixture;
    
    /// <summary>
    /// Uses the shared schema from assembly fixture - no separate schema creation needed.
    /// </summary>
    public string SchemaName => _stackFixture.CommonSchemaName;
    public TrinoIcebergStack Stack => _stackFixture.Stack;

    public TrinoClientIntegrationTestsClassFixture(TrinoIcebergStackFixture stackFixture)
    {
        _stackFixture = stackFixture;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Integration tests for official Trino.Client against a real Trino stack
/// </summary>
public class TrinoClientIntegrationTests : IClassFixture<TrinoClientIntegrationTestsClassFixture>
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoClientIntegrationTestsClassFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.SchemaName;

    public TrinoClientIntegrationTests(ITestOutputHelper output, TrinoClientIntegrationTestsClassFixture classFixture)
    {
        _output = output;
        _classFixture = classFixture;
    }

    /// <summary>
    /// Generates a unique schema name for tests that need isolated schemas
    /// </summary>
    private static string GetUniqueSchemaName(string baseName) => $"{baseName}_{Guid.NewGuid():N}".ToLowerInvariant();

    /// <summary>
    /// Helper to execute a query and return results as a list
    /// </summary>
    private static async Task<List<List<object>>> ExecuteQueryAsync(ClientSession session, string sql)
    {
        var executor = await RecordExecutor.Execute(session, sql);
        var results = new List<List<object>>();
        foreach (var row in executor)
        {
            results.Add(row);
        }
        return results;
    }

    /// <summary>
    /// Create a client session with a specific schema
    /// </summary>
    private ClientSession CreateSession(string schemaName)
    {
        var sessionProperties = new ClientSessionProperties
        {
            Server = new Uri(Stack.TrinoEndpoint),
            Catalog = "iceberg",
            Schema = schemaName,
        };
        return new ClientSession(sessionProperties: sessionProperties, auth: null);
    }

    [Fact]
    public async Task TrinoClient_CanCreateSchema()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("client_test");
        var session = CreateSession(schemaName);

        // Act
        var results = await ExecuteQueryAsync(session, $"CREATE SCHEMA IF NOT EXISTS {schemaName}");

        // Assert
        _output.WriteLine($"Create schema results: {results.Count} rows");
        Assert.NotNull(results);
    }

    [Fact]
    public async Task TrinoClient_CanCreateTableAndInsertData()
    {
        // Use pre-created schema and table from class fixture
        var session = CreateSession(SchemaName);

        // Query the pre-existing data
        var results = await ExecuteQueryAsync(session, "SELECT * FROM test_data ORDER BY id");

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
        // Use pre-created schema and table from class fixture
        var session = CreateSession(SchemaName);

        // Query with filter (id > 100 returns id=200)
        var results = await ExecuteQueryAsync(session, "SELECT id, value FROM test_data WHERE id > 100 ORDER BY id");

        // Assert
        _output.WriteLine($"Query returned {results.Count} rows");
        Assert.Equal(1, results.Count);
        Assert.Equal(200, Convert.ToInt32(results[0][0]));
        Assert.Equal("data", results[0][1]);
    }

    [Fact]
    public async Task TrinoClient_CanExecuteAggregateQuery()
    {
        // Use the shared sales table from assembly fixture
        var session = CreateSession(SchemaName);

        // Act - Query the pre-populated sales table
        var results = await ExecuteQueryAsync(
            session,
            "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY category"
        );

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
        // Use pre-created schema and table from class fixture
        var session = CreateSession(SchemaName);

        // Act
        var results = await ExecuteQueryAsync(session, "SELECT COUNT(*) as total FROM test_data");

        // Assert
        _output.WriteLine($"Count query returned {results.Count} rows");
        Assert.Single(results);
        Assert.Single(results[0]);
        Assert.Equal(2, Convert.ToInt64(results[0][0]));
    }

    [Fact]
    public async Task TrinoClient_HandlesEmptyResultSet()
    {
        // Use pre-created schema and table, but query with impossible WHERE clause
        var session = CreateSession(SchemaName);

        // Act - Query with condition that returns no rows
        var results = await ExecuteQueryAsync(session, "SELECT * FROM test_data WHERE id > 1000");

        // Assert
        _output.WriteLine($"Empty query returned {results.Count} rows");
        Assert.Empty(results);
    }

    [Fact]
    public async Task TrinoClient_CanExecuteMultipleQueriesSequentially()
    {
        // Arrange
        var session = CreateSession(SchemaName);

        // Act - Execute multiple queries in sequence using pre-created test_data
        var countResults = await ExecuteQueryAsync(session, "SELECT COUNT(*) FROM test_data");
        var sumResults = await ExecuteQueryAsync(session, "SELECT SUM(id) FROM test_data");
        var avgResults = await ExecuteQueryAsync(session, "SELECT AVG(CAST(id AS DOUBLE)) FROM test_data");

        // Assert
        _output.WriteLine($"Sequential queries completed successfully");
        Assert.Single(countResults);
        Assert.True(Convert.ToInt32(countResults[0][0]) >= 2, "Expected at least 2 rows");
        Assert.Single(sumResults);
        Assert.True(Convert.ToInt64(sumResults[0][0]) >= 300, "Expected sum >= 300 (100+200)");
    }

    [Fact]
    public async Task TrinoClient_ThrowsExceptionForInvalidSQL()
    {
        // Arrange
        var session = CreateSession(SchemaName);

        // Act & Assert - TrinoAggregateException wraps TrinoException for query errors
        await Assert.ThrowsAsync<TrinoAggregateException>(async () =>
            await ExecuteQueryAsync(session, "SELECT * FROM nonexistent.invalid.table")
        );
    }

    [Fact]
    public async Task TrinoClient_CanHandleComplexDataTypes()
    {
        // Use the shared mixed_types table from assembly fixture
        var session = CreateSession(SchemaName);

        // Act - Query the pre-populated mixed_types table
        var results = await ExecuteQueryAsync(session, "SELECT * FROM mixed_types ORDER BY id");

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
