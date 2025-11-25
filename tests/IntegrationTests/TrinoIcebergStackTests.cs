namespace IntegrationTests;

/// <summary>
/// Class fixture for TrinoIcebergStackTests - uses the shared common schema from assembly fixture.
/// No additional setup queries needed - reuses pre-populated test data.
/// </summary>
public sealed class TrinoIcebergStackTestsClassFixture : IAsyncLifetime
{
    private readonly TrinoIcebergStackFixture _stackFixture;
    
    /// <summary>
    /// Uses the shared schema from assembly fixture - no separate schema creation needed.
    /// </summary>
    public string SchemaName => _stackFixture.CommonSchemaName;
    public TrinoIcebergStack Stack => _stackFixture.Stack;

    public TrinoIcebergStackTestsClassFixture(TrinoIcebergStackFixture stackFixture)
    {
        _stackFixture = stackFixture;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Integration tests for the Trino + Nessie + MinIO stack
/// </summary>
public class TrinoIcebergStackTests : IClassFixture<TrinoIcebergStackTestsClassFixture>
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackTestsClassFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.SchemaName;

    public TrinoIcebergStackTests(ITestOutputHelper output, TrinoIcebergStackTestsClassFixture classFixture)
    {
        _output = output;
        _classFixture = classFixture;
    }

    [Fact]
    public async Task CanCreateSchemaInNessieCatalog()
    {
        // Schema already created in class fixture - just verify it exists
        var result = await Stack.ExecuteTrinoQueryAsync(
            $"SHOW SCHEMAS IN iceberg LIKE '{SchemaName}'",
            cancellationToken: TestContext.Current.CancellationToken
        );

        // Assert
        _output.WriteLine($"Result: {result}");
        Assert.Contains(SchemaName, result);
    }

    [Fact]
    public async Task CanCreateAndQueryIcebergTable()
    {
        // Use shared numbers table from assembly fixture
        var result = await Stack.ExecuteTrinoQueryAsync(
            $"SELECT * FROM iceberg.{SchemaName}.numbers ORDER BY id",
            cancellationToken: TestContext.Current.CancellationToken
        );

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
        // Use shared numbers table from assembly fixture
        var countResult = await Stack.ExecuteTrinoQueryAsync(
            $"SELECT COUNT(*) as total FROM iceberg.{SchemaName}.numbers",
            cancellationToken: TestContext.Current.CancellationToken
        );

        var groupResult = await Stack.ExecuteTrinoQueryAsync(
            $"SELECT name, id FROM iceberg.{SchemaName}.numbers ORDER BY name",
            cancellationToken: TestContext.Current.CancellationToken
        );

        // Assert
        _output.WriteLine($"Count result: {countResult}");
        _output.WriteLine($"Group result: {groupResult}");

        Assert.Contains("\"3\"", countResult);
        Assert.Contains("\"one\"", groupResult);
        Assert.Contains("\"two\"", groupResult);
        Assert.Contains("\"three\"", groupResult);
    }

    [Fact]
    public async Task TrinoHealthCheckPasses()
    {
        // Arrange
        using var client = new HttpClient();

        // Act
        var response = await client.GetAsync($"{Stack.TrinoEndpoint}/v1/info", TestContext.Current.CancellationToken);

        // Assert
        response.EnsureSuccessStatusCode();
        var content = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        _output.WriteLine($"Trino info: {content}");
        Assert.Contains("\"starting\":false", content.ToLower());
    }

    [Fact]
    public async Task NessieHealthCheckPasses()
    {
        // Arrange
        using var client = new HttpClient();

        // Act
        var response = await client.GetAsync(
            $"{Stack.NessieEndpoint}/api/v2/config",
            TestContext.Current.CancellationToken
        );

        // Assert
        response.EnsureSuccessStatusCode();
        var content = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        _output.WriteLine($"Nessie config: {content}");
        Assert.NotEmpty(content);
    }
}
