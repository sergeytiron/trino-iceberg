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
    public void CanCreateSchemaInNessieCatalog()
    {
        // Schema already created in assembly fixture - verify it exists using ADO.NET
        var results = Stack.ExecuteQueryFast($"SHOW SCHEMAS IN iceberg LIKE '{SchemaName}'");

        // Assert
        _output.WriteLine($"Result rows: {results.Count}");
        foreach (var row in results)
        {
            _output.WriteLine($"  Schema: {string.Join(", ", row)}");
        }
        Assert.Contains(results, row => row.Any(col => col.Contains(SchemaName)));
    }

    [Fact]
    public void CanCreateAndQueryIcebergTable()
    {
        // Use shared numbers table from assembly fixture
        var results = Stack.ExecuteQueryFast($"SELECT * FROM iceberg.{SchemaName}.numbers ORDER BY id");

        // Assert
        _output.WriteLine($"Query returned {results.Count} rows:");
        foreach (var row in results)
        {
            _output.WriteLine($"  {string.Join(", ", row)}");
        }
        
        Assert.Equal(3, results.Count);
        Assert.Contains(results, row => row[0] == "1" && row[1] == "one");
        Assert.Contains(results, row => row[0] == "2" && row[1] == "two");
        Assert.Contains(results, row => row[0] == "3" && row[1] == "three");
    }

    [Fact]
    public void CanExecuteMultipleQueries()
    {
        // Use shared numbers table from assembly fixture
        var count = Stack.ExecuteScalarFast($"SELECT COUNT(*) as total FROM iceberg.{SchemaName}.numbers");
        var groupResults = Stack.ExecuteQueryFast($"SELECT name, id FROM iceberg.{SchemaName}.numbers ORDER BY name");

        // Assert
        _output.WriteLine($"Count result: {count}");
        _output.WriteLine($"Group results ({groupResults.Count} rows):");
        foreach (var row in groupResults)
        {
            _output.WriteLine($"  {string.Join(", ", row)}");
        }

        Assert.Equal(3L, count);
        Assert.Equal(3, groupResults.Count);
        Assert.Contains(groupResults, row => row[0] == "one");
        Assert.Contains(groupResults, row => row[0] == "two");
        Assert.Contains(groupResults, row => row[0] == "three");
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
