using Trino.Client;

namespace IntegrationTests;

/// <summary>
/// Integration tests for official Trino.Client against a real Trino stack
/// </summary>
public class TrinoClientIntegrationTests
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.CommonSchemaName;

    public TrinoClientIntegrationTests(ITestOutputHelper output, TrinoIcebergStackFixture classFixture)
    {
        _output = output;
        _classFixture = classFixture;
    }

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
    public async Task TrinoClient_BasicQueries_Work()
    {
        var session = CreateSession(SchemaName);

        // Count - using shared_data rows with id >= 100 (test_data pattern)
        var countRows = await ExecuteQueryAsync(session, "SELECT COUNT(*) FROM shared_data WHERE id >= 100");
        Assert.Single(countRows);
        Assert.Equal(2, Convert.ToInt64(countRows[0][0]));

        // Filter (id > 100) -> only 200
        var filtered = await ExecuteQueryAsync(session, "SELECT id, value FROM shared_data WHERE id > 100 ORDER BY id");
        Assert.Single(filtered);
        Assert.Equal(200, Convert.ToInt32(filtered[0][0]));
        Assert.Equal("data", filtered[0][1]);

        // Empty result
        var empty = await ExecuteQueryAsync(session, "SELECT * FROM shared_data WHERE id > 1000");
        Assert.Empty(empty);

        // Sequential aggregate style queries (sum + avg over ids >= 100)
        var sumRows = await ExecuteQueryAsync(session, "SELECT SUM(id) FROM shared_data WHERE id >= 100");
        var avgRows = await ExecuteQueryAsync(
            session,
            "SELECT AVG(CAST(id AS DOUBLE)) FROM shared_data WHERE id >= 100"
        );
        Assert.Single(sumRows);
        Assert.Single(avgRows);
        Assert.Equal(300, Convert.ToInt64(sumRows[0][0]));
        Assert.True(Convert.ToDouble(avgRows[0][0]) >= 150.0);
    }

    [Fact]
    public async Task TrinoClient_Aggregates_And_Types()
    {
        var session = CreateSession(SchemaName);

        // Sales aggregate - using category_data
        var salesAgg = await ExecuteQueryAsync(
            session,
            "SELECT category, SUM(amount) AS total FROM category_data GROUP BY category ORDER BY category"
        );
        Assert.Equal(2, salesAgg.Count);
        Assert.Equal("A", salesAgg[0][0]);
        Assert.Equal(250L, Convert.ToInt64(salesAgg[0][1]));
        Assert.Equal("B", salesAgg[1][0]);
        Assert.Equal(500L, Convert.ToInt64(salesAgg[1][1]));

        // People simple types - using shared_data rows 1-3
        var people = await ExecuteQueryAsync(
            session,
            "SELECT id, name, age, active FROM shared_data WHERE id <= 3 ORDER BY id"
        );
        Assert.Equal(3, people.Count);
        Assert.Equal("Alice", people[0][1]);
        Assert.Equal(30, Convert.ToInt32(people[0][2]));
        Assert.True((bool)people[0][3]!);

        // Measurements numeric extremes + decimal - using shared_data row with id=100
        var measurements = await ExecuteQueryAsync(
            session,
            "SELECT id, value_int, value_double, value_decimal FROM shared_data WHERE id = 100 ORDER BY id"
        );
        Assert.Single(measurements);
        Assert.Equal(9223372036854775807L, Convert.ToInt64(measurements[0][1]));
        Assert.True(Math.Abs(Convert.ToDouble(measurements[0][2]) - 3.14159) < 1e-5);

        // Messages string escaping - using shared_data row 2
        var messages = await ExecuteQueryAsync(
            session,
            "SELECT id, content FROM shared_data WHERE content = 'It''s a test'"
        );
        Assert.Single(messages);
        Assert.Equal("It's a test", messages[0][1]);
    }

    [Fact]
    public async Task TrinoClient_InvalidSql_Throws()
    {
        var session = CreateSession(SchemaName);
        await Assert.ThrowsAsync<TrinoAggregateException>(async () =>
            await ExecuteQueryAsync(session, "SELECT * FROM nonexistent.invalid.table")
        );
    }
}
