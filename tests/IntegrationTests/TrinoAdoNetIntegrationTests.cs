using System.Data;
using Trino.Data.ADO.Server;

namespace IntegrationTests;

/// <summary>
/// Integration tests for Trino ADO.NET provider against a real Trino stack
/// </summary>
public class TrinoAdoNetIntegrationTests
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.CommonSchemaName;

    public TrinoAdoNetIntegrationTests(ITestOutputHelper output, TrinoIcebergStackFixture classFixture)
    {
        _output = output;
        _classFixture = classFixture;
    }

    /// <summary>
    /// Create a Trino connection with a specific schema
    /// </summary>
    private TrinoConnection CreateConnection(string schemaName)
    {
        var properties = new TrinoConnectionProperties
        {
            Server = new Uri(Stack.TrinoEndpoint),
            Catalog = "iceberg",
            Schema = schemaName,
        };
        return new TrinoConnection(properties);
    }

    [Fact]
    public void AdoNet_ConnectionInitialization()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        // Simple scalar - using shared_data rows with id >= 100
        using var cmdScalar = new TrinoCommand(connection, "SELECT COUNT(*) FROM shared_data WHERE id >= 100");
        var count = Convert.ToInt64(cmdScalar.ExecuteScalar());
        Assert.Equal(2, count);
    }

    [Fact]
    public void AdoNet_DataRetrievalAndNulls()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Reader over shared_data (test_data pattern: id >= 100)
        using (var cmd = new TrinoCommand(connection, "SELECT id, value FROM shared_data WHERE id >= 100 ORDER BY id"))
        using (var reader = cmd.ExecuteReader())
        {
            var rows = new List<(int Id, string Value)>();
            while (reader.Read())
            {
                rows.Add((reader.GetInt32(0), reader.GetString(1)));
            }
            Assert.Equal(2, rows.Count);
        }

        // Contacts pattern for null value handling (shared_data rows 1-3)
        using (var cmdNulls = new TrinoCommand(connection, "SELECT id, email, phone FROM shared_data WHERE id <= 3 ORDER BY id"))
        using (var readerNulls = cmdNulls.ExecuteReader())
        {
            int rowIndex = 0;
            while (readerNulls.Read())
            {
                var emailIsNull = readerNulls.IsDBNull(1);
                var phoneIsNull = readerNulls.IsDBNull(2);
                if (rowIndex == 1) // Bob row has NULL email
                {
                    Assert.True(emailIsNull);
                }
                if (rowIndex == 2) // Charlie row has NULL phone
                {
                    Assert.True(phoneIsNull);
                }
                rowIndex++;
            }
            Assert.Equal(3, rowIndex);
        }

        // Schema metadata check (employee_data)
        using (var cmdSchema = new TrinoCommand(connection, "SELECT employee_id, first_name, last_name, hire_date FROM employee_data"))
        using (var readerSchema = cmdSchema.ExecuteReader())
        {
            Assert.Equal(4, readerSchema.FieldCount);
            Assert.Equal("employee_id", readerSchema.GetName(0));
            Assert.Equal("first_name", readerSchema.GetName(1));
            Assert.Equal("last_name", readerSchema.GetName(2));
        }
    }

    [Fact]
    public void AdoNet_AggregateQuery()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();
        using var command = new TrinoCommand(connection, "SELECT category, SUM(amount) FROM category_data GROUP BY category ORDER BY category");
        using var reader = command.ExecuteReader();
        var results = new List<(string Cat, long Total)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetInt64(1)));
        }
        Assert.Equal(2, results.Count);
        Assert.Equal("A", results[0].Cat);
        Assert.Equal(250, results[0].Total);
        Assert.Equal("B", results[1].Cat);
        Assert.Equal(500, results[1].Total);
    }
}
