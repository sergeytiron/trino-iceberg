using System.Data;
using Trino.Data.ADO.Server;

namespace IntegrationTests;

/// <summary>
/// Class fixture for TrinoAdoNetIntegrationTests - uses the shared common schema from assembly fixture.
/// No additional setup queries needed - reuses pre-populated test data.
/// </summary>
public sealed class TrinoAdoNetIntegrationTestsClassFixture : IAsyncLifetime
{
    private readonly TrinoIcebergStackFixture _stackFixture;
    
    /// <summary>
    /// Uses the shared schema from assembly fixture - no separate schema creation needed.
    /// </summary>
    public string SchemaName => _stackFixture.CommonSchemaName;
    public TrinoIcebergStack Stack => _stackFixture.Stack;

    public TrinoAdoNetIntegrationTestsClassFixture(TrinoIcebergStackFixture stackFixture)
    {
        _stackFixture = stackFixture;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Integration tests for Trino ADO.NET provider against a real Trino stack
/// </summary>
public class TrinoAdoNetIntegrationTests : IClassFixture<TrinoAdoNetIntegrationTestsClassFixture>
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoAdoNetIntegrationTestsClassFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.SchemaName;

    public TrinoAdoNetIntegrationTests(ITestOutputHelper output, TrinoAdoNetIntegrationTestsClassFixture classFixture)
    {
        _output = output;
        _classFixture = classFixture;
    }

    /// <summary>
    /// Generates a unique schema name for tests that need isolated schemas
    /// </summary>
    private static string GetUniqueSchemaName(string baseName) => $"{baseName}_{Guid.NewGuid():N}".ToLowerInvariant();

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
    public void AdoNet_CanOpenConnection()
    {
        // Arrange
        using var connection = CreateConnection(SchemaName);

        // Act
        connection.Open();

        // Assert
        Assert.Equal(ConnectionState.Open, connection.State);
        _output.WriteLine($"Connection state: {connection.State}");
    }

    [Fact]
    public void AdoNet_CanCreateSchema()
    {
        // Arrange
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act - verify schema exists by querying test_table
        using var command = new TrinoCommand(connection, "SELECT COUNT(*) FROM test_table");
        var result = command.ExecuteScalar();

        // Assert - query should complete without error
        Assert.NotNull(result);
        _output.WriteLine($"Schema '{SchemaName}' verified with test_table query");
    }

    [Fact]
    public void AdoNet_CanExecuteScalar()
    {
        // Arrange
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act
        using var command = new TrinoCommand(connection, "SELECT COUNT(*) FROM test_table");
        var result = command.ExecuteScalar();

        // Assert
        Assert.NotNull(result);
        var count = Convert.ToInt64(result);
        Assert.True(count >= 1, "Expected at least 1 row in test_table");
        _output.WriteLine($"ExecuteScalar returned: {result}");
    }

    [Fact]
    public void AdoNet_CanExecuteReader()
    {
        // Arrange
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act
        using var command = new TrinoCommand(connection, "SELECT id, value FROM test_table ORDER BY id");
        using var reader = command.ExecuteReader();

        // Assert
        var rowCount = 0;
        while (reader.Read())
        {
            rowCount++;
            var id = reader.GetInt32(0);
            var value = reader.GetString(1);

            _output.WriteLine($"Row {rowCount}: ID={id}, Value={value}");

            Assert.True(id > 0, "ID should be positive");
            Assert.NotEmpty(value);
        }

        Assert.True(rowCount >= 1, "Expected at least 1 row in test_table");
    }

    [Fact]
    public void AdoNet_ReaderHasCorrectSchema()
    {
        // Arrange
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act
        using var command = new TrinoCommand(connection, "SELECT * FROM test_table");
        using var reader = command.ExecuteReader();

        // Assert - test_table has 3 columns: id, name, value
        Assert.Equal(3, reader.FieldCount);

        // Check column names
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("name", reader.GetName(1));
        Assert.Equal("value", reader.GetName(2));

        _output.WriteLine(
            $"Schema: {reader.GetName(0)} ({reader.GetDataTypeName(0)}), "
                + $"{reader.GetName(1)} ({reader.GetDataTypeName(1)}), "
                + $"{reader.GetName(2)} ({reader.GetDataTypeName(2)})"
        );
    }

    [Fact]
    public void AdoNet_CanHandleNullValues()
    {
        // Arrange - use the shared null_test table from assembly fixture
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act - query the pre-populated null_test table
        using var command = new TrinoCommand(connection, "SELECT id, nullable_value FROM null_test ORDER BY id");
        using var reader = command.ExecuteReader();

        // Assert
        Assert.True(reader.Read());
        Assert.False(reader.IsDBNull(0)); // id should not be null
        Assert.True(reader.IsDBNull(1)); // nullable_value should be null
        _output.WriteLine($"Row 1: ID={reader.GetInt32(0)}, Value=NULL");

        Assert.True(reader.Read());
        Assert.False(reader.IsDBNull(0));
        Assert.False(reader.IsDBNull(1));
        _output.WriteLine($"Row 2: ID={reader.GetInt32(0)}, Value={reader.GetString(1)}");
    }

    [Fact]
    public void AdoNet_CanExecuteAggregateQuery()
    {
        // Arrange - use the shared sales table from assembly fixture
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act - query the pre-populated sales table
        using var command = new TrinoCommand(
            connection,
            "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY category"
        );
        using var reader = command.ExecuteReader();

        // Assert
        var results = new List<(string Category, long Total)>();
        while (reader.Read())
        {
            var category = reader.GetString(0);
            var total = reader.GetInt64(1);
            results.Add((category, total));
            _output.WriteLine($"Category: {category}, Total: {total}");
        }

        Assert.Equal(2, results.Count);
        Assert.Equal("A", results[0].Category);
        Assert.Equal(250, results[0].Total); // 100 + 150
        Assert.Equal("B", results[1].Category);
        Assert.Equal(500, results[1].Total); // 200 + 300
    }

    [Fact]
    public void AdoNet_ConnectionPropertiesWork()
    {
        // Arrange
        var properties = new TrinoConnectionProperties
        {
            Server = new Uri(Stack.TrinoEndpoint),
            Catalog = "iceberg",
            Schema = SchemaName,
        };
        using var connection = new TrinoConnection(properties);

        // Act
        connection.Open();

        // Assert
        Assert.Equal(ConnectionState.Open, connection.State);
        _output.WriteLine($"Connected using TrinoConnectionProperties");
    }

    [Fact]
    public void AdoNet_CanUseCommandWithCommandText()
    {
        // Arrange
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as test_value";
        var result = command.ExecuteScalar();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(1, Convert.ToInt32(result));
        _output.WriteLine($"Command with CommandText returned: {result}");
    }

    [Fact]
    public void AdoNet_MultipleCommandsOnSameConnection()
    {
        // Arrange
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Act & Assert - Execute multiple commands
        using (var cmd1 = new TrinoCommand(connection, "CREATE TABLE multi_cmd_test (value int)"))
        {
            cmd1.ExecuteNonQuery();
            _output.WriteLine("Command 1: Table created");
        }

        using (var cmd2 = new TrinoCommand(connection, "INSERT INTO multi_cmd_test VALUES (42)"))
        {
            cmd2.ExecuteNonQuery();
            _output.WriteLine("Command 2: Data inserted");
        }

        using (var cmd3 = new TrinoCommand(connection, "SELECT COUNT(*) FROM multi_cmd_test"))
        {
            var count = cmd3.ExecuteScalar();
            Assert.Equal(1, Convert.ToInt64(count));
            _output.WriteLine($"Command 3: Count query returned {count}");
        }
    }
}
