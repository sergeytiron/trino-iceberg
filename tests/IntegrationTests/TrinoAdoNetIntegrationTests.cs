using System.Data;
using Trino.Data.ADO.Server;

namespace IntegrationTests;

/// <summary>
/// Integration tests for Trino ADO.NET provider against a real Trino stack
/// </summary>
public class TrinoAdoNetIntegrationTests
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _fixture;
    private TrinoIcebergStack Stack => _fixture.Stack;

    public TrinoAdoNetIntegrationTests(ITestOutputHelper output, TrinoIcebergStackFixture fixture)
    {
        _output = output;
        _fixture = fixture;
    }

    /// <summary>
    /// Generates a unique schema name for test isolation
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
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);

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
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
        connection.Open();

        // Act
        using var command = new TrinoCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {schemaName}");
        command.ExecuteNonQuery();

        // Assert - query should complete without error
        _output.WriteLine($"Schema '{schemaName}' created successfully");
    }

    [Fact]
    public void AdoNet_CanExecuteScalar()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
        connection.Open();

        // Setup
        using (var setupCmd = new TrinoCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {schemaName}"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (var setupCmd = new TrinoCommand(connection, "CREATE TABLE test_table (id int, value varchar)"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (var setupCmd = new TrinoCommand(connection, "INSERT INTO test_table VALUES (1, 'test')"))
        {
            setupCmd.ExecuteNonQuery();
        }

        // Act
        using var command = new TrinoCommand(connection, "SELECT COUNT(*) FROM test_table");
        var result = command.ExecuteScalar();

        // Assert
        Assert.NotNull(result);
        var count = Convert.ToInt64(result);
        Assert.Equal(1, count);
        _output.WriteLine($"ExecuteScalar returned: {result}");
    }

    [Fact]
    public void AdoNet_CanExecuteReader()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
        connection.Open();

        // Setup
        using (var setupCmd = new TrinoCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {schemaName}"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (var setupCmd = new TrinoCommand(connection, "CREATE TABLE users (id int, name varchar, age int)"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (
            var setupCmd = new TrinoCommand(
                connection,
                "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
            )
        )
        {
            setupCmd.ExecuteNonQuery();
        }

        // Act
        using var command = new TrinoCommand(connection, "SELECT id, name, age FROM users ORDER BY id");
        using var reader = command.ExecuteReader();

        // Assert
        var rowCount = 0;
        while (reader.Read())
        {
            rowCount++;
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var age = reader.GetInt32(2);

            _output.WriteLine($"Row {rowCount}: ID={id}, Name={name}, Age={age}");

            Assert.InRange(id, 1, 3);
            Assert.NotEmpty(name);
            Assert.InRange(age, 20, 40);
        }

        Assert.Equal(3, rowCount);
    }

    [Fact]
    public void AdoNet_ReaderHasCorrectSchema()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
        connection.Open();

        // Setup
        using (var setupCmd = new TrinoCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {schemaName}"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (
            var setupCmd = new TrinoCommand(
                connection,
                "CREATE TABLE schema_test (id int, name varchar, active boolean)"
            )
        )
        {
            setupCmd.ExecuteNonQuery();
        }

        using (var setupCmd = new TrinoCommand(connection, "INSERT INTO schema_test VALUES (1, 'test', true)"))
        {
            setupCmd.ExecuteNonQuery();
        }

        // Act
        using var command = new TrinoCommand(connection, "SELECT * FROM schema_test");
        using var reader = command.ExecuteReader();

        // Assert
        Assert.Equal(3, reader.FieldCount);

        // Check column names
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("name", reader.GetName(1));
        Assert.Equal("active", reader.GetName(2));

        _output.WriteLine(
            $"Schema: {reader.GetName(0)} ({reader.GetDataTypeName(0)}), "
                + $"{reader.GetName(1)} ({reader.GetDataTypeName(1)}), "
                + $"{reader.GetName(2)} ({reader.GetDataTypeName(2)})"
        );
    }

    [Fact]
    public void AdoNet_CanHandleNullValues()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
        connection.Open();

        // Setup
        using (var setupCmd = new TrinoCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {schemaName}"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (var setupCmd = new TrinoCommand(connection, "CREATE TABLE null_test (id int, nullable_value varchar)"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (var setupCmd = new TrinoCommand(connection, "INSERT INTO null_test VALUES (1, null), (2, 'not null')"))
        {
            setupCmd.ExecuteNonQuery();
        }

        // Act
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
        // Arrange
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
        connection.Open();

        // Setup
        using (var setupCmd = new TrinoCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {schemaName}"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (var setupCmd = new TrinoCommand(connection, "CREATE TABLE sales (amount bigint, category varchar)"))
        {
            setupCmd.ExecuteNonQuery();
        }

        using (
            var setupCmd = new TrinoCommand(
                connection,
                "INSERT INTO sales VALUES (100, 'A'), (200, 'B'), (150, 'A'), (300, 'B')"
            )
        )
        {
            setupCmd.ExecuteNonQuery();
        }

        // Act
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
        var schemaName = GetUniqueSchemaName("ado_test");
        var properties = new TrinoConnectionProperties
        {
            Server = new Uri(Stack.TrinoEndpoint),
            Catalog = "iceberg",
            Schema = schemaName,
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
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
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
        var schemaName = GetUniqueSchemaName("ado_test");
        using var connection = CreateConnection(schemaName);
        connection.Open();

        // Act & Assert - Execute multiple commands
        using (var cmd1 = new TrinoCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {schemaName}"))
        {
            cmd1.ExecuteNonQuery();
            _output.WriteLine("Command 1: Schema created");
        }

        using (var cmd2 = new TrinoCommand(connection, "CREATE TABLE multi_cmd_test (value int)"))
        {
            cmd2.ExecuteNonQuery();
            _output.WriteLine("Command 2: Table created");
        }

        using (var cmd3 = new TrinoCommand(connection, "INSERT INTO multi_cmd_test VALUES (42)"))
        {
            cmd3.ExecuteNonQuery();
            _output.WriteLine("Command 3: Data inserted");
        }

        using (var cmd4 = new TrinoCommand(connection, "SELECT COUNT(*) FROM multi_cmd_test"))
        {
            var count = cmd4.ExecuteScalar();
            Assert.Equal(1, Convert.ToInt64(count));
            _output.WriteLine($"Command 4: Count query returned {count}");
        }
    }
}
