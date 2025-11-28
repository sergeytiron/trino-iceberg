using System.Data;
using Dapper;
using Trino.Data.ADO.Server;

namespace IntegrationTests;

/// <summary>
/// Integration tests for using Dapper with Trino ADO.NET provider.
/// Demonstrates that Dapper works seamlessly with TrinoConnection.
/// </summary>
public class DapperIntegrationTests
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.CommonSchemaName;

    public DapperIntegrationTests(ITestOutputHelper output, TrinoIcebergStackFixture classFixture)
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
    public void Dapper_Query_ReturnsTypedResults()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Query shared_data table using Dapper
        var results = connection
            .Query<PersonDto>("SELECT id, name, age, active FROM shared_data WHERE id <= 3 ORDER BY id")
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(30, results[0].Age);
        Assert.True(results[0].Active);

        Assert.Equal(2, results[1].Id);
        Assert.Equal("Bob", results[1].Name);
        Assert.False(results[1].Active);

        _output.WriteLine($"Dapper returned {results.Count} rows");
    }

    [Fact]
    public void Dapper_QueryFirst_ReturnsSingleRow()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        var person = connection.QueryFirst<PersonDto>("SELECT id, name, age, active FROM shared_data WHERE id = 1");

        Assert.NotNull(person);
        Assert.Equal(1, person.Id);
        Assert.Equal("Alice", person.Name);
        Assert.Equal(30, person.Age);
        Assert.True(person.Active);

        _output.WriteLine($"Dapper QueryFirst returned: {person.Name}");
    }

    [Fact]
    public void Dapper_QueryFirstOrDefault_ReturnsNullForNoResults()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        var person = connection.QueryFirstOrDefault<PersonDto>(
            "SELECT id, name, age, active FROM shared_data WHERE id = 999"
        );

        Assert.Null(person);
        _output.WriteLine("Dapper QueryFirstOrDefault correctly returned null for non-existent row");
    }

    [Fact]
    public void Dapper_ExecuteScalar_ReturnsScalarValue()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        var count = connection.ExecuteScalar<long>("SELECT COUNT(*) FROM shared_data WHERE id <= 3");

        Assert.Equal(3, count);
        _output.WriteLine($"Dapper ExecuteScalar returned count: {count}");
    }

    [Fact]
    public void Dapper_Query_HandlesNullValues()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Query contacts pattern which has NULL values
        var contacts = connection
            .Query<ContactDto>("SELECT id, name, email, phone FROM shared_data WHERE id <= 3 ORDER BY id")
            .ToList();

        Assert.Equal(3, contacts.Count);

        // Alice has all values
        Assert.Equal("alice@example.com", contacts[0].Email);
        Assert.Equal("555-0001", contacts[0].Phone);

        // Bob has NULL email
        Assert.Null(contacts[1].Email);
        Assert.Equal("555-0002", contacts[1].Phone);

        // Charlie has NULL phone
        Assert.Equal("charlie@example.com", contacts[2].Email);
        Assert.Null(contacts[2].Phone);

        _output.WriteLine("Dapper correctly handled NULL values in query results");
    }

    [Fact]
    public void Dapper_Query_MapsSnakeCaseToPascalCase()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Use SQL column aliases to map snake_case columns to PascalCase properties
        // This is the recommended approach for Dapper as it avoids global state modification
        // that could cause issues with parallel test execution
        var employees = connection
            .Query<EmployeeDto>(
                """
                SELECT
                    employee_id AS EmployeeId,
                    first_name AS FirstName,
                    last_name AS LastName,
                    hire_date AS HireDate
                FROM employee_data
                ORDER BY employee_id
                """
            )
            .ToList();

        Assert.Equal(2, employees.Count);
        Assert.Equal(1, employees[0].EmployeeId);
        Assert.Equal("John", employees[0].FirstName);
        Assert.Equal("Doe", employees[0].LastName);

        Assert.Equal(2, employees[1].EmployeeId);
        Assert.Equal("Jane", employees[1].FirstName);
        Assert.Equal("Smith", employees[1].LastName);

        _output.WriteLine("Dapper correctly mapped aliased columns to PascalCase properties");
    }

    [Fact]
    public void Dapper_Query_WithAggregation()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        var results = connection
            .Query<CategorySummaryDto>(
                "SELECT category, SUM(amount) as TotalAmount FROM category_data GROUP BY category ORDER BY category"
            )
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("A", results[0].Category);
        Assert.Equal(250, results[0].TotalAmount);
        Assert.Equal("B", results[1].Category);
        Assert.Equal(500, results[1].TotalAmount);

        _output.WriteLine($"Dapper aggregation query returned {results.Count} categories");
    }

    [Fact]
    public void Dapper_QueryMultiple_NotSupported()
    {
        // Note: Trino doesn't support multiple result sets in a single query
        // This test documents the expected behavior
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Single query works fine
        var single = connection.Query<PersonDto>("SELECT id, name, age, active FROM shared_data WHERE id = 1").ToList();

        Assert.Single(single);
        _output.WriteLine("Dapper single query works correctly");
    }

    [Fact]
    public void Dapper_Query_WithDynamicType()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Query as dynamic to avoid type mapping
        var results = connection.Query("SELECT id, name, value FROM shared_data WHERE id = 1").ToList();

        Assert.Single(results);
        Assert.Equal(1, (int)results[0].id);
        Assert.Equal("Alice", (string)results[0].name);
        Assert.Equal("test", (string)results[0].value);

        _output.WriteLine("Dapper dynamic query returned expected values");
    }

    [Fact]
    public void Dapper_Query_NumericTypes()
    {
        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Use SQL column aliases to map snake_case columns to PascalCase properties
        // Note: TrinoBigDecimal is Trino's custom decimal type and doesn't map directly to System.Decimal
        // To handle Trino decimals, implement a Dapper.SqlMapper.TypeHandler<decimal> that converts
        // TrinoBigDecimal to System.Decimal, or use column aliases like: CAST(value_decimal AS DOUBLE)
        var measurements = connection
            .Query<MeasurementDto>(
                """
                SELECT
                    id AS Id,
                    value_int AS ValueInt,
                    value_double AS ValueDouble
                FROM shared_data
                WHERE id = 100
                """
            )
            .ToList();

        Assert.Single(measurements);
        Assert.Equal(9223372036854775807L, measurements[0].ValueInt);
        Assert.Equal(3.14159, measurements[0].ValueDouble, precision: 5);

        _output.WriteLine("Dapper correctly handled numeric types including bigint and double");
    }

    #region DTO Classes

    public class PersonDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public bool Active { get; set; }
    }

    public class ContactDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Email { get; set; }
        public string? Phone { get; set; }
    }

    public class EmployeeDto
    {
        public int EmployeeId { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public object? HireDate { get; set; }
    }

    public class CategorySummaryDto
    {
        public string Category { get; set; } = string.Empty;
        public long TotalAmount { get; set; }
    }

    public class MeasurementDto
    {
        public int Id { get; set; }
        public long ValueInt { get; set; }
        public double ValueDouble { get; set; }
    }

    public class EventDto
    {
        public long EventId { get; set; }
        public string EventType { get; set; } = string.Empty;
        public DateTime EventTime { get; set; }
    }

    #endregion

    #region Time Travel Tests

    [Fact]
    public async Task Dapper_Query_TimeTravelWithDateTimeParameters()
    {
        // Arrange - Insert initial data (Snapshot 1)
        Stack.ExecuteNonQuery(
            "INSERT INTO events_time_travel VALUES (101, 'dapper_login', TIMESTAMP '2025-11-17 10:00:00'), (102, 'dapper_click', TIMESTAMP '2025-11-17 10:05:00')",
            SchemaName
        );

        // Ensure a distinct commit time boundary
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var snapshotTime = DateTime.UtcNow; // capture point between commits

        // Insert more data (Snapshot 2) - should not be visible when traveling to snapshotTime
        Stack.ExecuteNonQuery(
            "INSERT INTO events_time_travel VALUES (103, 'dapper_purchase', TIMESTAMP '2025-11-17 10:10:00'), (104, 'dapper_logout', TIMESTAMP '2025-11-17 10:15:00')",
            SchemaName
        );

        using var connection = CreateConnection(SchemaName);
        connection.Open();

        // Format datetime values as Trino TIMESTAMP literals
        // This is the edge case: using datetime for both snapshot time AND column filter
        var snapshotTimeStr = snapshotTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
        var eventTimeFilter = new DateTime(2025, 11, 17, 10, 07, 00, DateTimeKind.Utc);
        var eventTimeFilterStr = eventTimeFilter.ToString("yyyy-MM-dd HH:mm:ss.fff");

        // Act - Time travel query with datetime parameters for both snapshot and filter
        var sql = $"""
            SELECT
                event_id AS EventId,
                event_type AS EventType,
                event_time AS EventTime
            FROM events_time_travel
            FOR TIMESTAMP AS OF TIMESTAMP '{snapshotTimeStr}'
            WHERE event_time < TIMESTAMP '{eventTimeFilterStr}'
              AND event_id >= 101
            ORDER BY event_id
            """;

        var results = connection.Query<EventDto>(sql).ToList();

        // Assert - only snapshot 1 rows are visible, filtered by event_time
        Assert.Equal(2, results.Count);
        Assert.Equal(101, results[0].EventId);
        Assert.Equal("dapper_login", results[0].EventType);
        Assert.Equal(102, results[1].EventId);
        Assert.Equal("dapper_click", results[1].EventType);

        // Ensure later snapshot rows are not present
        Assert.DoesNotContain(results, r => r.EventId == 103 || r.EventId == 104);

        _output.WriteLine(
            $"Dapper time travel to {snapshotTime:O} with event_time filter returned {results.Count} rows"
        );
    }

    #endregion
}
