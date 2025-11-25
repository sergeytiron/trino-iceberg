using AthenaTrinoClient;

namespace IntegrationTests;

/// <summary>
/// Class fixture for AthenaClientTests - uses the shared common schema from assembly fixture.
/// No additional setup queries needed - reuses pre-populated test data.
/// </summary>
public sealed class AthenaClientTestsClassFixture : IAsyncLifetime
{
    private readonly TrinoIcebergStackFixture _stackFixture;
    
    /// <summary>
    /// Uses the shared schema from assembly fixture - no separate schema creation needed.
    /// </summary>
    public string SchemaName => _stackFixture.CommonSchemaName;
    public TrinoIcebergStack Stack => _stackFixture.Stack;

    public AthenaClientTestsClassFixture(TrinoIcebergStackFixture stackFixture)
    {
        _stackFixture = stackFixture;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Integration tests for TrinoClient implementation against a real Trino stack.
/// Tests Query&lt;T&gt; deserialization, Unload functionality, and FormattableString parameterization.
/// </summary>
public class AthenaClientTests : IClassFixture<AthenaClientTestsClassFixture>
{
    private readonly ITestOutputHelper _output;
    private readonly AthenaClientTestsClassFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.SchemaName;

    public AthenaClientTests(ITestOutputHelper output, AthenaClientTestsClassFixture classFixture)
    {
        _output = output;
        _classFixture = classFixture;
    }

    /// <summary>
    /// Generates a unique schema name for tests that need isolated schemas
    /// </summary>
    private static string GetUniqueSchemaName(string baseName) => $"{baseName}_{Guid.NewGuid():N}".ToLowerInvariant();

    #region Query<T> Tests

    [Fact]
    public async Task Query_TimeTravelToTimestamp_WithTimestampColumnFilter()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_timetravel");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.events (event_id bigint, event_type varchar, event_time timestamp) WITH (format='PARQUET')",
            // Snapshot 1 data
            $"INSERT INTO iceberg.{schemaName}.events VALUES "
                + "(1, 'login', TIMESTAMP '2025-11-17 10:00:00'), "
                + "(2, 'click', TIMESTAMP '2025-11-17 10:05:00')"
        ]);

        // Ensure a distinct commit time boundary
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        var timeTravelInstant = DateTime.UtcNow; // capture point between commits

        // Snapshot 2 data (should not be visible when traveling to timeTravelInstant)
        Stack.ExecuteSqlFast(
            $"INSERT INTO iceberg.{schemaName}.events VALUES "
                + "(3, 'purchase', TIMESTAMP '2025-11-17 10:10:00'), "
                + "(4, 'logout', TIMESTAMP '2025-11-17 10:15:00')"
        );

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Filter timestamp that only includes snapshot 1 rows
        var filterUpperBound = new DateTime(2025, 11, 17, 10, 07, 00, DateTimeKind.Utc);

        // Act - time travel query with DateTime parameters (AthenaClient handles formatting)
        var results = await client.Query<EventDto>(
            $"SELECT event_id, event_type, event_time FROM events FOR TIMESTAMP AS OF TIMESTAMP {timeTravelInstant} WHERE event_time < {filterUpperBound} ORDER BY event_id",
            TestContext.Current.CancellationToken
        );

        // Assert - only snapshot 1 rows are visible
        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].EventId);
        Assert.Equal("login", results[0].EventType);
        Assert.Equal(2, results[1].EventId);
        Assert.Equal("click", results[1].EventType);

        // Ensure later snapshot rows are not present
        Assert.DoesNotContain(results, r => r.EventId == 3 || r.EventId == 4);

        _output.WriteLine($"Time travel to {timeTravelInstant:O} returned {results.Count} rows; snapshot 2 rows excluded as expected.");
    }

    [Fact]
    public async Task Query_CanDeserializeSimpleTypes()
    {
        // Use pre-created people table from class fixture
        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName);

        // Act
        var results = await client.Query<PersonDto>(
            $"SELECT id, name, age, active FROM people ORDER BY id",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Equal(3, results.Count);

        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(30, results[0].Age);
        Assert.True(results[0].Active);

        Assert.Equal(2, results[1].Id);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal(25, results[1].Age);
        Assert.False(results[1].Active);

        Assert.Equal(3, results[2].Id);
        Assert.Equal("Charlie", results[2].Name);
        Assert.Equal(35, results[2].Age);
        Assert.True(results[2].Active);

        _output.WriteLine($"Successfully deserialized {results.Count} records");
    }

    [Fact]
    public async Task Query_WithParameters_PreventsSqlInjection()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_params");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.users (id int, username varchar) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act - use parameterized query (protects against SQL injection)
        var userId = 2;
        var results = await client.Query<UserDto>(
            $"SELECT id, username FROM users WHERE id = {userId}",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Single(results);
        Assert.Equal(2, results[0].Id);
        Assert.Equal("bob", results[0].Username);

        _output.WriteLine($"Parameterized query executed safely, returned {results.Count} record(s)");
    }

    [Fact]
    public async Task Query_WithMultipleParameters()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_multi_params");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.products (id int, name varchar, price double, in_stock boolean) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.products VALUES (1, 'Widget', 9.99, true), (2, 'Gadget', 19.99, true), (3, 'Doohickey', 29.99, false), (4, 'Thingamajig', 39.99, true)"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var minPrice = 15.0;
        var inStock = true;
        var results = await client.Query<ProductDto>(
            $"SELECT id, name, price, in_stock FROM products WHERE price > {minPrice} AND in_stock = {inStock} ORDER BY id",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(2, results[0].Id);
        Assert.Equal("Gadget", results[0].Name);
        Assert.Equal(4, results[1].Id);
        Assert.Equal("Thingamajig", results[1].Name);

        _output.WriteLine($"Multi-parameter query returned {results.Count} matching records");
    }

    [Fact]
    public async Task Query_WithNullValues_HandlesCorrectly()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_nulls");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.contacts (id int, name varchar, email varchar, phone varchar) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.contacts VALUES (1, 'Alice', 'alice@example.com', '555-0001'), (2, 'Bob', NULL, '555-0002'), (3, 'Charlie', 'charlie@example.com', NULL)"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = await client.Query<ContactDto>(
            $"SELECT id, name, email, phone FROM contacts ORDER BY id",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Equal(3, results.Count);

        Assert.Equal(1, results[0].Id);
        Assert.Equal("alice@example.com", results[0].Email);
        Assert.Equal("555-0001", results[0].Phone);

        Assert.Equal(2, results[1].Id);
        Assert.Null(results[1].Email);
        Assert.Equal("555-0002", results[1].Phone);

        Assert.Equal(3, results[2].Id);
        Assert.Equal("charlie@example.com", results[2].Email);
        Assert.Null(results[2].Phone);

        _output.WriteLine("NULL values handled correctly in deserialization");
    }

    [Fact]
    public async Task Query_WithSnakeCaseColumns_MapsCorrectly()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_snake_case");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.employees (employee_id int, first_name varchar, last_name varchar, hire_date date) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.employees VALUES (101, 'John', 'Doe', DATE '2020-01-15'), (102, 'Jane', 'Smith', DATE '2021-03-20')"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = await client.Query<EmployeeDto>(
            $"SELECT employee_id, first_name, last_name, hire_date FROM employees ORDER BY employee_id",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(101, results[0].EmployeeId);
        Assert.Equal("John", results[0].FirstName);
        Assert.Equal("Doe", results[0].LastName);

        _output.WriteLine("snake_case column names mapped to PascalCase properties successfully");
    }

    [Fact]
    public async Task Query_EmptyResultSet_ReturnsEmptyList()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_empty");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.items (id int, name varchar) WITH (format='PARQUET')"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = await client.Query<ItemDto>(
            $"SELECT id, name FROM items WHERE id > 1000",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Empty(results);

        _output.WriteLine("Empty result set handled correctly");
    }

    #endregion

    #region Unload Tests

    [Fact]
    public async Task Unload_ExecutesSuccessfully_ReturnsMetadata()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("unload_test");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.data (id int, value varchar) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.data VALUES (1, 'one'), (2, 'two'), (3, 'three')"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);
        var exportPath = $"exports/{schemaName}";

        // Act
        var response = await client.Unload($"SELECT * FROM data", exportPath, TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(3, response.RowCount);
        Assert.Equal($"s3://warehouse/exports/{schemaName}", response.S3AbsolutePath);

        _output.WriteLine($"Successfully unloaded {response.RowCount} rows to {response.S3AbsolutePath}");

        // Verify the data was written to S3 by querying the files
        var verifyResults = Stack.ExecuteQueryFast(
            $"SELECT COUNT(*) FROM iceberg.\"$path\".files WHERE path LIKE '%exports/{schemaName}%'"
        );
        _output.WriteLine($"Verification query result: {verifyResults.FirstOrDefault()?.FirstOrDefault() ?? "no rows"}");
    }

    [Fact]
    public async Task Unload_WithParameters_HandlesCorrectly()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("unload_params");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.sales (id int, amount double, region varchar) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.sales VALUES (1, 100.50, 'North'), (2, 200.75, 'South'), (3, 150.25, 'North')"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);
        var exportPath = $"exports/{schemaName}";
        var region = "North";

        // Act
        var response = await client.Unload(
            $"SELECT * FROM sales WHERE region = {region}",
            exportPath,
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Equal(2, response.RowCount); // Two records with region='North'
        Assert.Equal($"s3://warehouse/exports/{schemaName}", response.S3AbsolutePath);

        _output.WriteLine(
            $"Successfully unloaded {response.RowCount} rows with parameterized query to {response.S3AbsolutePath}"
        );
    }

    #endregion

    #region Type Conversion Tests

    [Fact]
    public async Task Query_WithNumericTypes_ConvertsCorrectly()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_numeric");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.measurements (id int, value_int bigint, value_double double, value_decimal decimal(10,2)) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.measurements VALUES (1, 9223372036854775807, 3.14159, 99.99)"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = await client.Query<MeasurementDto>(
            $"SELECT id, value_int, value_double, value_decimal FROM measurements",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal(9223372036854775807L, results[0].ValueInt);
        Assert.Equal(3.14159, results[0].ValueDouble, precision: 5);

        _output.WriteLine("Numeric type conversions handled correctly");
    }

    [Fact]
    public async Task Query_WithStringParameter_EscapesCorrectly()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_string_escape");
        Stack.ExecuteSqlBatchFast([
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            $"CREATE TABLE iceberg.{schemaName}.messages (id int, content varchar) WITH (format='PARQUET')",
            $"INSERT INTO iceberg.{schemaName}.messages VALUES (1, 'Hello World'), (2, 'It''s a test'), (3, 'Quote: \"test\"')"
        ]);

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var searchTerm = "It's a test";
        var results = await client.Query<MessageDto>(
            $"SELECT id, content FROM messages WHERE content = {searchTerm}",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Single(results);
        Assert.Equal(2, results[0].Id);
        Assert.Equal("It's a test", results[0].Content);

        _output.WriteLine("String parameter with quotes handled correctly");
    }

    #endregion

    #region DTO Classes

    public class PersonDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public bool Active { get; set; }
    }

    public class UserDto
    {
        public int Id { get; set; }
        public string Username { get; set; } = string.Empty;
    }

    public class ProductDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public double Price { get; set; }
        public bool In_Stock { get; set; } // Tests exact column name matching
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
        public int EmployeeId { get; set; } // Maps from employee_id
        public string FirstName { get; set; } = string.Empty; // Maps from first_name
        public string LastName { get; set; } = string.Empty; // Maps from last_name
        public object? HireDate { get; set; } // DATE type from Trino
    }

    public class ItemDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class MeasurementDto
    {
        public int Id { get; set; }
        public long ValueInt { get; set; } // Maps from value_int
        public double ValueDouble { get; set; } // Maps from value_double
        public object? ValueDecimal { get; set; } // Maps from value_decimal (decimal handling)
    }

    public class MessageDto
    {
        public int Id { get; set; }
        public string Content { get; set; } = string.Empty;
    }

    public class EventDto
    {
        public long EventId { get; set; }
        public string EventType { get; set; } = string.Empty;
        public DateTime EventTime { get; set; }
    }

    #endregion
}
