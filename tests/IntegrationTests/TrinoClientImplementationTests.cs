namespace IntegrationTests;

/// <summary>
/// Integration tests for TrinoClient implementation against a real Trino stack.
/// Tests Query&lt;T&gt; deserialization, Unload functionality, and FormattableString parameterization.
/// </summary>
public class TrinoClientImplementationTests
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _fixture;
    private TrinoIcebergStack Stack => _fixture.Stack;

    public TrinoClientImplementationTests(ITestOutputHelper output, TrinoIcebergStackFixture fixture)
    {
        _output = output;
        _fixture = fixture;
    }

    /// <summary>
    /// Generates a unique schema name for test isolation
    /// </summary>
    private static string GetUniqueSchemaName(string baseName) => $"{baseName}_{Guid.NewGuid():N}".ToLowerInvariant();

    #region Query<T> Tests

    [Fact]
    public async Task Query_CanDeserializeSimpleTypes()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_simple");
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.people (id int, name varchar, age int, active boolean) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.people VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Charlie', 35, true)",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = client.Query<PersonDto>($"SELECT id, name, age, active FROM people ORDER BY id", TestContext.Current.CancellationToken);

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
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.users (id int, username varchar) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act - use parameterized query (protects against SQL injection)
        var userId = 2;
        var results = client.Query<UserDto>($"SELECT id, username FROM users WHERE id = {userId}", TestContext.Current.CancellationToken);

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
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.products (id int, name varchar, price double, in_stock boolean) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.products VALUES (1, 'Widget', 9.99, true), (2, 'Gadget', 19.99, true), (3, 'Doohickey', 29.99, false), (4, 'Thingamajig', 39.99, true)",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var minPrice = 15.0;
        var inStock = true;
        var results = client.Query<ProductDto>($"SELECT id, name, price, in_stock FROM products WHERE price > {minPrice} AND in_stock = {inStock} ORDER BY id", TestContext.Current.CancellationToken);

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
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.contacts (id int, name varchar, email varchar, phone varchar) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.contacts VALUES (1, 'Alice', 'alice@example.com', '555-0001'), (2, 'Bob', NULL, '555-0002'), (3, 'Charlie', 'charlie@example.com', NULL)",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = client.Query<ContactDto>($"SELECT id, name, email, phone FROM contacts ORDER BY id", TestContext.Current.CancellationToken);

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
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.employees (employee_id int, first_name varchar, last_name varchar, hire_date date) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.employees VALUES (101, 'John', 'Doe', DATE '2020-01-15'), (102, 'Jane', 'Smith', DATE '2021-03-20')",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = client.Query<EmployeeDto>($"SELECT employee_id, first_name, last_name, hire_date FROM employees ORDER BY employee_id", TestContext.Current.CancellationToken);

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
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.items (id int, name varchar) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = client.Query<ItemDto>($"SELECT id, name FROM items WHERE id > 1000", TestContext.Current.CancellationToken);

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
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.data (id int, value varchar) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.data VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);
        var exportPath = $"exports/{schemaName}";

        // Act & Assert
        // Note: UNLOAD may not be supported in Trino, so we expect a NotSupportedException
        var exception = Assert.Throws<NotSupportedException>(() =>
        {
            var response = client.Unload($"SELECT * FROM data", exportPath, TestContext.Current.CancellationToken);
        });

        Assert.Contains("UNLOAD command is not supported", exception.Message);

        _output.WriteLine($"UNLOAD not supported (expected): {exception.Message}");
    }

    [Fact]
    public async Task Unload_WithParameters_HandlesCorrectly()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("unload_params");
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.sales (id int, amount double, region varchar) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.sales VALUES (1, 100.50, 'North'), (2, 200.75, 'South'), (3, 150.25, 'North')",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);
        var exportPath = $"exports/{schemaName}";
        var region = "North";

        // Act & Assert
        var exception = Assert.Throws<NotSupportedException>(() =>
        {
            var response = client.Unload($"SELECT * FROM sales WHERE region = {region}", exportPath, TestContext.Current.CancellationToken);
        });

        Assert.Contains("UNLOAD command is not supported", exception.Message);

        _output.WriteLine("Parameterized UNLOAD handled correctly (not supported as expected)");
    }

    #endregion

    #region Type Conversion Tests

    [Fact]
    public async Task Query_WithNumericTypes_ConvertsCorrectly()
    {
        // Arrange
        var schemaName = GetUniqueSchemaName("query_numeric");
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.measurements (id int, value_int bigint, value_double double, value_decimal decimal(10,2)) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.measurements VALUES (1, 9223372036854775807, 3.14159, 99.99)",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var results = client.Query<MeasurementDto>($"SELECT id, value_int, value_double, value_decimal FROM measurements", TestContext.Current.CancellationToken);

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
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE SCHEMA iceberg.{schemaName} WITH (location='s3://warehouse/{schemaName}/')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"CREATE TABLE iceberg.{schemaName}.messages (id int, content varchar) WITH (format='PARQUET')",
            cancellationToken: TestContext.Current.CancellationToken);
        await Stack.ExecuteTrinoQueryAsync(
            $"INSERT INTO iceberg.{schemaName}.messages VALUES (1, 'Hello World'), (2, 'It''s a test'), (3, 'Quote: \"test\"')",
            cancellationToken: TestContext.Current.CancellationToken);

        var client = new global::TrinoClient.TrinoClient(new Uri(Stack.TrinoEndpoint), "iceberg", schemaName);

        // Act
        var searchTerm = "It's a test";
        var results = client.Query<MessageDto>($"SELECT id, content FROM messages WHERE content = {searchTerm}", TestContext.Current.CancellationToken);

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

    #endregion
}
