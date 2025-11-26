using AthenaTrinoClient;
using S3Client;

namespace IntegrationTests;

/// <summary>
/// Integration tests for TrinoClient implementation against a real Trino stack.
/// Tests Query&lt;T&gt; deserialization, Unload functionality, and FormattableString parameterization.
/// </summary>
public class AthenaClientTests
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _classFixture;
    private TrinoIcebergStack Stack => _classFixture.Stack;
    private string SchemaName => _classFixture.CommonSchemaName;

    public AthenaClientTests(ITestOutputHelper output, TrinoIcebergStackFixture classFixture)
    {
        _output = output;
        _classFixture = classFixture;
    }

    #region Query<T> Tests

    [Fact]
    public async Task Query_MappingAndNulls_AndEmptyResult()
    {
        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName);

        // People (simple types) - using shared_data with rows 1-3
        var people = await client.QueryAsync<PersonDto>($"SELECT id, name, age, active FROM shared_data WHERE id <= 3 ORDER BY id", TestContext.Current.CancellationToken);
        Assert.Equal(3, people.Count);
        Assert.True(people[0].Active);
        Assert.False(people[1].Active);

        // Contacts (null handling) - using shared_data with rows 1-3
        var contacts = await client.QueryAsync<ContactDto>($"SELECT id, name, email, phone FROM shared_data WHERE id <= 3 ORDER BY id", TestContext.Current.CancellationToken);
        Assert.Equal(3, contacts.Count);
        Assert.Null(contacts[1].Email); // Bob email null
        Assert.Null(contacts[2].Phone); // Charlie phone null

        // Employees (snake_case mapping) - using employee_data
        var employees = await client.QueryAsync<EmployeeDto>($"SELECT employee_id, first_name, last_name, hire_date FROM employee_data ORDER BY employee_id", TestContext.Current.CancellationToken);
        Assert.Equal(2, employees.Count);
        Assert.Equal(1, employees[0].EmployeeId);

        // Empty result (id > 999)
        var empty = await client.QueryAsync<PersonDto>($"SELECT id, name, age, active FROM shared_data WHERE id > 999", TestContext.Current.CancellationToken);
        Assert.Empty(empty);
    }

    [Fact]
    public async Task Query_NumericAndEscapingAndParameterized()
    {
        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName);

        // Measurements numeric - using shared_data row 4 (id=100)
        var measurements = await client.QueryAsync<MeasurementDto>($"SELECT id, value_int, value_double, value_decimal FROM shared_data WHERE id = 100", TestContext.Current.CancellationToken);
        Assert.Single(measurements);
        Assert.Equal(9223372036854775807L, measurements[0].ValueInt);
        Assert.Equal(3.14159, measurements[0].ValueDouble, precision: 5);
        Assert.NotNull(measurements[0].ValueDecimal);

        // Messages escaping (parameterized string) - using shared_data row 2
        var escaped = await client.QueryAsync<MessageDto>($"SELECT id, content FROM shared_data WHERE content = {"It's a test"}", TestContext.Current.CancellationToken);
        Assert.Single(escaped);
        Assert.Equal("It's a test", escaped[0].Content);

        // Parameterization (users) - using shared_data row 2
        var userId = 2;
        var users = await client.QueryAsync<UserDto>($"SELECT id, username FROM shared_data WHERE id = {userId}", TestContext.Current.CancellationToken);
        Assert.Single(users);
        Assert.Equal(2, users[0].Id);
        Assert.Equal("bob", users[0].Username);
    }

    [Fact]
    public async Task Unload_WithParameters_HandlesCorrectly()
    {
        using var s3Client = new MinioS3Client(
            endpoint: new Uri(Stack.MinioEndpoint),
            accessKey: "minioadmin",
            secretKey: "minioadmin",
            bucketName: "warehouse");

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName, s3Client);
        var exportPath = $"exports/sales_{Guid.NewGuid():N}";
        var category = "B";
        var response = await client.UnloadAsync($"SELECT * FROM category_data WHERE category = {category}", exportPath, TestContext.Current.CancellationToken);
        Assert.Equal(2, response.RowCount);
        Assert.Equal($"s3://warehouse/{exportPath}", response.S3AbsolutePath);
    }

    [Fact]
    public async Task Unload_PlacesDataFilesDirectlyAtTargetPath()
    {
        // Arrange
        using var s3Client = new MinioS3Client(
            endpoint: new Uri(Stack.MinioEndpoint),
            accessKey: "minioadmin",
            secretKey: "minioadmin",
            bucketName: "warehouse");

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName, s3Client);
        var exportPath = $"exports/unload_test_{Guid.NewGuid():N}";

        // Act
        var response = await client.UnloadAsync(
            $"SELECT id, name FROM shared_data WHERE id <= 3",
            exportPath,
            TestContext.Current.CancellationToken);

        // Assert - verify row count
        Assert.Equal(3, response.RowCount);
        Assert.Equal($"s3://warehouse/{exportPath}", response.S3AbsolutePath);

        // Assert - verify files are placed directly at target path (no data/ or metadata/ subfolders)
        var files = await s3Client.ListFilesAsync(exportPath, TestContext.Current.CancellationToken);

        _output.WriteLine($"Files at {exportPath}:");
        foreach (var file in files)
        {
            _output.WriteLine($"  - {file.Key} ({file.Size} bytes)");
        }

        // Should have parquet files directly at the target path
        Assert.NotEmpty(files);
        Assert.All(files, f =>
        {
            // Files should be directly under exportPath, not in data/ or metadata/ subfolders
            Assert.StartsWith(exportPath + "/", f.Key);
            Assert.DoesNotContain("/data/", f.Key);
            Assert.DoesNotContain("/metadata/", f.Key);
            // Should be parquet files
            Assert.EndsWith(".parquet", f.Key);
        });
    }

    [Fact]
    public async Task Unload_CleansUpTemporaryFiles()
    {
        // Arrange
        using var s3Client = new MinioS3Client(
            endpoint: new Uri(Stack.MinioEndpoint),
            accessKey: "minioadmin",
            secretKey: "minioadmin",
            bucketName: "warehouse");

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName, s3Client);
        var exportPath = $"exports/cleanup_test_{Guid.NewGuid():N}";

        // Act
        var response = await client.UnloadAsync(
            $"SELECT id, name FROM shared_data WHERE id = 1",
            exportPath,
            TestContext.Current.CancellationToken);

        // Assert - verify the operation succeeded
        Assert.Equal(1, response.RowCount);

        // Assert - verify temp files are cleaned up
        var tempFiles = await s3Client.ListFilesAsync("_unload_temp/", TestContext.Current.CancellationToken);

        _output.WriteLine($"Temp files remaining: {tempFiles.Count}");
        foreach (var file in tempFiles)
        {
            _output.WriteLine($"  - {file.Key}");
        }

        // Temp folder should be empty (or not exist) after cleanup
        Assert.Empty(tempFiles);
    }

    [Fact]
    public async Task Unload_WithoutS3Client_ThrowsInvalidOperationException()
    {
        // Arrange - create client without S3 client
        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName);
        var exportPath = $"exports/no_s3_test_{Guid.NewGuid():N}";

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await client.UnloadAsync(
                $"SELECT id FROM shared_data WHERE id = 1",
                exportPath,
                TestContext.Current.CancellationToken));

        Assert.Contains("S3 client is required", exception.Message);
        _output.WriteLine($"Expected exception: {exception.Message}");
    }

    [Fact]
    public async Task Unload_MultipleFiles_AllPlacedAtTargetPath()
    {
        // Arrange - create enough data to generate multiple parquet files
        using var s3Client = new MinioS3Client(
            endpoint: new Uri(Stack.MinioEndpoint),
            accessKey: "minioadmin",
            secretKey: "minioadmin",
            bucketName: "warehouse");

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName, s3Client);
        var exportPath = $"exports/multi_file_test_{Guid.NewGuid():N}";

        // Act - export data that includes multiple rows
        var response = await client.UnloadAsync(
            $"SELECT * FROM shared_data",
            exportPath,
            TestContext.Current.CancellationToken);

        // Assert
        Assert.True(response.RowCount > 0, "Expected at least one row");

        var files = await s3Client.ListFilesAsync(exportPath, TestContext.Current.CancellationToken);

        _output.WriteLine($"Exported {response.RowCount} rows to {files.Count} file(s):");
        foreach (var file in files)
        {
            _output.WriteLine($"  - {file.Key} ({file.Size} bytes)");
        }

        // All files should be parquet files directly at target path
        Assert.NotEmpty(files);
        Assert.All(files, f =>
        {
            Assert.StartsWith(exportPath + "/", f.Key);
            Assert.EndsWith(".parquet", f.Key);
            Assert.True(f.Size > 0, "File should have content");
        });
    }

    [Fact]
    public async Task Query_TimeTravelToTimestamp_WithTimestampColumnFilter()
    {
        // Arrange
        Stack.ExecuteNonQuery(
            // Snapshot 1 data
            "INSERT INTO events_time_travel VALUES (1, 'login', TIMESTAMP '2025-11-17 10:00:00'), (2, 'click', TIMESTAMP '2025-11-17 10:05:00')",
            SchemaName
        );

        // Ensure a distinct commit time boundary
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var timeTravelInstant = DateTime.UtcNow; // capture point between commits

        // Snapshot 2 data (should not be visible when traveling to timeTravelInstant)
        Stack.ExecuteNonQuery(
            "INSERT INTO events_time_travel VALUES (3, 'purchase', TIMESTAMP '2025-11-17 10:10:00'), (4, 'logout', TIMESTAMP '2025-11-17 10:15:00')",
            SchemaName
        );

        var client = new AthenaClient(new Uri(Stack.TrinoEndpoint), "iceberg", SchemaName);

        // Filter timestamp that only includes snapshot 1 rows
        var filterUpperBound = new DateTime(2025, 11, 17, 10, 07, 00, DateTimeKind.Utc);

        // Act - time travel query with DateTime parameters (AthenaClient handles formatting)
        var results = await client.QueryAsync<EventDto>(
            $"SELECT event_id, event_type, event_time FROM events_time_travel FOR TIMESTAMP AS OF TIMESTAMP {timeTravelInstant} WHERE event_time < {filterUpperBound} ORDER BY event_id",
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
