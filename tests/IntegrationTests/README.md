# Trino Iceberg Testcontainers - C#

C# Testcontainers implementation of the Trino + Nessie + MinIO stack for integration testing.

## Prerequisites

- .NET 8.0 SDK or later
- Docker running locally

## Project Structure

- `TrinoIcebergStack.cs` - Main stack orchestration (mirrors docker-compose.yml)
- `TrinoConfigurationProvider.cs` - Embedded Trino configuration (no external files needed)
- `TrinoIcebergStackFixture.cs` - Shared fixture for all tests (created once per test run)
- `Scripts/` - SQL init scripts for test data setup
- `IntegrationTests.csproj` - Project file with NuGet references

## NuGet Packages

- `Testcontainers` (3.10.0) - Container orchestration
- `xunit` - Testing framework
- `Microsoft.NET.Test.Sdk` - Test SDK
- `Trino.Data.ADO` - Trino C# Client for ADO.NET queries
- `AWSSDK.S3` - AWS SDK for S3 operations (used by S3Client)

## Stack Components

The `TrinoIcebergStack` class manages:

1. **Network** - Dedicated Docker network for container communication
2. **MinIO** - Object storage on ports 9000 (API) and 9001 (console), with bucket initialization via exec
3. **Nessie** - Iceberg catalog on port 19120
4. **Trino** - Query engine on port 8080 with embedded configuration

## Run Tests

From the repository root:
```bash
dotnet test TrinoIcebergTests.slnx
```

Or from the tests directory:
```bash
cd tests
dotnet test
```

## Run Specific Test

```bash
dotnet.exe test --filter "FullyQualifiedName~CanCreateAndQueryIcebergTable"
```

## Verbose Output

```bash
dotnet.exe test --logger "console;verbosity=detailed"
```

## Example Usage

```csharp
public class MyTests(TrinoIcebergStackFixture fixture)
{
    private readonly TrinoIcebergStack _stack = fixture.Stack;

    [Fact]
    public async Task CanQueryIceberg()
    {
        // Create schema
        _stack.ExecuteNonQuery(
            "CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/')");

        // Create table
        _stack.ExecuteNonQuery(
            "CREATE TABLE iceberg.demo.numbers (n int) WITH (format='PARQUET')");

        // Insert data
        _stack.ExecuteNonQuery(
            "INSERT INTO iceberg.demo.numbers VALUES (1), (2), (3)");

        // Query using AthenaClient for type-safe results
        var client = new AthenaClient(new Uri(_stack.TrinoEndpoint), "iceberg", "demo");
        var rows = await client.Query<NumberDto>($"SELECT * FROM numbers ORDER BY n");

        // Use QueryScalar for single-value results
        var count = await client.QueryScalar<long>($"SELECT count(*) FROM numbers");

        Assert.Equal(3, rows.Count);
        Assert.Equal(3L, count);
    }
}
```

## Stack Methods

### `StartAsync()`
Starts all containers in dependency order:
1. Network
2. MinIO and Nessie (in parallel)
3. MinIO bucket initialization and Trino startup (in parallel)
4. Init scripts execution (if configured)

### `WithInitScript(string schema, string resourcePath)`
Configures a `.sql` file to be executed after the stack starts. The schema is created automatically.

```csharp
var stack = new TrinoIcebergStack()
    .WithInitScript("my_schema", "Scripts/init.sql");
await stack.StartAsync();
```

SQL files support:
- Semicolon-separated statements
- Single-line comments (`--`)
- Multi-line comments (`/* */`)
- String literals with quotes

### Query Execution Methods (ADO.NET)

#### `ExecuteNonQuery(string sql, string? schema = null)`
Executes DDL/DML statements via ADO.NET. Returns rows affected.

#### `ExecuteBatch(IEnumerable<string> sqlStatements, string? schema = null)`
Executes multiple statements with connection reuse (parallel execution).

> **Note**: For SELECT queries, use `AthenaClient` which provides type-safe deserialization.

### `DisposeAsync()`
Stops and removes all containers and the network in reverse order of startup.

### Endpoint Properties
- `TrinoEndpoint` - http://localhost:{mapped-port}
- `MinioEndpoint` - http://localhost:{mapped-port}

### S3Client Usage

The `MinioS3Client` provides direct S3 access to MinIO for file operations:

```csharp
using S3Client;

public class S3Tests(TrinoIcebergStackFixture fixture)
{
    [Fact]
    public async Task CanUploadAndDownload()
    {
        var s3Client = new MinioS3Client(
            endpoint: new Uri(fixture.Stack.MinioEndpoint),
            accessKey: "minioadmin",
            secretKey: "minioadmin",
            bucketName: "warehouse"
        );

        // Upload a file
        await s3Client.UploadFileAsync("local.txt", "test/file.txt");

        // List files
        var files = await s3Client.ListFilesAsync("test/");
        Assert.Single(files);

        // Download
        await s3Client.DownloadFileAsync("test/file.txt", "downloaded.txt");
    }
}
```

## Notes

- All containers use dynamic port mapping for parallel test execution
- **Trino config files are embedded in C#** via `TrinoConfigurationProvider` - no external files needed
- Configuration is completely self-contained within the test code
- **MinIO bucket initialization uses exec** - no separate mc-init container needed
- Proper wait strategies ensure containers are ready before tests run
- Uses ADO.NET via Trino C# Client for faster query execution than CLI
- Containers auto-cleanup after tests via `IAsyncDisposable` in reverse order
- Robust disposal continues cleanup even if individual containers fail
- Network isolation prevents conflicts between test runs
- `TrinoIcebergStackFixture` is created once per test run and injected via constructor

## Configuration

### Trino Configuration

All Trino configuration is **embedded in `TrinoConfigurationProvider.cs`** using C# string literals. No external configuration files, directories, or environment variables are needed. The configuration includes:

- `config.properties` - Trino coordinator settings
- `node.properties` - Node identification and data directory
- `log.properties` - Logging configuration
- `jvm.config` - JVM settings for the Trino server
- `catalog/iceberg.properties` - Iceberg connector with Nessie catalog and MinIO S3 settings

To modify configuration, edit the `Get*Bytes()` methods in `TrinoConfigurationProvider.cs`.

## Troubleshooting

**Containers don't start**: Check Docker daemon is running and you have sufficient resources.

**Slow tests**: First run downloads images. Subsequent runs are faster.
