# Trino Iceberg Testcontainers - C#

C# Testcontainers implementation of the Trino + Nessie + MinIO stack for integration testing.

## Prerequisites

- .NET 8.0 SDK or later
- Docker running locally

## Project Structure

- `TrinoIcebergStack.cs` - Main stack orchestration (mirrors docker-compose.yml)
- `TrinoIcebergStackTests.cs` - Integration tests using the stack
- `TrinoIcebergTests.csproj` - Project file with NuGet references

## NuGet Packages

- `Testcontainers` (3.10.0) - Container orchestration
- `xunit` - Testing framework
- `Microsoft.NET.Test.Sdk` - Test SDK

## Stack Components

The `TrinoIcebergStack` class manages:

1. **Network** - Dedicated Docker network for container communication
2. **MinIO** - Object storage on ports 9000 (API) and 9001 (console)
3. **mc-init** - One-shot container to create the `warehouse` bucket
4. **Nessie** - Iceberg catalog on port 19120
5. **Trino** - Query engine on port 8080 with catalog config mounted from `../trino/etc`

## Run Tests

```bash
cd tests
dotnet.exe test
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
public class MyTests : IAsyncLifetime
{
    private TrinoIcebergStack? _stack;

    public async Task InitializeAsync()
    {
        _stack = new TrinoIcebergStack();
        await _stack.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (_stack != null)
            await _stack.DisposeAsync();
    }

    [Fact]
    public async Task CanQueryIceberg()
    {
        // Create schema
        await _stack!.ExecuteTrinoQueryAsync(
            "CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/')");

        // Create table
        await _stack.ExecuteTrinoQueryAsync(
            "CREATE TABLE iceberg.demo.numbers (n int) WITH (format='PARQUET')");

        // Insert data
        await _stack.ExecuteTrinoQueryAsync(
            "INSERT INTO iceberg.demo.numbers VALUES (1), (2), (3)");

        // Query
        var result = await _stack.ExecuteTrinoQueryAsync(
            "SELECT * FROM iceberg.demo.numbers ORDER BY n");

        Assert.Contains("\"1\"", result);
    }
}
```

## Stack Methods

### `StartAsync()`
Starts all containers in dependency order:
1. Network
2. MinIO
3. mc-init (bucket creation)
4. Nessie
5. Trino

### `ExecuteTrinoQueryAsync(string sql)`
Executes SQL via Trino CLI and returns output (stdout and stderr combined).

### `DisposeAsync()`
Stops and removes all containers and the network in reverse order of startup.

### Endpoint Properties
- `TrinoEndpoint` - http://localhost:{mapped-port}
- `NessieEndpoint` - http://localhost:{mapped-port}
- `MinioEndpoint` - http://localhost:{mapped-port}
- `MinioConsoleEndpoint` - http://localhost:{mapped-port}

## Notes

- All containers use dynamic port mapping for parallel test execution
- Trino config files are **embedded into the container** (copied at runtime, not mounted)
- Config directory can be specified via `TRINO_CONFIG_DIR` environment variable or auto-detected
- Proper wait strategies ensure containers are ready before tests run
- mc-init container completion is verified by exit code (no arbitrary delays)
- Error messages include both stdout and stderr for better debugging
- Containers auto-cleanup after tests via `IAsyncDisposable` in reverse order
- Robust disposal continues cleanup even if individual containers fail
- Network isolation prevents conflicts between test runs
- Input validation on ExecuteTrinoQueryAsync prevents empty SQL queries

## Configuration

### Trino Configuration Files

The Trino container needs configuration files from `trino/etc/`. By default, the stack looks for this directory relative to the test binary location. You can override this using the `TRINO_CONFIG_DIR` environment variable:

```bash
export TRINO_CONFIG_DIR=/absolute/path/to/trino/etc
dotnet test
```

**Expected directory structure:**
```
trino-iceberg/
├── trino/etc/
│   ├── config.properties
│   ├── jvm.config
│   ├── log.properties
│   ├── node.properties
│   └── catalog/
│       └── iceberg.properties
└── tests/
    ├── TrinoIcebergStack.cs
    └── ...
```

## Troubleshooting

**Config not found**: Set the `TRINO_CONFIG_DIR` environment variable to the absolute path of your `trino/etc` directory, or ensure the directory exists at the default relative path.

**Containers don't start**: Check Docker daemon is running and you have sufficient resources.

**Slow tests**: First run downloads images. Subsequent runs are faster.
