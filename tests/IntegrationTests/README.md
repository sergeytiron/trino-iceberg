# Integration Tests

Testcontainers-based integration tests for the Trino + Iceberg stack.

## Prerequisites

- .NET 8.0+
- Docker running

## Run Tests

```bash
# From repo root
dotnet test TrinoIcebergTests.slnx

# Specific test
dotnet test --filter "FullyQualifiedName~CanCreateAndQueryIcebergTable"

# Verbose
dotnet test --logger "console;verbosity=detailed"
```

## Key Files

| File | Purpose |
|------|---------|
| `TrinoIcebergStack.cs` | Container orchestration |
| `TrinoConfigurationProvider.cs` | Embedded Trino config |
| `TrinoIcebergStackFixture.cs` | Shared test fixture |
| `Scripts/create/` | Schema/table DDL scripts |
| `Scripts/insert/` | Data insertion scripts |

## Usage

```csharp
public class MyTests(TrinoIcebergStackFixture fixture)
{
    [Fact]
    public async Task MyTest()
    {
        // DDL/DML
        fixture.Stack.ExecuteNonQuery("CREATE TABLE iceberg.demo.t (id int)");
        
        // Type-safe SELECT
        var client = new AthenaClient(new Uri(fixture.Stack.TrinoEndpoint), "iceberg", "demo");
        var rows = await client.QueryAsync<MyDto>($"SELECT * FROM t WHERE id = {id}");
        var count = await client.QueryScalarAsync<long>($"SELECT count(*) FROM t");
    }
}
```

## Stack API

| Method | Description |
|--------|-------------|
| `ExecuteNonQuery(sql)` | Run DDL/DML via ADO.NET |
| `ExecuteBatch(statements)` | Run multiple statements |
| `TrinoEndpoint` | `http://localhost:{port}` |
| `MinioEndpoint` | `http://localhost:{port}` |

## Adding Test Data

**Recommended**: Add tables and data via SQL scripts in `Scripts/`:

```
Scripts/
├── create/          # DDL (executed first, alphabetically)
│   ├── 01-schema.sql
│   └── 02-tables.sql
└── insert/          # DML (executed second, alphabetically)
    └── 01-test-data.sql
```

Scripts run automatically on stack startup before tests execute.

## Notes

- Dynamic port mapping enables parallel test runs
- Trino config is embedded in C# (no file mounts)
- Auto-cleanup via `IAsyncDisposable`
- First run downloads images (~30s), subsequent runs are faster
