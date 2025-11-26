# Copilot Instructions

## Project Overview

.NET integration testing stack: Trino + Iceberg + Nessie + MinIO. Mirrors AWS Athena architecture for local development.

## Architecture

```
Trino (8080) → Nessie (19120) → MinIO (9000)
     ↓              ↓               ↓
  Queries      Catalog          S3 Storage
```

**Two run modes:**
1. **Docker Compose**: `docker-compose.yml` with inline config for manual testing
2. **Testcontainers**: `TrinoIcebergStack.cs` with embedded config (preferred for tests)

## Key Components

| Component | Path | Purpose |
|-----------|------|---------|
| Container Stack | `tests/IntegrationTests/TrinoIcebergStack.cs` | Testcontainers orchestration (MinIO → Nessie → Trino) |
| Trino Config | `tests/IntegrationTests/TrinoConfigurationProvider.cs` | **All Trino config is embedded here as C# strings** |
| Test Fixture | `tests/IntegrationTests/TrinoIcebergStackFixture.cs` | xUnit 3 AssemblyFixture for shared stack |
| Query Client | `src/AthenaTrinoClient/AthenaClient.cs` | Type-safe Trino queries with `FormattableString` |
| S3 Client | `src/S3Client/MinioS3Client.cs` | MinIO/S3 file operations |

## Test Patterns

Tests inject `TrinoIcebergStackFixture` and use `ITestOutputHelper`:

```csharp
public class MyTests
{
    private readonly TrinoIcebergStackFixture _fixture;
    
    public MyTests(ITestOutputHelper output, TrinoIcebergStackFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task MyTest()
    {
        // DDL/DML via ExecuteNonQuery (uses ADO.NET internally)
        _fixture.Stack.ExecuteNonQuery("CREATE TABLE iceberg.myschema.t (id int)", "myschema");
        
        // SELECT via AthenaClient (uses FormattableString for safe parameterization)
        var client = new AthenaClient(new Uri(_fixture.Stack.TrinoEndpoint), "iceberg", "myschema");
        var rows = await client.QueryAsync<MyDto>($"SELECT * FROM t WHERE id = {id}");
        var count = await client.QueryScalarAsync<long>($"SELECT count(*) FROM t");
    }
}
```

**Query Parameterization**: Always use `FormattableString` (`$"...{param}..."`) - parameters are escaped via `SqlParameterFormatter.cs`. Never use string concatenation.

## Init Scripts Convention

Auto-executed on stack startup in alphabetical order:
```
tests/IntegrationTests/Scripts/
├── create/          # DDL first (schemas, tables)
│   ├── 01-schema.sql
│   └── 02-tables.sql
└── insert/          # Then DML (test data)
    └── 01-test-data.sql
```

Use `iceberg.common_test_data` schema for shared test tables (available via `_fixture.CommonSchemaName`).

## Commands

```bash
dotnet test TrinoIcebergTests.slnx                          # All tests
dotnet test --filter "FullyQualifiedName~CanCreateAndQuery" # Specific test
dotnet test --logger "console;verbosity=detailed"           # Verbose

docker compose up -d     # Manual stack (fixed ports)
docker compose down -v   # Reset state (Nessie is in-memory)
```

## Critical Implementation Details

- **Dynamic ports**: Always use `fixture.Stack.TrinoEndpoint` / `MinioEndpoint`, never hardcoded ports
- **Trino config changes**: Edit `TrinoConfigurationProvider.cs` embedded strings, not external files
- **MinIO path-style**: `ForcePathStyle = true` required (see `MinioS3Client.cs`)
- **Iceberg catalog**: Uses Nessie catalog type, location pattern: `s3://warehouse/{path}/`
- **xUnit 3**: Uses `TestContext.Current.CancellationToken` instead of `CancellationToken.None`
- **Multi-target**: Tests run on both `net8.0` and `net10.0`