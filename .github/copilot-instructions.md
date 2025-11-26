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
1. **Docker Compose**: Uses `docker-compose.yml` with inline config
2. **Testcontainers**: Uses `TrinoIcebergStack.cs` with embedded config in `TrinoConfigurationProvider.cs`

## Key Files

| File | Purpose |
|------|---------|
| `tests/IntegrationTests/TrinoIcebergStack.cs` | Container orchestration |
| `tests/IntegrationTests/TrinoConfigurationProvider.cs` | Embedded Trino config |
| `tests/IntegrationTests/TrinoIcebergStackFixture.cs` | Shared test fixture |
| `src/AthenaTrinoClient/AthenaClient.cs` | Type-safe query client |
| `src/S3Client/MinioS3Client.cs` | S3/MinIO operations |

## Code Patterns

### Test Structure

```csharp
public class MyTests(TrinoIcebergStackFixture fixture)
{
    [Fact]
    public async Task MyTest()
    {
        // DDL/DML via ExecuteNonQuery
        fixture.Stack.ExecuteNonQuery("CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/')");
        
        // SELECT via AthenaClient
        var client = new AthenaClient(new Uri(fixture.Stack.TrinoEndpoint), "iceberg", "demo");
        var rows = await client.QueryAsync<MyDto>($"SELECT * FROM table WHERE id = {id}");
        var count = await client.QueryScalarAsync<long>($"SELECT count(*) FROM table");
    }
}
```

### S3 Operations

```csharp
var s3 = new MinioS3Client(
    new Uri(fixture.Stack.MinioEndpoint),
    "minioadmin", "minioadmin", "warehouse"
);
await s3.UploadFileAsync(localPath, "s3/key");
```

## Commands

```bash
# Run tests
dotnet test TrinoIcebergTests.slnx

# Run specific test
dotnet test --filter "FullyQualifiedName~TestName"

# Docker Compose
docker compose up -d
docker compose down -v  # Reset state
```

## Important Notes

- **Dynamic ports**: Always use `fixture.Stack.TrinoEndpoint` / `MinioEndpoint`, never hardcoded ports
- **S3 path-style**: Required for MinIO (`s3.path-style-access=true`)
- **Nessie in-memory**: State resets on container restart
- **Trino config in tests**: Modify `TrinoConfigurationProvider.cs`, not external files
- **Init scripts**: Place in `Scripts/create/` and `Scripts/insert/` folders
