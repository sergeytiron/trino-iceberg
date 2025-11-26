# AI Coding Agents – Trino + Iceberg Repo Guide

Use this guide to work productively in this repo. It documents the architecture, workflows, and project-specific conventions you'll need to extend tests or the local stack confidently.

## Big Picture
- Stack: Trino queries Iceberg via the Nessie catalog; table data lives in MinIO S3. Two ways to run:
  - Docker Compose with config files in `trino/etc/**`.
  - C# integration tests using Testcontainers with Trino config embedded in code (no file mounts).
- Service topology (Compose and Tests): `trino` → `nessie` → `minio` (S3). Network aliases: `trino`, `nessie`, `minio`.

## Key Services and Versions
- Trino: `trinodb/trino:478` on port 8080.
- Nessie: `ghcr.io/projectnessie/nessie:latest` on port 19120 (IN_MEMORY store by default).
- MinIO: `minio/minio:latest` on ports 9000 (S3) and 9001 (console). Bucket: `warehouse`.

## Developer Workflows
- Bring up the local stack (Compose):
  - `docker compose up -d --build`
  - Validate: `./validate.sh` (requires bash; on Windows use Git Bash/WSL).
- Run all .NET tests:
  - From repo root: `dotnet test TrinoIcebergTests.slnx`
  - From `tests/`: `dotnet test`
- Targeted/verbose tests:
  - `dotnet test --filter "FullyQualifiedName~CanCreateAndQueryIcebergTable"`
  - `dotnet test --logger "console;verbosity=detailed"`

## Test Stack Patterns (C# / Testcontainers)
- Entry point: `tests/IntegrationTests/TrinoIcebergStack.cs` orchestrates network + 3 containers. Ports are dynamically mapped for parallel runs.
- Trino config is provided via in-memory bytes mapped into `/etc/trino/**` using `WithResourceMapping` from `TrinoConfigurationProvider`.
- MinIO bucket init happens with `mc` via `Exec` in the MinIO container (no separate `mc` container):
  - `mc alias set local http://localhost:9000 ... && mc mb -p local/warehouse || true`
- Shared fixture across tests:
  - `TrinoIcebergStackFixture` is created once per test run and injected via constructor.
- Query execution from tests uses ADO.NET via the Trino C# Client (faster than CLI exec):
  - `ExecuteNonQuery("SQL...")` → executes DDL/DML and returns rows affected
  - `ExecuteBatch(IEnumerable<string>)` → executes multiple statements with connection reuse

## Trino Configuration (Two Sources)
- Compose mode uses files under `trino/etc/**`:
  - `config.properties`, `node.properties`, `log.properties`, `jvm.config`, `catalog/iceberg.properties`.
- Test mode uses `tests/IntegrationTests/TrinoConfigurationProvider.cs` with equivalent content embedded as string literals.
  - Modify `Get*Bytes()` methods to change Trino behavior for tests.

## Conventions and Gotchas
- S3 path-style addressing required: `s3.path-style-access=true`; endpoint is `http://minio:9000` (inside containers) and `http://localhost:{mapped}` (from host/tests).
- Nessie default store is in-memory; state resets on container restart. For persistence, switch to JDBC in Compose (not wired in tests).
- Wait strategies ensure readiness: MinIO HTTP health, Nessie `/api/v2/config`, Trino `"SERVER STARTED"` log.
- Tests rely on dynamic host ports; use `Stack.TrinoEndpoint`/`NessieEndpoint`/`MinioEndpoint` properties rather than assuming fixed ports.

## Example Test Flow
```csharp
public class MyTests(TrinoIcebergStackFixture fixture)
{
    [Fact]
    public async Task CanQueryData()
    {
        fixture.Stack.ExecuteNonQuery("CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/')");
        fixture.Stack.ExecuteNonQuery("CREATE TABLE iceberg.demo.numbers (n int) WITH (format='PARQUET')");
        fixture.Stack.ExecuteNonQuery("INSERT INTO iceberg.demo.numbers VALUES (1),(2),(3)");
        
        // Use AthenaClient for type-safe SELECT queries
        var client = new AthenaClient(new Uri(fixture.Stack.TrinoEndpoint), "iceberg", "demo");
        var rows = await client.QueryAsync<NumberDto>($"SELECT * FROM numbers ORDER BY n");
        
        // Use QueryScalarAsync for single-value results (aggregates, counts, etc.)
        var count = await client.QueryScalarAsync<long>($"SELECT count(*) FROM numbers");
        var max = await client.QueryScalarAsync<int?>($"SELECT max(n) FROM numbers");
    }
}
```

## S3Client Library
The `src/S3Client/` project provides direct S3/MinIO access:
- `IS3Client` interface with `UploadFileAsync`, `DownloadFileAsync`, `ListFilesAsync`
- `MinioS3Client` implementation using AWSSDK.S3 with path-style addressing
- Integration tests in `tests/IntegrationTests/S3ClientTests.cs`

Example usage in tests:
```csharp
var s3Client = new MinioS3Client(
    endpoint: new Uri(fixture.Stack.MinioEndpoint),
    accessKey: "minioadmin",
    secretKey: "minioadmin",
    bucketName: "warehouse"
);
await s3Client.UploadFileAsync(localPath, "path/in/s3/file.txt");
var files = await s3Client.ListFilesAsync("path/in/s3/");
```

## Useful Paths
- Compose configs: `trino/etc/**`
- Test stack: `tests/IntegrationTests/TrinoIcebergStack.cs`, `tests/IntegrationTests/TrinoConfigurationProvider.cs`
- S3 client: `src/S3Client/IS3Client.cs`, `src/S3Client/MinioS3Client.cs`
- Tests: `tests/IntegrationTests/`, `tests/UnitTests/`
- Validation script: `validate.sh`

If anything here is unclear or missing (e.g., adding JDBC-backed Nessie, auth, TLS), tell me what you're trying to do and I'll extend these instructions.
