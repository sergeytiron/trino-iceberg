# Trino Iceberg Local Stack

Local Trino + Iceberg + MinIO stack for AWS Athena integration testing in .NET.

## Quick Start

```bash
# Start the stack
docker compose up -d

# Run integration tests
dotnet test TrinoIcebergTests.slnx
```

**UIs**: Trino http://localhost:8080 | MinIO http://localhost:9001 (minioadmin/minioadmin)

## Stack Components

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| Trino | `trinodb/trino:478` | 8080 | Query engine |
| Nessie | `ghcr.io/projectnessie/nessie:latest` | 19120 | Iceberg catalog |
| MinIO | `minio/minio:latest` | 9000, 9001 | S3-compatible storage |

## Libraries

### AthenaClient

Type-safe Trino queries with FormattableString parameterization:

```csharp
var client = new AthenaClient(new Uri("http://localhost:8080"), "iceberg", "demo");

// Type-safe query
var users = await client.QueryAsync<User>($"SELECT * FROM users WHERE id = {userId}");

// Scalar queries
var count = await client.QueryScalarAsync<long>($"SELECT count(*) FROM users");

// Export to S3 (Athena UNLOAD)
var result = await client.UnloadAsync($"SELECT * FROM orders", "exports/orders");
```

### S3Client

MinIO/S3 file operations:

```csharp
var s3 = new MinioS3Client(new Uri("http://localhost:9000"), "minioadmin", "minioadmin", "warehouse");

await s3.UploadFileAsync("local.txt", "remote/file.txt");
await s3.DownloadFileAsync("remote/file.txt", "downloaded.txt");
var files = await s3.ListFilesAsync("remote/");
```

## Integration Tests

Tests use Testcontainers—no external dependencies required:

```bash
# All tests
dotnet test TrinoIcebergTests.slnx

# Specific test
dotnet test --filter "FullyQualifiedName~CanCreateAndQueryIcebergTable"

# Verbose output
dotnet test --logger "console;verbosity=detailed"
```

Test stack auto-configures Trino with embedded settings (no file mounts), creates the MinIO bucket, and handles cleanup.

## Project Structure

```
├── src/
│   ├── AthenaTrinoClient/   # Type-safe Trino client
│   └── S3Client/            # MinIO/S3 operations
├── tests/IntegrationTests/  # Testcontainers tests
├── examples/                # Console app examples
├── lib/trino-csharp-client/ # Official Trino client (submodule)
└── docker-compose.yml       # Local stack
```

## Notes

- Nessie uses in-memory storage (state lost on restart)
- Reset everything: `docker compose down -v`
- Windows users: run `validate.sh` in Git Bash or WSL
