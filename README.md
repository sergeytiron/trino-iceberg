# Trino + Iceberg + Nessie + MinIO (Docker Compose)

Minimal local stack for **integration testing with AWS Athena**: Trino queries Iceberg tables via the Nessie catalog with table data in MinIO object storage, providing a local environment that mirrors AWS Athena's architecture.

## Purpose

This project provides a local development and testing environment that replicates AWS Athena's infrastructure for integration testing:

- **AWS Athena** uses Trino as its query engine, Iceberg for table format, and S3 for storage
- **This stack** uses the same components locally: Trino + Iceberg + MinIO (S3-compatible storage)
- **AthenaClient** provides an Athena-compatible API for C# applications with type-safe querying and UNLOAD functionality

## Services
- Trino: `trinodb/trino:478` on `localhost:8080`
- Nessie: `ghcr.io/projectnessie/nessie:latest` on `localhost:19120`
- MinIO: `minio/minio:latest` API `:9000`, console `:9001`
- mc-init: one-shot to create `warehouse` bucket in MinIO

## Quick start

```bash
# From this folder
docker compose up -d --build
```

Wait until `nessie` logs show ready (serving on 19120). Nessie runs with in-memory storage for simplicity.

Open UIs:
- Trino UI: http://localhost:8080
- MinIO console: http://localhost:9001 (minioadmin/minioadmin)

## Validate with SQL

Run the automated validation script:

```bash
./validate.sh
```

Note (Windows): run `validate.sh` from Git Bash or WSL.

Or use Trino CLI directly:

```bash
# Launch interactive CLI
docker exec -it trino trino
```

Then run:

```sql
-- Create a schema in the Nessie catalog
CREATE SCHEMA iceberg.demo WITH (location = 's3://warehouse/demo/');

-- Create a small table
CREATE TABLE iceberg.demo.numbers (n int) WITH (format = 'PARQUET');

INSERT INTO iceberg.demo.numbers VALUES (1), (2), (3);

SELECT * FROM iceberg.demo.numbers;
```

Non-interactive example:

```bash
echo "SELECT * FROM iceberg.demo.numbers;" | docker exec -i trino trino
```

## .NET Testcontainers (Integration Tests)

Run tests from the repository root:

```bash
dotnet test TrinoIcebergTests.slnx
```

Or from the tests directory:

```bash
cd tests
dotnet test
```

Targeted/verbose examples:

```bash
dotnet test --filter "FullyQualifiedName~CanCreateAndQueryIcebergTable"
dotnet test --logger "console;verbosity=detailed"
```

Details:
- Test stack lives in `tests/IntegrationTests/TrinoIcebergStack.cs` and mirrors the Compose services (dynamic ports, isolated network).
- Trino config is embedded in code via `tests/IntegrationTests/TrinoConfigurationProvider.cs` and mapped into `/etc/trino/**`.
- MinIO bucket `warehouse` is created via `mc` executed inside the MinIO container (no extra container).
- Query execution uses ADO.NET via the Trino C# Client (faster than CLI exec).

## Notes / Troubleshooting
- Nessie uses in-memory storage by default (data lost on restart). For persistence, switch to `NESSIE_VERSION_STORE_TYPE=JDBC` and add Postgres config.
- MinIO requires path-style addressing; both Nessie and Trino are configured accordingly.
- Warehouse URI is managed by Trino's `iceberg.nessie-catalog.default-warehouse-dir=s3://warehouse/`.
- If you enable TLS on MinIO, change endpoints to `https://` and configure Trino truststore.
- To reset state, remove volumes: `docker compose down -v` (destroys MinIO and Postgres data).

## Trino Client Library (.NET)

This repository includes the official [Trino C# Client](https://github.com/trinodb/trino-csharp-client) as a git submodule for executing queries against Trino.

### Quick Example

```csharp
using Trino.Client;

// Create a client session
var sessionProperties = new ClientSessionProperties
{
    Server = new Uri("http://localhost:8080"),
    Catalog = "iceberg",
    Schema = "default"
};

var session = new ClientSession(sessionProperties: sessionProperties, auth: null);

// Execute a query
var executor = await RecordExecutor.Execute(session, "SELECT * FROM my_table");

foreach (var row in executor)
{
    Console.WriteLine($"Column 1: {row[0]}, Column 2: {row[1]}");
}
```

### Running the Example Application

```bash
# Start the stack
docker compose up -d

# Run the example (uses the official Trino C# Client)
cd examples/TrinoClientExample
dotnet run
```

**Key Features:**
- ✅ Official Trino C# Client from the Trino project
- ✅ No dependencies on Testcontainers or test infrastructure
- ✅ Full-featured client with ADO.NET support
- ✅ Async/await support with streaming
- ✅ Authentication support (Basic, JWT, OAuth, etc.)
- ✅ Session management and query parameters

For detailed documentation, see the [official Trino C# Client documentation](https://github.com/trinodb/trino-csharp-client).

## AthenaClient (AWS Athena Integration Testing)

The `AthenaClient` is a high-level wrapper around the Trino C# Client designed for integration testing with AWS Athena. It provides an Athena-compatible API with additional features:

### Key Features

- **Type-safe querying**: Deserialize query results directly into C# objects
- **FormattableString parameterization**: Use C# string interpolation with automatic SQL escaping
- **AWS Athena UNLOAD support**: Export query results to S3 in Parquet format
- **Time-travel queries**: Query Iceberg table snapshots using `FOR TIMESTAMP AS OF`
- **Snake_case to PascalCase mapping**: Automatic conversion of database column names to C# property names
- **Async/await support**: All methods are fully asynchronous

### Quick Example

```csharp
using AthenaTrinoClient;

// Create an AthenaClient
var client = new AthenaClient(
    trinoEndpoint: new Uri("http://localhost:8080"),
    catalog: "iceberg",
    schema: "demo"
);

// Type-safe query with parameterization
var userId = 123;
var results = await client.Query<User>(
    $"SELECT id, name, email FROM users WHERE id = {userId}"
);

// UNLOAD query results to S3
var response = await client.Unload(
    query: $"SELECT * FROM orders WHERE created_at > {DateTime.UtcNow.AddDays(-7)}",
    s3RelativePath: "exports/recent_orders"
);

Console.WriteLine($"Exported {response.RowCount} rows to {response.S3AbsolutePath}");
```

### Testing Against Production

Since AWS Athena uses Trino as its query engine with Iceberg tables, this local stack provides an accurate testing environment:

1. **Develop locally** against Trino + Iceberg + MinIO
2. **Test integration** with the same query patterns and data formats
3. **Deploy to AWS** where Athena uses the identical Trino + Iceberg stack with S3

The `AthenaClient` API works identically in both environments, just change the endpoint from `localhost:8080` to your AWS Athena endpoint.

### Integration Tests

See `tests/IntegrationTests/AthenaClientTests.cs` for comprehensive examples of:
- Type-safe deserialization
- Parameterized queries
- Time-travel queries
- UNLOAD operations
- Null value handling
- Snake_case column mapping

## File layout
- `docker-compose.yml`: services definitions
- `trino/etc/*`: Trino server config
- `trino/etc/catalog/iceberg.properties`: Iceberg catalog using Nessie + S3 (MinIO)
- `lib/trino-csharp-client/`: Official Trino C# Client (git submodule)
- `src/AthenaTrinoClient/`: AthenaClient library for AWS Athena integration testing
  - `AthenaClient.cs`: Main client implementation with Query and Unload methods
  - `IAthenaClient.cs`: Interface for AthenaClient
  - `TypeConversionUtilities.cs`: Utilities for type conversion and mapping
  - `UnloadResponse.cs`: Response model for UNLOAD operations
- `examples/TrinoClientExample/`: Example console application demonstrating client usage
- `tests/`: C# Testcontainers implementation with integration tests
  - `IntegrationTests/AthenaClientTests.cs`: Comprehensive AthenaClient integration tests
- `TrinoIcebergTests.slnx`: .NET solution file including client, example, and tests
