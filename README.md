# Trino + Iceberg + Nessie + MinIO (Docker Compose)

Minimal local stack: Trino queries Iceberg tables via the Nessie catalog; table data lives in MinIO object storage.

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
- Test stack lives in `tests/TrinoIcebergStack.cs` and mirrors the Compose services (dynamic ports, isolated network).
- Trino config is embedded in code via `tests/TrinoConfigurationProvider.cs` and mapped into `/etc/trino/**`.
- MinIO bucket `warehouse` is created via `mc` executed inside the MinIO container (no extra container).

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

## File layout
- `docker-compose.yml`: services definitions
- `trino/etc/*`: Trino server config
- `trino/etc/catalog/iceberg.properties`: Iceberg catalog using Nessie + S3 (MinIO)
- `lib/trino-csharp-client/`: Official Trino C# Client (git submodule)
- `examples/TrinoClientExample/`: Example console application demonstrating client usage
- `tests/`: C# Testcontainers implementation with integration tests
- `TrinoIcebergTests.slnx`: .NET solution file including client, example, and tests
