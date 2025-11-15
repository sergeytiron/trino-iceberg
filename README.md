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

## File layout
- `docker-compose.yml`: services definitions
- `trino/etc/*`: Trino server config
- `trino/etc/catalog/iceberg.properties`: Iceberg catalog using Nessie + S3 (MinIO)
- `tests/`: C# Testcontainers implementation with integration tests
- `TrinoIcebergTests.slnx`: .NET solution file for the test project
