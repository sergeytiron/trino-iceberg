# TrinoClient

A lightweight .NET client library for executing queries against Trino (formerly PrestoSQL). This is a simplified wrapper around the official [Trino C# Client](https://github.com/trinodb/trino-csharp-client).

## Features

- ✅ Execute SQL queries against any Trino server
- ✅ Built on the official Trino C# Client library
- ✅ No dependencies on Testcontainers or test infrastructure
- ✅ Simple, intuitive API
- ✅ Async/await support
- ✅ Automatic query polling and result retrieval
- ✅ Support for structured data results
- ✅ Configurable catalog and schema
- ✅ Proper error handling with custom exceptions

## Installation

Add the project reference to your application:

```bash
dotnet add reference path/to/TrinoClient.csproj
```

## Quick Start

```csharp
using TrinoClient;

// Create a client instance
using var client = new TrinoQueryClient(
    trinoEndpoint: "http://localhost:8080",
    catalog: "iceberg",
    schema: "default"
);

// Execute a query and get results
var results = await client.ExecuteQueryAsync("SELECT * FROM my_table");

// Process results (each row is a list of column values)
foreach (var row in results)
{
    Console.WriteLine($"Column 1: {row[0]}, Column 2: {row[1]}");
}
```

## Usage Examples

### Create Schema and Table

```csharp
using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

// Create schema
await client.ExecuteQueryAsync(
    "CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/')");

// Create table
await client.ExecuteQueryAsync(
    "CREATE TABLE iceberg.demo.users (id int, name varchar) WITH (format='PARQUET')");
```

### Insert and Query Data

```csharp
// Insert data
await client.ExecuteQueryAsync(
    "INSERT INTO iceberg.demo.users VALUES (1, 'Alice'), (2, 'Bob')");

// Query data
var results = await client.ExecuteQueryAsync(
    "SELECT * FROM iceberg.demo.users ORDER BY id");

foreach (var row in results)
{
    var id = row[0];
    var name = row[1];
    Console.WriteLine($"User {id}: {name}");
}
```

### Aggregate Queries

```csharp
var countResults = await client.ExecuteQueryAsync(
    "SELECT COUNT(*) as total FROM iceberg.demo.users");

if (countResults.Count > 0 && countResults[0].Count > 0)
{
    Console.WriteLine($"Total users: {countResults[0][0]}");
}
```

### Using Environment Variables

```csharp
var trinoEndpoint = Environment.GetEnvironmentVariable("TRINO_ENDPOINT") ?? "http://localhost:8080";
var catalog = Environment.GetEnvironmentVariable("TRINO_CATALOG") ?? "iceberg";
var schema = Environment.GetEnvironmentVariable("TRINO_SCHEMA") ?? "default";

using var client = new TrinoQueryClient(trinoEndpoint, catalog, schema);
```



## API Reference

### TrinoQueryClient Constructor

```csharp
public TrinoQueryClient(
    string trinoEndpoint,       // e.g., "http://localhost:8080"
    string catalog = "iceberg",  // Default catalog
    string schema = "default"    // Default schema
)
```

### ExecuteQueryAsync

Executes a SQL query and returns results as structured data.

```csharp
public async Task<List<List<object?>>> ExecuteQueryAsync(
    string sql,
    CancellationToken cancellationToken = default
)
```

**Returns**: A list of rows, where each row is a list of column values.

**Throws**:
- `ArgumentException` - When SQL is null or empty
- `TrinoQueryException` - When the query fails

## Error Handling

```csharp
try
{
    var results = await client.ExecuteQueryAsync("SELECT * FROM invalid_table");
}
catch (TrinoQueryException ex)
{
    Console.WriteLine($"Query failed: {ex.Message}");
}
catch (Exception ex)
{
    Console.WriteLine($"Unexpected error: {ex.Message}");
}
```

## Under the Hood

This library is a simplified wrapper around the official [Trino C# Client](https://github.com/trinodb/trino-csharp-client), which provides a comprehensive .NET client for Trino with ADO.NET interfaces, authentication support, and advanced features.

The wrapper provides a simple interface for basic query operations while leveraging the battle-tested official client for all communication with Trino servers.

## Testing

See the `examples/TrinoClientExample` project for a complete working example.

To run the example against a local Trino instance:

```bash
# Start the stack (requires docker-compose)
docker compose up -d

# Wait for Trino to be ready (check logs)
docker logs trino

# Run the example
cd examples/TrinoClientExample
dotnet run
```

## Requirements

- .NET 10.0 or later
- Access to a Trino server

## License

This project follows the same license as the parent repository.

## Contributing

Contributions are welcome! Please ensure your changes:
- Follow existing code style
- Include appropriate error handling
- Work with the existing test infrastructure
