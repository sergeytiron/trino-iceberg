using TrinoClient;

// Example usage of TrinoQueryClient
// This demonstrates how to connect to a Trino server and execute queries

Console.WriteLine("Trino Client Example");
Console.WriteLine("====================\n");

// Get Trino endpoint from environment variable or use default
var trinoEndpoint = Environment.GetEnvironmentVariable("TRINO_ENDPOINT") ?? "http://localhost:8080";
var catalog = Environment.GetEnvironmentVariable("TRINO_CATALOG") ?? "iceberg";
var schema = Environment.GetEnvironmentVariable("TRINO_SCHEMA") ?? "default";

Console.WriteLine($"Connecting to Trino at: {trinoEndpoint}");
Console.WriteLine($"Using catalog: {catalog}, schema: {schema}\n");

try
{
    using var client = new TrinoQueryClient(trinoEndpoint, catalog, schema);

    // Example 1: Create a schema
    Console.WriteLine("Example 1: Creating schema...");
    var schemaName = $"demo_{Guid.NewGuid():N}".ToLowerInvariant();
    await client.ExecuteQueryAsync($"CREATE SCHEMA IF NOT EXISTS {catalog}.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
    Console.WriteLine($"✓ Schema '{schemaName}' created\n");

    // Example 2: Create a table
    Console.WriteLine("Example 2: Creating table...");
    await client.ExecuteQueryAsync($"CREATE TABLE IF NOT EXISTS {catalog}.{schemaName}.numbers (id int, name varchar) WITH (format='PARQUET')");
    Console.WriteLine("✓ Table 'numbers' created\n");

    // Example 3: Insert data
    Console.WriteLine("Example 3: Inserting data...");
    await client.ExecuteQueryAsync($"INSERT INTO {catalog}.{schemaName}.numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    Console.WriteLine("✓ Data inserted\n");

    // Example 4: Query data
    Console.WriteLine("Example 4: Querying data...");
    var results = await client.ExecuteQueryAsync($"SELECT * FROM {catalog}.{schemaName}.numbers ORDER BY id");
    
    Console.WriteLine($"Found {results.Count} rows:");
    foreach (var row in results)
    {
        Console.WriteLine($"  ID: {row[0]}, Name: {row[1]}");
    }
    Console.WriteLine();

    // Example 5: Aggregate query
    Console.WriteLine("Example 5: Running aggregate query...");
    var countResults = await client.ExecuteQueryAsync($"SELECT COUNT(*) as total FROM {catalog}.{schemaName}.numbers");
    if (countResults.Count > 0 && countResults[0].Count > 0)
    {
        Console.WriteLine($"Total rows: {countResults[0][0]}");
    }
    Console.WriteLine();

    Console.WriteLine("✓ All examples completed successfully!");
}
catch (TrinoQueryException ex)
{
    Console.WriteLine($"❌ Trino query error: {ex.Message}");
    return 1;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ Error: {ex.Message}");
    return 1;
}

return 0;
