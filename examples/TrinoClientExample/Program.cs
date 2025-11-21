using Trino.Client;

// Example usage of the official Trino C# Client
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
    // Create a client session using the official Trino.Client library
    var sessionProperties = new ClientSessionProperties
    {
        Server = new Uri(trinoEndpoint),
        Catalog = catalog,
        Schema = schema
    };

    var session = new ClientSession(sessionProperties: sessionProperties, auth: null);

    // Example 1: Create a schema
    Console.WriteLine("Example 1: Creating schema...");
    var schemaName = $"demo_{Guid.NewGuid():N}".ToLowerInvariant();
    var createSchemaExec = await RecordExecutor.Execute(session, $"CREATE SCHEMA IF NOT EXISTS {catalog}.{schemaName} WITH (location='s3://warehouse/{schemaName}/')");
    // Consume the results to ensure query completes
    foreach (var _ in createSchemaExec) { }
    Console.WriteLine($"✓ Schema '{schemaName}' created\n");

    // Example 2: Create a table
    Console.WriteLine("Example 2: Creating table...");
    var createTableExec = await RecordExecutor.Execute(session, $"CREATE TABLE IF NOT EXISTS {catalog}.{schemaName}.numbers (id int, name varchar) WITH (format='PARQUET')");
    foreach (var _ in createTableExec) { }
    Console.WriteLine("✓ Table 'numbers' created\n");

    // Example 3: Insert data
    Console.WriteLine("Example 3: Inserting data...");
    var insertExec = await RecordExecutor.Execute(session, $"INSERT INTO {catalog}.{schemaName}.numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    foreach (var _ in insertExec) { }
    Console.WriteLine("✓ Data inserted\n");

    // Example 4: Query data
    Console.WriteLine("Example 4: Querying data...");
    var executor = await RecordExecutor.Execute(session, $"SELECT * FROM {catalog}.{schemaName}.numbers ORDER BY id");

    var results = new List<List<object>>();
    foreach (var row in executor)
    {
        results.Add(row);
    }

    Console.WriteLine($"Found {results.Count} rows:");
    foreach (var row in results)
    {
        Console.WriteLine($"  ID: {row[0]}, Name: {row[1]}");
    }
    Console.WriteLine();

    // Example 5: Aggregate query
    Console.WriteLine("Example 5: Running aggregate query...");
    var countExecutor = await RecordExecutor.Execute(session, $"SELECT COUNT(*) as total FROM {catalog}.{schemaName}.numbers");
    var countResults = new List<List<object>>();
    foreach (var row in countExecutor)
    {
        countResults.Add(row);
    }

    if (countResults.Count > 0 && countResults[0].Count > 0)
    {
        Console.WriteLine($"Total rows: {countResults[0][0]}");
    }
    Console.WriteLine();

    Console.WriteLine("✓ All examples completed successfully!");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ Error: {ex.Message}");
    return 1;
}

return 0;
