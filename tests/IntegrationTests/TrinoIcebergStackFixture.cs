[assembly: AssemblyFixture(typeof(IntegrationTests.TrinoIcebergStackFixture))]

namespace IntegrationTests;

/// <summary>
/// xUnit 3 fixture that starts the Trino + Nessie + MinIO stack once and shares it across all tests.
/// Uses AssemblyFixture for assembly-level sharing (xUnit 3 pattern).
/// Also creates a shared "common" schema with test data that can be reused by multiple tests.
/// </summary>
public sealed class TrinoIcebergStackFixture : IAsyncLifetime
{
    public TrinoIcebergStack Stack { get; private set; } = null!;
    
    /// <summary>
    /// Shared schema name available to all tests. Contains pre-populated test tables.
    /// </summary>
    public string CommonSchemaName { get; } = "common_test_data";

    public async ValueTask InitializeAsync()
    {
        Stack = new TrinoIcebergStack();
        await Stack.StartAsync(CancellationToken.None);
        
        // Warmup query - this primes Trino's JIT and catalog initialization
        // The first query is always slow (~15-20s), subsequent queries are much faster (~1-3s)
        await Stack.ExecuteTrinoQueryAsync("SELECT 1");
        
        // Create shared test data schema and tables in minimal queries
        // Batch DDL and DML to reduce round-trips to Trino CLI
        await Stack.ExecuteTrinoQueryAsync($"CREATE SCHEMA IF NOT EXISTS iceberg.{CommonSchemaName} WITH (location='s3://warehouse/{CommonSchemaName}/')");
        
        // Create all tables in parallel since they don't depend on each other
        await Task.WhenAll(
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.numbers (id int, name varchar) WITH (format='PARQUET')"),
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.test_data (id int, value varchar) WITH (format='PARQUET')"),
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.test_table (id int, name varchar, value int) WITH (format='PARQUET')"),
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.sales (amount bigint, category varchar) WITH (format='PARQUET')"),
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.mixed_types (id int, name varchar, amount bigint, active boolean) WITH (format='PARQUET')"),
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.null_test (id int, nullable_value varchar) WITH (format='PARQUET')"),
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.people (id int, name varchar, age int, active boolean) WITH (format='PARQUET')"),
            Stack.ExecuteTrinoQueryAsync($"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.products (product_id int, price double, quantity bigint) WITH (format='PARQUET')")
        );
        
        // Insert data in parallel since tables are independent
        await Task.WhenAll(
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')"),
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.test_data VALUES (100, 'test'), (200, 'data')"),
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.test_table VALUES (1, 'first', 100), (2, 'second', 200), (3, 'third', 300)"),
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.sales VALUES (100, 'A'), (200, 'B'), (150, 'A'), (300, 'B')"),
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.mixed_types VALUES (1, 'Alice', 1000, true), (2, 'Bob', 2000, false)"),
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.null_test VALUES (1, null), (2, 'not null')"),
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.people VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Charlie', 35, true)"),
            Stack.ExecuteTrinoQueryAsync($"INSERT INTO iceberg.{CommonSchemaName}.products VALUES (100, 29.99, 50), (200, 49.99, 30), (300, 19.99, 100)")
        );
    }

    public async ValueTask DisposeAsync()
    {
        if (Stack != null)
        {
            await Stack.DisposeAsync();
        }
    }
}
