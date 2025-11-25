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
        
        // Warmup query using ADO.NET - this primes Trino's JIT and catalog initialization
        // The first query is always slow (~15-20s), subsequent queries are much faster (~1-3s)
        Stack.ExecuteSqlFast("SELECT 1");
        
        // Create shared test data schema and all tables using ADO.NET batch
        // This is much faster than CLI calls since it reuses a single connection
        Stack.ExecuteSqlBatchFast([
            // Create schema
            $"CREATE SCHEMA IF NOT EXISTS iceberg.{CommonSchemaName} WITH (location='s3://warehouse/{CommonSchemaName}/')",
            
            // Create all tables
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.numbers (id int, name varchar) WITH (format='PARQUET')",
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.test_data (id int, value varchar) WITH (format='PARQUET')",
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.test_table (id int, name varchar, value int) WITH (format='PARQUET')",
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.sales (amount bigint, category varchar) WITH (format='PARQUET')",
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.mixed_types (id int, name varchar, amount bigint, active boolean) WITH (format='PARQUET')",
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.null_test (id int, nullable_value varchar) WITH (format='PARQUET')",
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.people (id int, name varchar, age int, active boolean) WITH (format='PARQUET')",
            $"CREATE TABLE IF NOT EXISTS iceberg.{CommonSchemaName}.products (product_id int, price double, quantity bigint) WITH (format='PARQUET')",
            
            // Insert data into all tables
            $"INSERT INTO iceberg.{CommonSchemaName}.numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            $"INSERT INTO iceberg.{CommonSchemaName}.test_data VALUES (100, 'test'), (200, 'data')",
            $"INSERT INTO iceberg.{CommonSchemaName}.test_table VALUES (1, 'first', 100), (2, 'second', 200), (3, 'third', 300)",
            $"INSERT INTO iceberg.{CommonSchemaName}.sales VALUES (100, 'A'), (200, 'B'), (150, 'A'), (300, 'B')",
            $"INSERT INTO iceberg.{CommonSchemaName}.mixed_types VALUES (1, 'Alice', 1000, true), (2, 'Bob', 2000, false)",
            $"INSERT INTO iceberg.{CommonSchemaName}.null_test VALUES (1, null), (2, 'not null')",
            $"INSERT INTO iceberg.{CommonSchemaName}.people VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Charlie', 35, true)",
            $"INSERT INTO iceberg.{CommonSchemaName}.products VALUES (100, 29.99, 50), (200, 49.99, 30), (300, 19.99, 100)"
        ]);
    }

    public async ValueTask DisposeAsync()
    {
        if (Stack != null)
        {
            await Stack.DisposeAsync();
        }
    }
}
