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
    public string CommonSchemaName => "common_test_data";

    public async ValueTask InitializeAsync()
    {
        Stack = new TrinoIcebergStack();
        await Stack.StartAsync(TestContext.Current.CancellationToken);

        Stack.ExecuteSqlFast($"CREATE SCHEMA IF NOT EXISTS {CommonSchemaName}");

        // Consolidated tables (reduced from 9 to 4 tables + 1 per-test table)
        // shared_data: combines test_data, people, users, contacts, messages into one multi-purpose table
        // category_data: for aggregation tests (formerly sales)
        // employee_data: for snake_case column mapping + date type (formerly employees)
        // numeric_data: for numeric extremes + decimal (formerly measurements)
        // events_time_travel: kept separate as tests INSERT into it (not pre-populated)
        Stack.ExecuteSqlBatchFast([
            @"CREATE TABLE IF NOT EXISTS shared_data (
                id int,
                value varchar,
                name varchar,
                age int,
                active boolean,
                username varchar,
                email varchar,
                phone varchar,
                content varchar,
                value_int bigint,
                value_double double,
                value_decimal decimal(10,2)
            )",
            "CREATE TABLE IF NOT EXISTS category_data (amount bigint, category varchar)",
            "CREATE TABLE IF NOT EXISTS employee_data (employee_id int, first_name varchar, last_name varchar, hire_date date)",
            "CREATE TABLE IF NOT EXISTS events_time_travel (event_id bigint, event_type varchar, event_time timestamp)"
        ], CommonSchemaName);

        Stack.ExecuteSqlBatchFast([
            // shared_data combines multiple use cases:
            // - Rows 1-2: test_data pattern (id, value)
            // - Rows 1-3: people pattern (id, name, age, active)
            // - Rows 1-3: users pattern (id, username)
            // - Rows 1-3: contacts pattern (id, name, email, phone) with NULLs
            // - Rows 1-3: messages pattern (id, content) with escaping
            // - Row 4: measurements pattern (numeric extremes)
            @"INSERT INTO shared_data VALUES
                (1, 'test', 'Alice', 30, true, 'alice', 'alice@example.com', '555-0001', 'Hello World', NULL, NULL, NULL),
                (2, 'data', 'Bob', 25, false, 'bob', NULL, '555-0002', 'It''s a test', NULL, NULL, NULL),
                (3, NULL, 'Charlie', 35, true, 'charlie', 'charlie@example.com', NULL, 'Quote: ""test""', NULL, NULL, NULL),
                (100, 'test', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 9223372036854775807, 3.14159, 99.99),
                (200, 'data', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            "INSERT INTO category_data VALUES (100, 'A'), (200, 'B'), (150, 'A'), (300, 'B')",
            "INSERT INTO employee_data VALUES (1, 'John', 'Doe', DATE '2020-01-15'), (2, 'Jane', 'Smith', DATE '2019-03-22')"
        ], CommonSchemaName);

    }

    public async ValueTask DisposeAsync()
    {
        if (Stack != null)
        {
            await Stack.DisposeAsync();
        }
    }
}
