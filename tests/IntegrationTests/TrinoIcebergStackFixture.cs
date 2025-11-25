[assembly: AssemblyFixture(typeof(IntegrationTests.TrinoIcebergStackFixture))]

namespace IntegrationTests;

/// <summary>
/// xUnit 3 fixture that starts the Trino + Nessie + MinIO stack once and shares it across all tests.
/// Uses AssemblyFixture for assembly-level sharing (xUnit 3 pattern).
/// Init scripts are auto-discovered from Scripts/create and Scripts/insert folders.
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
    }

    public async ValueTask DisposeAsync()
    {
        if (Stack != null)
        {
            await Stack.DisposeAsync();
        }
    }
}
