[assembly: AssemblyFixture(typeof(IntegrationTests.TrinoIcebergStackFixture))]

namespace IntegrationTests;

/// <summary>
/// xUnit 3 fixture that starts the Trino + Nessie + MinIO stack once and shares it across all tests.
/// Uses AssemblyFixture for assembly-level sharing (xUnit 3 pattern).
/// </summary>
public sealed class TrinoIcebergStackFixture : IAsyncLifetime
{
    public TrinoIcebergStack Stack { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        Stack = new TrinoIcebergStack();
        await Stack.StartAsync(CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        if (Stack != null)
        {
            await Stack.DisposeAsync();
        }
    }
}
