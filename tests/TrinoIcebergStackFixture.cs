namespace TrinoIcebergTests;

/// <summary>
/// xUnit fixture that starts the Trino + Nessie + MinIO stack once and shares it across all tests
/// in <see cref="TrinoIcebergStackTests"/> (and other test classes if converted to a collection fixture).
/// </summary>
public class TrinoIcebergStackFixture : IAsyncLifetime
{
    public TrinoIcebergStack Stack { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        Stack = new TrinoIcebergStack();
        await Stack.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (Stack != null)
        {
            await Stack.DisposeAsync();
        }
    }
}

