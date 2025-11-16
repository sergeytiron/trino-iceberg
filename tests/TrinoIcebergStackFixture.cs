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
        // xUnit's IAsyncLifetime doesn't provide a cancellation token, but we use default
        Stack = new TrinoIcebergStack();
        await Stack.StartAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await Stack.DisposeAsync();
    }
}
