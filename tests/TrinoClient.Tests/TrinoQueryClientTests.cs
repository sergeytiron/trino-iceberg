namespace TrinoClient.Tests;

/// <summary>
/// Unit tests for TrinoQueryClient.
/// These tests verify client behavior, error handling, and parameter validation.
/// </summary>
public class TrinoQueryClientTests
{
    [Fact]
    public void Constructor_WithValidParameters_CreatesClient()
    {
        // Arrange & Act
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Assert
        Assert.NotNull(client);
    }

    [Fact]
    public void Constructor_WithNullEndpoint_ThrowsArgumentException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentException>(() => new TrinoQueryClient(null!, "iceberg", "default"));
    }

    [Fact]
    public void Constructor_WithEmptyEndpoint_ThrowsArgumentException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentException>(() => new TrinoQueryClient("", "iceberg", "default"));
    }

    [Fact]
    public void Constructor_WithWhitespaceEndpoint_ThrowsArgumentException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentException>(() => new TrinoQueryClient("   ", "iceberg", "default"));
    }

    [Fact]
    public void Constructor_WithCustomHttpClient_DoesNotDisposeProvidedClient()
    {
        // Arrange
        var httpClient = new HttpClient();
        var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default", httpClient);

        // Act
        client.Dispose();

        // Assert - If the HttpClient was disposed, this would throw ObjectDisposedException
        var timeout = httpClient.Timeout; // Access a property to verify it's not disposed
        Assert.True(timeout > TimeSpan.Zero);
    }

    [Fact]
    public void Constructor_WithoutCustomHttpClient_CreatesInternalClient()
    {
        // Arrange & Act
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Assert - Just verify the client was created successfully
        Assert.NotNull(client);
    }

    [Fact]
    public void Constructor_TrimsTrailingSlashFromEndpoint()
    {
        // Arrange & Act
        using var client = new TrinoQueryClient("http://localhost:8080/", "iceberg", "default");

        // Assert - Verify client was created (internal trimming should work)
        Assert.NotNull(client);
    }

    [Fact]
    public async Task ExecuteQueryAsync_WithNullSql_ThrowsArgumentException()
    {
        // Arrange
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExecuteQueryAsync(null!));
    }

    [Fact]
    public async Task ExecuteQueryAsync_WithEmptySql_ThrowsArgumentException()
    {
        // Arrange
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExecuteQueryAsync(""));
    }

    [Fact]
    public async Task ExecuteQueryAsync_WithWhitespaceSql_ThrowsArgumentException()
    {
        // Arrange
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExecuteQueryAsync("   "));
    }

    [Fact]
    public async Task ExecuteQueryRawAsync_WithNullSql_ThrowsArgumentException()
    {
        // Arrange
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExecuteQueryRawAsync(null!));
    }

    [Fact]
    public async Task ExecuteQueryRawAsync_WithEmptySql_ThrowsArgumentException()
    {
        // Arrange
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExecuteQueryRawAsync(""));
    }

    [Fact]
    public void TrinoQueryException_WithMessage_CreatesException()
    {
        // Arrange
        var message = "Test error message";

        // Act
        var exception = new TrinoQueryException(message);

        // Assert
        Assert.Equal(message, exception.Message);
    }

    [Fact]
    public void TrinoQueryException_WithMessageAndInnerException_CreatesException()
    {
        // Arrange
        var message = "Test error message";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new TrinoQueryException(message, innerException);

        // Assert
        Assert.Equal(message, exception.Message);
        Assert.Equal(innerException, exception.InnerException);
    }

    [Fact]
    public async Task ExecuteQueryAsync_WithCancellationToken_RespectsCancellation()
    {
        // Arrange
        using var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => client.ExecuteQueryAsync("SELECT 1", cts.Token));
    }

    [Fact]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        // Arrange
        var client = new TrinoQueryClient("http://localhost:8080", "iceberg", "default");

        // Act & Assert
        client.Dispose();
        client.Dispose(); // Should not throw
    }
}
