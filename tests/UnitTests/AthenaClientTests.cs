using AthenaTrinoClient;
using Xunit;

namespace UnitTests;

/// <summary>
/// Unit tests for AthenaClient. 
/// These tests verify the client can be instantiated correctly.
/// Most functionality is covered by integration tests.
/// </summary>
public class AthenaClientTests
{
    [Fact]
    public void Constructor_CreatesInstance()
    {
        var client = new AthenaClient(
            new Uri("http://localhost:8080"),
            "test_catalog",
            "test_schema"
        );

        Assert.NotNull(client);
    }
}
