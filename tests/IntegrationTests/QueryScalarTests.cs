using AthenaTrinoClient;

namespace IntegrationTests;

public class QueryScalarTests
{
    private readonly AthenaClient _client;

    public QueryScalarTests(TrinoIcebergStackFixture fixture)
    {
        _client = new AthenaClient(new Uri(fixture.Stack.TrinoEndpoint), "iceberg", "common_test_data");
    }

    [Fact]
    public async Task QueryScalar_WithInt_ReturnsCorrectValue()
    {
        // Act
        var result = await _client.QueryScalar<int>($"SELECT max(int_value) FROM scalar_test", TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(30, result);
    }

    [Fact]
    public async Task QueryScalar_WithNullableInt_ReturnsValue()
    {
        // Act
        var result = await _client.QueryScalar<int?>($"SELECT min(int_value) FROM scalar_test", TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(10, result);
    }

    [Fact]
    public async Task QueryScalar_WithNullableInt_ReturnsNull()
    {
        // Act
        var result = await _client.QueryScalar<int?>($"SELECT max(int_value) FROM scalar_test WHERE id = 999", TestContext.Current.CancellationToken);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task QueryScalar_WithString_ReturnsCorrectValue()
    {
        // Act
        var result = await _client.QueryScalar<string>($"SELECT min(string_value) FROM scalar_test", TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal("apple", result);
    }

    [Fact]
    public async Task QueryScalar_WithBool_ReturnsCorrectValue()
    {
        // Act
        var result = await _client.QueryScalar<bool>($"SELECT bool_value FROM scalar_test WHERE id = 1", TestContext.Current.CancellationToken);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task QueryScalar_WithGuid_ReturnsCorrectValue()
    {
        // Act
        var result = await _client.QueryScalar<Guid>($"SELECT guid_value FROM scalar_test WHERE id = 1", TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(Guid.Parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890"), result);
    }

    [Fact]
    public async Task QueryScalar_WithDateTime_ReturnsCorrectValue()
    {
        // Act
        var result = await _client.QueryScalar<DateTime>($"SELECT datetime_value FROM scalar_test WHERE id = 1", TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(new DateTime(2024, 1, 15, 10, 30, 0), result);
    }

    [Fact]
    public async Task QueryScalar_WithDecimal_ReturnsCorrectValue()
    {
        // Act
        var result = await _client.QueryScalar<decimal>($"SELECT sum(decimal_value) FROM scalar_test", TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(351.00m, result);
    }

    [Fact]
    public async Task QueryScalar_WithParameter_ReturnsFilteredResult()
    {
        // Arrange
        var category = "A";

        // Act
        var result = await _client.QueryScalar<int>(
            $"SELECT sum(int_value) FROM scalar_test WHERE category = {category}",
            TestContext.Current.CancellationToken
        );

        // Assert
        Assert.Equal(30, result);
    }
}
