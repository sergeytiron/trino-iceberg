using AthenaTrinoClient.Mapping;
using Xunit;

namespace UnitTests;

/// <summary>
/// Unit tests for QueryResultMapper.
/// Most mapping functionality is tested through integration tests.
/// These tests verify basic instantiation.
/// </summary>
public class QueryResultMapperTests
{
    [Fact]
    public void Constructor_CreatesInstance()
    {
        var mapper = new QueryResultMapper();
        Assert.NotNull(mapper);
    }
}
