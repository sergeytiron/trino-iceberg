using AthenaTrinoClient.Mapping;
using System;
using System.Collections.Generic;
using Trino.Client.Model.StatementV1;
using Xunit;

namespace UnitTests;

public class QueryResultMapperTests
{
    private readonly QueryResultMapper _mapper = new();

    public class TestEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public double Price { get; set; }
        public bool IsActive { get; set; }
        public DateTime? CreatedAt { get; set; }
    }

    [Fact]
    public void DeserializeResults_MapsColumnsToProperties()
    {
        var columns = new List<TrinoColumn>
        {
            new TrinoColumn { name = "id", type = "integer" },
            new TrinoColumn { name = "name", type = "varchar" },
            new TrinoColumn { name = "price", type = "double" },
            new TrinoColumn { name = "is_active", type = "boolean" },
            new TrinoColumn { name = "created_at", type = "timestamp" }
        };

        var records = new List<List<object>>
        {
            new List<object> { 1, "Item 1", 10.5, true, new DateTime(2023, 1, 1) },
            new List<object> { 2, "Item 2", 20.0, false, DBNull.Value }
        };

        var result = _mapper.DeserializeResults<TestEntity>(records, columns);

        Assert.Equal(2, result.Count);

        Assert.Equal(1, result[0].Id);
        Assert.Equal("Item 1", result[0].Name);
        Assert.Equal(10.5, result[0].Price);
        Assert.True(result[0].IsActive);
        Assert.Equal(new DateTime(2023, 1, 1), result[0].CreatedAt);

        Assert.Equal(2, result[1].Id);
        Assert.Equal("Item 2", result[1].Name);
        Assert.Equal(20.0, result[1].Price);
        Assert.False(result[1].IsActive);
        Assert.Null(result[1].CreatedAt);
    }

    [Fact]
    public void DeserializeResults_HandlesSnakeCaseToPascalCase()
    {
        var columns = new List<TrinoColumn>
        {
            new TrinoColumn { name = "is_active", type = "boolean" }
        };

        var records = new List<List<object>>
        {
            new List<object> { true }
        };

        var result = _mapper.DeserializeResults<TestEntity>(records, columns);

        Assert.Single(result);
        Assert.True(result[0].IsActive);
    }

    [Fact]
    public void DeserializeResults_IgnoresUnknownColumns()
    {
        var columns = new List<TrinoColumn>
        {
            new TrinoColumn { name = "unknown_column", type = "varchar" },
            new TrinoColumn { name = "id", type = "integer" }
        };

        var records = new List<List<object>>
        {
            new List<object> { "some value", 1 }
        };

        var result = _mapper.DeserializeResults<TestEntity>(records, columns);

        Assert.Single(result);
        Assert.Equal(1, result[0].Id);
    }

    [Fact]
    public void DeserializeResults_HandlesNullsForNullableTypes()
    {
        var columns = new List<TrinoColumn>
        {
            new TrinoColumn { name = "name", type = "varchar" }
        };

        var records = new List<List<object>>
        {
            new List<object> { null! }, // null
            new List<object> { DBNull.Value }
        };

        var result = _mapper.DeserializeResults<TestEntity>(records, columns);

        Assert.Equal(2, result.Count);
        Assert.Null(result[0].Name);
        Assert.Null(result[1].Name);
    }
}
