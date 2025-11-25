using AthenaTrinoClient.Formatting;
using System;
using Xunit;

namespace UnitTests;

public class SqlParameterFormatterTests
{
    private readonly SqlParameterFormatter _formatter = new();

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_NoArguments_ReturnsOriginalFormat()
    {
        FormattableString query = $"SELECT * FROM table";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal("SELECT * FROM table", result);
    }

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_StringArgument_FormatsCorrectly()
    {
        var name = "test";
        FormattableString query = $"SELECT * FROM table WHERE name = {name}";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal("SELECT * FROM table WHERE name = 'test'", result);
    }

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_StringWithQuotes_EscapesCorrectly()
    {
        var name = "O'Reilly";
        FormattableString query = $"SELECT * FROM table WHERE name = {name}";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal("SELECT * FROM table WHERE name = 'O''Reilly'", result);
    }

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_NumberArguments_FormatsCorrectly()
    {
        var id = 123;
        var price = 12.34m;
        FormattableString query = $"SELECT * FROM table WHERE id = {id} AND price = {price}";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal("SELECT * FROM table WHERE id = 123 AND price = 12.34", result);
    }

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_DateTime_FormatsAsTimestamp()
    {
        var dt = new DateTime(2023, 1, 1, 12, 0, 0);
        FormattableString query = $"SELECT * FROM table WHERE created_at = {dt}";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal("SELECT * FROM table WHERE created_at = TIMESTAMP '2023-01-01 12:00:00.000000'", result);
    }

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_NullArgument_FormatsAsNull()
    {
        string? name = null;
        FormattableString query = $"SELECT * FROM table WHERE name = {name}";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal("SELECT * FROM table WHERE name = NULL", result);
    }

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_BoolArgument_FormatsCorrectly()
    {
        var isActive = true;
        FormattableString query = $"SELECT * FROM table WHERE is_active = {isActive}";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal("SELECT * FROM table WHERE is_active = true", result);
    }

    [Fact]
    public void ConvertFormattableStringToParameterizedQuery_GuidArgument_FormatsCorrectly()
    {
        var guid = Guid.Parse("d23e4f50-6070-8090-0010-203040506070");
        FormattableString query = $"SELECT * FROM table WHERE id = {guid}";
        var result = _formatter.ConvertFormattableStringToParameterizedQuery(query);
        Assert.Equal($"SELECT * FROM table WHERE id = '{guid}'", result);
    }
}
