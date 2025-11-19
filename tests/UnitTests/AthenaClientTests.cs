using AthenaTrinoClient;
using AthenaTrinoClient.Execution;
using AthenaTrinoClient.Formatting;
using AthenaTrinoClient.Mapping;
using AthenaTrinoClient.Models;
using Moq;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Trino.Client;
using Trino.Client.Model.StatementV1;
using Xunit;

namespace UnitTests;

public class AthenaClientTests
{
    private readonly Mock<ISqlParameterFormatter> _mockFormatter;
    private readonly Mock<IQueryResultMapper> _mockMapper;
    private readonly Mock<IQueryExecutor> _mockExecutor;
    private readonly AthenaClient _client;

    public AthenaClientTests()
    {
        _mockFormatter = new Mock<ISqlParameterFormatter>();
        _mockMapper = new Mock<IQueryResultMapper>();
        _mockExecutor = new Mock<IQueryExecutor>();
        
        var properties = new ClientSessionProperties
        {
            Server = new Uri("http://localhost:8080"),
            Catalog = "test_catalog",
            Schema = "test_schema",
            User = "test_user"
        };

        _client = new AthenaClient(properties, _mockFormatter.Object, _mockMapper.Object, _mockExecutor.Object);
    }

    public class TestEntity
    {
        public int Id { get; set; }
    }

    [Fact]
    public async Task Query_CallsDependenciesCorrectly()
    {
        var query = (FormattableString)$"SELECT * FROM table";
        var sql = "SELECT * FROM table";
        var columns = new List<TrinoColumn>();
        var records = new List<List<object>>();
        var queryResult = new QueryResult(records, columns);
        
        _mockFormatter.Setup(f => f.ConvertFormattableStringToParameterizedQuery(query)).Returns(sql);
        _mockExecutor.Setup(e => e.Execute(It.IsAny<ClientSession>(), sql, It.IsAny<CancellationToken>()))
            .ReturnsAsync(queryResult);
        _mockMapper.Setup(m => m.DeserializeResults<TestEntity>(It.IsAny<IEnumerable<List<object>>>(), It.IsAny<IList<TrinoColumn>>()))
            .Returns(new List<TestEntity>());

        await _client.Query<TestEntity>(query);

        _mockFormatter.Verify(f => f.ConvertFormattableStringToParameterizedQuery(query), Times.Once);
        _mockExecutor.Verify(e => e.Execute(It.IsAny<ClientSession>(), sql, It.IsAny<CancellationToken>()), Times.Once);
        _mockMapper.Verify(m => m.DeserializeResults<TestEntity>(It.IsAny<IEnumerable<List<object>>>(), It.IsAny<IList<TrinoColumn>>()), Times.Once);
    }

    [Fact]
    public async Task Unload_GeneratesCorrectSqlAndCallsExecute()
    {
        var query = (FormattableString)$"SELECT * FROM table";
        var sql = "SELECT * FROM table";
        var s3Path = "exports/data";
        var absolutePath = $"s3://warehouse/{s3Path}";
        
        _mockFormatter.Setup(f => f.ConvertFormattableStringToParameterizedQuery(query)).Returns(sql);
        
        // Mock execution for CREATE TABLE
        var createResult = new QueryResult(new List<List<object>>(), new List<TrinoColumn>());
        
        _mockExecutor.Setup(e => e.Execute(It.IsAny<ClientSession>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(createResult);

        var response = await _client.Unload(query, s3Path);

        _mockFormatter.Verify(f => f.ConvertFormattableStringToParameterizedQuery(query), Times.Once);
        
        // Verify CREATE TABLE SQL
        _mockExecutor.Verify(e => e.Execute(
            It.IsAny<ClientSession>(), 
            It.Is<string>(s => s.StartsWith("CREATE TABLE unload_temp_") && s.Contains($"AS {sql}")), 
            It.IsAny<CancellationToken>()), 
            Times.Once);

        // Verify DROP TABLE SQL
        _mockExecutor.Verify(e => e.Execute(
            It.IsAny<ClientSession>(), 
            It.Is<string>(s => s.StartsWith("DROP TABLE unload_temp_")), 
            It.IsAny<CancellationToken>()), 
            Times.Once);
            
        Assert.Equal(absolutePath, response.S3AbsolutePath);
    }
}
