using System.Diagnostics;

namespace IntegrationTests;

/// <summary>
/// A temporary test to measure timing of different stack operations
/// </summary>
public class TimingTest
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _fixture;

    public TimingTest(ITestOutputHelper output, TrinoIcebergStackFixture fixture)
    {
        _output = output;
        _fixture = fixture;
    }

    [Fact]
    public async Task MeasureQueryTiming()
    {
        var stopwatch = Stopwatch.StartNew();

        // Measure a simple query with no data
        var sw1 = Stopwatch.StartNew();
        _fixture.Stack.ExecuteNonQuery("SELECT 1");
        sw1.Stop();
        _output.WriteLine($"SELECT 1: {sw1.ElapsedMilliseconds}ms");

        // Measure a query reading from existing table
        var sw2 = Stopwatch.StartNew();
        _fixture.Stack.ExecuteNonQuery("SELECT COUNT(*) FROM common_test_data.shared_data", "common_test_data");
        sw2.Stop();
        _output.WriteLine($"COUNT(*) from shared_data: {sw2.ElapsedMilliseconds}ms");

        // Measure a simple CREATE TABLE
        var uniqueTable = $"timing_test_{Guid.NewGuid():N}";
        var sw3 = Stopwatch.StartNew();
        _fixture.Stack.ExecuteNonQuery($"CREATE TABLE iceberg.common_test_data.{uniqueTable} (id int)");
        sw3.Stop();
        _output.WriteLine($"CREATE TABLE: {sw3.ElapsedMilliseconds}ms");

        // Measure INSERT
        var sw4 = Stopwatch.StartNew();
        _fixture.Stack.ExecuteNonQuery($"INSERT INTO {uniqueTable} VALUES (1), (2), (3)", "common_test_data");
        sw4.Stop();
        _output.WriteLine($"INSERT 3 rows: {sw4.ElapsedMilliseconds}ms");

        // Measure SELECT on new table
        var sw5 = Stopwatch.StartNew();
        _fixture.Stack.ExecuteNonQuery($"SELECT * FROM {uniqueTable}", "common_test_data");
        sw5.Stop();
        _output.WriteLine($"SELECT * from new table: {sw5.ElapsedMilliseconds}ms");

        // Clean up
        _fixture.Stack.ExecuteNonQuery($"DROP TABLE iceberg.common_test_data.{uniqueTable}");

        stopwatch.Stop();
        _output.WriteLine($"Total test time: {stopwatch.ElapsedMilliseconds}ms");

        await Task.CompletedTask;
    }
}
