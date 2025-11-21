using AthenaTrinoClient;
using System.Globalization;

Console.WriteLine("Starting AthenaClient Verification...");

var trinoEndpoint = new Uri("http://localhost:8080");
var catalog = "iceberg";
var schema = "demo";

// Ensure schema exists (using raw client or just assuming validate.sh ran)
// We'll use AthenaClient to query.

var client = new AthenaClient(trinoEndpoint, catalog, schema);

try 
{
    // 1. Verify Decimal Formatting (Invariant Culture)
    Console.WriteLine("\nTesting Decimal Formatting...");
    decimal testDecimal = 123.456m;
    // We expect the query to be formatted with '.' not ',' regardless of system locale
    // We can't easily check the generated SQL string directly without exposing it, 
    // but we can check if the query executes successfully. 
    // If it was formatted with ',' (e.g. 123,456) it might be interpreted as two values or syntax error depending on context.
    
    // Let's try to select it back.
    // This uses the FormattableString overload which calls FormatSqlValue
    
    var result = await client.Query<DecimalResult>($"SELECT {testDecimal} as val");
    
    if (result.Count == 1 && result[0].Val == testDecimal)
    {
        Console.WriteLine($"SUCCESS: Decimal {testDecimal} round-tripped correctly.");
    }
    else
    {
        Console.WriteLine($"FAILURE: Decimal round-trip failed. Expected {testDecimal}, got {result.FirstOrDefault()?.Val}");
    }

    // 2. Verify Object Mapping (Caching)
    Console.WriteLine("\nTesting Object Mapping...");
    // Create a table and insert some data if not exists
    // We'll just query a static select for simplicity to test mapping
    // Note: We must use interpolation $"" to create FormattableString, even if no args
    var mapResult = await client.Query<UserDto>($"SELECT 1 as id, 'Test User' as name, true as is_active");

    if (mapResult.Count == 1)
    {
        var user = mapResult[0];
        if (user.Id == 1 && user.Name == "Test User" && user.IsActive == true)
        {
             Console.WriteLine("SUCCESS: Object mapping worked correctly.");
        }
        else
        {
             Console.WriteLine($"FAILURE: Object mapping mismatch. Got: {user.Id}, {user.Name}, {user.IsActive}");
        }
    }
    else
    {
        Console.WriteLine("FAILURE: No results returned for mapping test.");
    }
    
    // 3. Verify Guid Support
    Console.WriteLine("\nTesting Guid Formatting...");
    Guid testGuid = Guid.NewGuid();
    
    // Trino doesn't have a native GUID type, usually treated as string/varchar. 
    // Our FormatSqlValue wraps it in single quotes.
    
    var guidResult = await client.Query<StringResult>($"SELECT {testGuid} as val");
    
    if (guidResult.Count == 1 && guidResult[0].Val == testGuid.ToString())
    {
        Console.WriteLine($"SUCCESS: Guid {testGuid} round-tripped correctly (as string).");
    }
    else
    {
        Console.WriteLine($"FAILURE: Guid round-trip failed. Expected {testGuid}, got {guidResult.FirstOrDefault()?.Val}");
    }

}
catch (Exception ex)
{
    Console.WriteLine($"\nEXCEPTION: {ex.Message}");
    Console.WriteLine(ex.StackTrace);
}

public class DecimalResult
{
    public decimal Val { get; set; }
}

public class StringResult
{
    public string Val { get; set; } = "";
}

public class UserDto
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public bool IsActive { get; set; } // Maps from is_active
}
