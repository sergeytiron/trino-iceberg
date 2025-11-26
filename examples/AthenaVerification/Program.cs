using AthenaTrinoClient;

Console.WriteLine("🧪 AthenaClient Verification");
Console.WriteLine("=============================\n");

var trinoEndpoint = Environment.GetEnvironmentVariable("TRINO_ENDPOINT") ?? "http://localhost:8080";
var catalog = Environment.GetEnvironmentVariable("TRINO_CATALOG") ?? "iceberg";
var schema = Environment.GetEnvironmentVariable("TRINO_SCHEMA") ?? "demo";

Console.WriteLine($"🔗 Connecting to Trino at: {trinoEndpoint}");
Console.WriteLine($"📂 Using catalog: {catalog}, schema: {schema}\n");

var client = new AthenaClient(new Uri(trinoEndpoint), catalog, schema);

try
{
    // 1. Verify Decimal Formatting (Invariant Culture)
    Console.WriteLine("🔢 Test 1: Decimal Formatting...");
    decimal testDecimal = 123.456m;
    var decimalResult = await client.QueryAsync<DecimalResult>($"SELECT {testDecimal} as val");

    if (decimalResult.Count == 1 && decimalResult[0].Val == testDecimal)
    {
        Console.WriteLine($"✅ Decimal {testDecimal} round-tripped correctly.\n");
    }
    else
    {
        Console.WriteLine($"❌ Decimal round-trip failed. Expected {testDecimal}, got {decimalResult.FirstOrDefault()?.Val}\n");
    }

    // 2. Verify Object Mapping
    Console.WriteLine("🗺️ Test 2: Object Mapping...");
    var mapResult = await client.QueryAsync<UserDto>($"SELECT 1 as id, 'Test User' as name, true as is_active");

    if (mapResult.Count == 1)
    {
        var user = mapResult[0];
        if (user.Id == 1 && user.Name == "Test User" && user.IsActive)
        {
            Console.WriteLine("✅ Object mapping worked correctly.\n");
        }
        else
        {
            Console.WriteLine($"❌ Object mapping mismatch. Got: Id={user.Id}, Name={user.Name}, IsActive={user.IsActive}\n");
        }
    }
    else
    {
        Console.WriteLine("❌ No results returned for mapping test.\n");
    }

    // 3. Verify Guid Support
    Console.WriteLine("🆔 Test 3: Guid Formatting...");
    Guid testGuid = Guid.NewGuid();
    var guidResult = await client.QueryAsync<StringResult>($"SELECT {testGuid} as val");

    if (guidResult.Count == 1 && guidResult[0].Val == testGuid.ToString())
    {
        Console.WriteLine($"✅ Guid {testGuid} round-tripped correctly.\n");
    }
    else
    {
        Console.WriteLine($"❌ Guid round-trip failed. Expected {testGuid}, got {guidResult.FirstOrDefault()?.Val}\n");
    }

    Console.WriteLine("🎉 All verification tests completed!");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ Exception: {ex.Message}");
    Console.WriteLine(ex.StackTrace);
    return 1;
}

return 0;

// DTOs for verification results
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
    public bool IsActive { get; set; }
}
