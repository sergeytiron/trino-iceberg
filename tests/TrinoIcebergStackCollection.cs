namespace TrinoIcebergTests;

/// <summary>
/// Collection definition for sharing a single TrinoIcebergStackFixture instance across multiple test classes.
/// Any test class annotated with [Collection("TrinoIcebergStack")] will receive the same fixture.
/// </summary>
[CollectionDefinition("TrinoIcebergStack")]
public class TrinoIcebergStackCollection : ICollectionFixture<TrinoIcebergStackFixture>
{
    // No code needed here; it's just the glue for xUnit.
}