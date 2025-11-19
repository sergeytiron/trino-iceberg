namespace AthenaTrinoClient.Mapping;

/// <summary>
/// Utility methods for type conversion and string manipulation.
/// </summary>
public static class TypeConversionUtilities
{
    /// <summary>
    /// Converts snake_case to PascalCase for property name matching.
    /// </summary>
    /// <param name="snakeCase">The snake_case string to convert.</param>
    /// <returns>A PascalCase string.</returns>
    public static string ConvertSnakeCaseToPascalCase(string snakeCase)
    {
        if (string.IsNullOrEmpty(snakeCase))
            return snakeCase;

        var parts = snakeCase.Split('_');
        var result = string.Join(
            "",
            parts.Select(p => p.Length > 0 ? char.ToUpperInvariant(p[0]) + p.Substring(1).ToLowerInvariant() : p)
        );

        return result;
    }

    /// <summary>
    /// Checks if a type is nullable (either a reference type or Nullable&lt;T&gt;).
    /// </summary>
    /// <param name="type">The type to check.</param>
    /// <returns>True if the type is nullable, false otherwise.</returns>
    public static bool IsNullableType(Type type)
    {
        return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
    }

    /// <summary>
    /// Converts a database value to the target property type, handling nulls and type conversions.
    /// </summary>
    /// <param name="value">The database value to convert.</param>
    /// <param name="targetType">The target property type.</param>
    /// <returns>The converted value, or null if the value is null and the type is nullable.</returns>
    /// <exception cref="InvalidOperationException">Thrown when a null value is assigned to a non-nullable type.</exception>
    public static object? ConvertValue(object? value, Type targetType)
    {
        if (value == null || value == DBNull.Value)
        {
            if (IsNullableType(targetType))
            {
                return null;
            }
            throw new InvalidOperationException($"Cannot assign null to non-nullable type {targetType.Name}");
        }

        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Direct assignment if types match
        if (underlyingType.IsAssignableFrom(value.GetType()))
        {
            return value;
        }

        // Convert if needed
        return Convert.ChangeType(value, underlyingType);
    }
}
