using System.Reflection;
using Trino.Client.Model.StatementV1;

namespace AthenaTrinoClient.Mapping;

/// <summary>
/// Maps Trino query results to strongly-typed objects.
/// </summary>
public class QueryResultMapper
{
    /// <summary>
    /// Deserializes query results into a list of typed objects.
    /// </summary>
    public List<T> DeserializeResults<T>(
        IEnumerable<List<object>> rows,
        IList<TrinoColumn> columns
    )
    {
        var results = new List<T>();
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var propertyMap = new PropertyInfo?[columns.Count];

        // Build column to property mapping
        for (int i = 0; i < columns.Count; i++)
        {
            var columnName = columns[i].name;
            propertyMap[i] = properties.FirstOrDefault(p =>
                p.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase) ||
                p.Name.Equals(ConvertSnakeCaseToPascalCase(columnName), StringComparison.OrdinalIgnoreCase)
            );
        }

        // Map each row to an object
        foreach (var row in rows)
        {
            var instance = Activator.CreateInstance<T>();

            for (int i = 0; i < propertyMap.Length && i < row.Count; i++)
            {
                var property = propertyMap[i];
                if (property == null || !property.CanWrite)
                    continue;

                var value = row[i];
                if (value == null || value == DBNull.Value)
                {
                    if (IsNullable(property.PropertyType))
                    {
                        property.SetValue(instance, null);
                    }
                }
                else
                {
                    try
                    {
                        var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                        var converted = targetType.IsAssignableFrom(value.GetType())
                            ? value
                            : Convert.ChangeType(value, targetType);
                        property.SetValue(instance, converted);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException(
                            $"Failed to map column '{columns[i].name}' to property '{property.Name}'. Value: {value}",
                            ex
                        );
                    }
                }
            }

            results.Add(instance);
        }

        return results;
    }

    private static string ConvertSnakeCaseToPascalCase(string snakeCase)
    {
        if (string.IsNullOrEmpty(snakeCase))
            return snakeCase;

        var parts = snakeCase.Split('_');
        return string.Concat(parts.Select(p =>
            p.Length > 0 ? char.ToUpperInvariant(p[0]) + p[1..].ToLowerInvariant() : p
        ));
    }

    private static bool IsNullable(Type type) =>
        !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
}
