using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Trino.Client;

namespace AthenaTrinoClient;

/// <summary>
/// Default implementation of IQueryResultMapper.
/// </summary>
public class QueryResultMapper : IQueryResultMapper
{
    /// <inheritdoc />
    public List<T> DeserializeResults<T>(
        IEnumerable<List<object>> rows,
        IList<Trino.Client.Model.StatementV1.TrinoColumn> columns
    )
    {
        var results = new List<T>();
        var propertyMap = CreateColumnToPropertyMap<T>(columns);

        foreach (var row in rows)
        {
            results.Add(MapRowToObject<T>(row, propertyMap));
        }
        return results;
    }

    /// <summary>
    /// Creates a mapping between column indices and properties of type T.
    /// </summary>
    private static PropertyInfo?[] CreateColumnToPropertyMap<T>(IList<Trino.Client.Model.StatementV1.TrinoColumn> columns)
    {
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var map = new PropertyInfo?[columns.Count];

        for (int i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            // Find matching property (case-insensitive)
            map[i] = properties.FirstOrDefault(p =>
                p.Name.Equals(column.name, StringComparison.OrdinalIgnoreCase)
                || p.Name.Equals(TypeConversionUtilities.ConvertSnakeCaseToPascalCase(column.name), StringComparison.OrdinalIgnoreCase)
            );
        }

        return map;
    }

    /// <summary>
    /// Maps a database row to an object of type T using a pre-calculated property map.
    /// </summary>
    private static T MapRowToObject<T>(List<object> row, PropertyInfo?[] propertyMap)
    {
        var instance = Activator.CreateInstance<T>();

        for (int i = 0; i < propertyMap.Length && i < row.Count; i++)
        {
            var property = propertyMap[i];
            var value = row[i];

            if (property != null && property.CanWrite)
            {
                try
                {
                    if (value == null || value == DBNull.Value)
                    {
                        // Handle nullable types
                        if (TypeConversionUtilities.IsNullableType(property.PropertyType))
                        {
                            property.SetValue(instance, null);
                        }
                        // Skip setting value for non-nullable types with null database values
                    }
                    else
                    {
                        var convertedValue = TypeConversionUtilities.ConvertValue(value, property.PropertyType);
                        property.SetValue(instance, convertedValue);
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Failed to map column at index {i} to property '{property.Name}' (type: {property.PropertyType.Name}). Value: {value}",
                        ex
                    );
                }
            }
        }

        return instance;
    }
}
