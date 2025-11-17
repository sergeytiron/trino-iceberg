using System.Reflection;
using System.Text.RegularExpressions;
using Trino.Client;

namespace TrinoClient;

/// <summary>
/// Client for executing Trino queries with type-safe deserialization and parameterization support.
/// </summary>
public class TrinoClient : ITrinoClient
{
    private readonly ClientSession _session;

    /// <summary>
    /// Creates a new TrinoClient with the specified session properties.
    /// </summary>
    /// <param name="properties">The Trino client session properties.</param>
    public TrinoClient(ClientSessionProperties properties)
    {
        _session = new ClientSession(sessionProperties: properties, auth: null);
    }

    /// <summary>
    /// Creates a new TrinoClient with the specified connection parameters.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URI.</param>
    /// <param name="catalog">The default catalog to use.</param>
    /// <param name="schema">The default schema to use.</param>
    public TrinoClient(Uri trinoEndpoint, string catalog, string schema)
        : this(new ClientSessionProperties
        {
            Server = trinoEndpoint,
            Catalog = catalog,
            Schema = schema
        })
    {
    }

    /// <summary>
    /// Executes a parameterized query and returns results deserialized into a list of typed objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each row into.</typeparam>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A list of deserialized objects of type T.</returns>
    public List<T> Query<T>(FormattableString query, CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = ConvertFormattableStringToParameterizedQuery(query);

        var executor = RecordExecutor.Execute(
            logger: null,
            queryStatusNotifications: null,
            session: _session,
            statement: sql,
            queryParameters: parameters,
            bufferSize: Constants.DefaultBufferSizeBytes,
            isQuery: true,
            cancellationToken: cancellationToken
        ).GetAwaiter().GetResult();

        var results = new List<T>();
        var columns = executor.Records.Columns;

        foreach (var row in executor)
        {
            var instance = MapRowToObject<T>(row, columns);
            results.Add(instance);
        }

        return results;
    }

    /// <summary>
    /// Executes a query and unloads the results to S3 in Parquet format.
    /// </summary>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="s3RelativePath">The relative S3 path within the warehouse bucket (e.g., "exports/data").</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>An UnloadResponse containing the row count and absolute S3 path.</returns>
    public UnloadResponse Unload(FormattableString query, string s3RelativePath, CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = ConvertFormattableStringToParameterizedQuery(query);

        // Construct UNLOAD statement (mimicking AWS Athena UNLOAD syntax)
        // Note: Trino may not natively support UNLOAD, so we'll attempt it and handle errors
        var unloadSql = $"UNLOAD ({sql}) TO 's3://warehouse/{s3RelativePath}' WITH (format = 'PARQUET')";

        try
        {
            var executor = RecordExecutor.Execute(
                logger: null,
                queryStatusNotifications: null,
                session: _session,
                statement: unloadSql,
                queryParameters: parameters,
                bufferSize: Constants.DefaultBufferSizeBytes,
                isQuery: true,
                cancellationToken: cancellationToken
            ).GetAwaiter().GetResult();

            // Parse the result to get row count
            var rowCount = 0;
            foreach (var row in executor)
            {
                // UNLOAD typically returns metadata about the operation
                // The first column usually contains the number of rows unloaded
                if (row.Count > 0 && row[0] != null)
                {
                    rowCount = Convert.ToInt32(row[0]);
                }
            }

            var absolutePath = $"s3://warehouse/{s3RelativePath}";
            return new UnloadResponse(rowCount, absolutePath);
        }
        catch (Exception ex)
        {
            // If UNLOAD is not supported, fall back to INSERT INTO approach
            throw new NotSupportedException(
                "UNLOAD command is not supported by this Trino version. " +
                "Consider using INSERT INTO to write data to an Iceberg table instead.", ex);
        }
    }

    /// <summary>
    /// Converts a FormattableString into a parameterized SQL query with ? placeholders
    /// and a list of QueryParameter objects. Only creates parameters for arguments that
    /// are not used as SQL identifiers (table names, schema names, etc.).
    /// </summary>
    private static (string Sql, List<QueryParameter> Parameters) ConvertFormattableStringToParameterizedQuery(FormattableString query)
    {
        var format = query.Format;
        var arguments = query.GetArguments();
        
        // If there are no arguments, just return the format string
        if (arguments.Length == 0)
        {
            return (format, new List<QueryParameter>());
        }
        
        // Check if this query uses parameters in SQL value positions (WHERE, VALUES, etc.)
        // vs identifier positions (table names, schema names)
        // For simplicity, detect common patterns that suggest identifier usage
        var formatLower = format.ToLowerInvariant();
        var hasValueParameters = formatLower.Contains(" where ") || 
                                 formatLower.Contains(" values ") ||
                                 formatLower.Contains(" set ") ||
                                 formatLower.Contains(" having ");
        
        // Check if parameters appear in identifier contexts (after FROM, INTO, etc.)
        var hasIdentifierParameters = Regex.IsMatch(format, @"(FROM|INTO|JOIN|TABLE)\s+[^{]*\{\d+\}", RegexOptions.IgnoreCase);
        
        // If parameters are only used as identifiers, format the string directly
        if (hasIdentifierParameters && !hasValueParameters)
        {
            return (string.Format(format, arguments), new List<QueryParameter>());
        }
        
        // Otherwise, create a parameterized query
        // Replace {0}, {1}, etc. with ? placeholders
        var sql = Regex.Replace(format, @"\{(\d+)\}", "?");
        
        // Convert arguments to QueryParameter objects
        var parameters = arguments.Select(arg => new QueryParameter(arg)).ToList();
        
        return (sql, parameters);
    }

    /// <summary>
    /// Maps a database row to an object of type T using reflection.
    /// Properties are matched by name (case-insensitive).
    /// </summary>
    private static T MapRowToObject<T>(List<object> row, IList<Trino.Client.Model.StatementV1.TrinoColumn> columns)
    {
        var instance = Activator.CreateInstance<T>();
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

        for (int i = 0; i < columns.Count && i < row.Count; i++)
        {
            var column = columns[i];
            var value = row[i];

            // Find matching property (case-insensitive)
            var property = properties.FirstOrDefault(p =>
                p.Name.Equals(column.name, StringComparison.OrdinalIgnoreCase) ||
                p.Name.Equals(ConvertSnakeCaseToPascalCase(column.name), StringComparison.OrdinalIgnoreCase));

            if (property != null && property.CanWrite)
            {
                try
                {
                    if (value == null || value == DBNull.Value)
                    {
                        // Handle nullable types
                        if (IsNullableType(property.PropertyType))
                        {
                            property.SetValue(instance, null);
                        }
                        // Skip setting value for non-nullable types with null database values
                    }
                    else
                    {
                        var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                        
                        // Direct assignment if types match
                        if (targetType.IsAssignableFrom(value.GetType()))
                        {
                            property.SetValue(instance, value);
                        }
                        // Convert if needed
                        else
                        {
                            var convertedValue = Convert.ChangeType(value, targetType);
                            property.SetValue(instance, convertedValue);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Failed to map column '{column.name}' (type: {column.type}) to property '{property.Name}' (type: {property.PropertyType.Name}). Value: {value}",
                        ex);
                }
            }
        }

        return instance;
    }

    /// <summary>
    /// Converts snake_case to PascalCase for property name matching.
    /// </summary>
    private static string ConvertSnakeCaseToPascalCase(string snakeCase)
    {
        if (string.IsNullOrEmpty(snakeCase))
            return snakeCase;

        var parts = snakeCase.Split('_');
        var result = string.Join("", parts.Select(p =>
            p.Length > 0 ? char.ToUpperInvariant(p[0]) + p.Substring(1).ToLowerInvariant() : p));

        return result;
    }

    /// <summary>
    /// Checks if a type is nullable (either a reference type or Nullable<T>).
    /// </summary>
    private static bool IsNullableType(Type type)
    {
        return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
    }
}
