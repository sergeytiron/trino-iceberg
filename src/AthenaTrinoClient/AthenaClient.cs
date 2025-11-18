using System.Reflection;
using Trino.Client;

namespace AthenaTrinoClient;

/// <summary>
/// Client for executing Trino queries with type-safe deserialization and parameterization support.
/// </summary>
public class AthenaClient : IAthenaClient
{
    private readonly ClientSession _session;

    /// <summary>
    /// Creates a new TrinoClient with the specified session properties.
    /// </summary>
    /// <param name="properties">The Trino client session properties.</param>
    public AthenaClient(ClientSessionProperties properties)
    {
        _session = new ClientSession(sessionProperties: properties, auth: null);
    }

    /// <summary>
    /// Creates a new TrinoClient with the specified connection parameters.
    /// </summary>
    /// <param name="trinoEndpoint">The Trino server endpoint URI.</param>
    /// <param name="catalog">The default catalog to use.</param>
    /// <param name="schema">The default schema to use.</param>
    public AthenaClient(Uri trinoEndpoint, string catalog, string schema)
        : this(
            new ClientSessionProperties
            {
                Server = trinoEndpoint,
                Catalog = catalog,
                Schema = schema,
            }
        ) { }

    /// <summary>
    /// Executes a SQL statement and returns the RecordExecutor for processing results.
    /// </summary>
    /// <param name="sql">The SQL statement to execute (with all parameters already inlined).</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A RecordExecutor for processing query results.</returns>
    private async Task<RecordExecutor> ExecuteStatement(
        string sql,
        CancellationToken cancellationToken = default
    )
    {
        return await RecordExecutor.Execute(
            logger: null,
            queryStatusNotifications: null,
            session: _session,
            statement: sql,
            queryParameters: null,
            bufferSize: Constants.DefaultBufferSizeBytes,
            isQuery: true,
            cancellationToken: cancellationToken
        );
    }

    /// <summary>
    /// Deserializes query results into a list of typed objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each row into.</typeparam>
    /// <param name="executor">The RecordExecutor containing query results.</param>
    /// <param name="columns">The column metadata from the query results.</param>
    /// <returns>A list of deserialized objects of type T.</returns>
    private List<T> DeserializeResults<T>(
        RecordExecutor executor,
        IList<Trino.Client.Model.StatementV1.TrinoColumn> columns
    )
    {
        var results = new List<T>();
        foreach (var row in executor)
        {
            results.Add(MapRowToObject<T>(row, columns));
        }
        return results;
    }

    /// <summary>
    /// Extracts the row count from a query result (typically from CREATE TABLE AS or similar statements).
    /// </summary>
    /// <param name="executor">The RecordExecutor containing the result.</param>
    /// <returns>The row count, or 0 if no count was found.</returns>
    private int ExtractRowCountFromResult(RecordExecutor executor)
    {
        foreach (var row in executor)
        {
            if (row.Count > 0 && row[0] != null)
            {
                return Convert.ToInt32(row[0]);
            }
        }
        return 0;
    }

    /// <summary>
    /// Executes a parameterized query and returns results deserialized into a list of typed objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each row into.</typeparam>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A list of deserialized objects of type T.</returns>
    public async Task<List<T>> Query<T>(FormattableString query, CancellationToken cancellationToken = default)
    {
        var sql = ConvertFormattableStringToParameterizedQuery(query);
        var executor = await ExecuteStatement(sql, cancellationToken);
        return DeserializeResults<T>(executor, executor.Records.Columns);
    }

    /// <summary>
    /// Executes a query and unloads the results to S3 in Parquet format.
    /// Mimics AWS Athena UNLOAD by creating a temporary Iceberg table and using INSERT INTO.
    /// </summary>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <param name="s3RelativePath">The relative S3 path within the warehouse bucket (e.g., "exports/data").</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>An UnloadResponse containing the row count and absolute S3 path.</returns>
    public async Task<UnloadResponse> Unload(
        FormattableString query,
        string s3RelativePath,
        CancellationToken cancellationToken = default
    )
    {
        var sql = ConvertFormattableStringToParameterizedQuery(query);

        // Generate a unique table name for the export
        var timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
        var guid = Guid.NewGuid().ToString("N").Substring(0, 8);
        var exportTableName = $"unload_temp_{timestamp}_{guid}";
        var absolutePath = $"s3://warehouse/{s3RelativePath}";

        try
        {
            // Step 1: Create a temporary table with the query results location
            var createTableSql =
                $"CREATE TABLE {exportTableName} "
                + $"WITH (location = '{absolutePath}', format = 'PARQUET') "
                + $"AS {sql}";

            var createExecutor = await ExecuteStatement(createTableSql, cancellationToken);
            var rowCount = ExtractRowCountFromResult(createExecutor);

            // Step 2: Drop the temporary table (keeps the data files in S3)
            try
            {
                var dropTableSql = $"DROP TABLE {exportTableName}";
                var dropExecutor = await ExecuteStatement(dropTableSql, cancellationToken);

                // Consume the results
                foreach (var _ in dropExecutor) { }
            }
            catch
            {
                // If drop fails, log but don't fail the operation
                // The table will be cleaned up eventually
            }

            return new UnloadResponse(rowCount, absolutePath);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to unload query results to '{absolutePath}'. "
                    + "Ensure the S3 bucket is accessible and the query is valid.",
                ex
            );
        }
    }

    /// <summary>
    /// Converts a FormattableString into a SQL query with all parameters inlined as literals.
    /// </summary>
    private static string ConvertFormattableStringToParameterizedQuery(FormattableString query)
    {
        var format = query.Format;
        var arguments = query.GetArguments();

        if (arguments.Length == 0)
        {
            return format;
        }

        var inlinedArguments = new object[arguments.Length];
        for (int i = 0; i < arguments.Length; i++)
        {
            inlinedArguments[i] = FormatSqlValue(arguments[i]);
        }

        return string.Format(format, inlinedArguments);
    }

    /// <summary>
    /// Formats a value as a SQL literal for inline use in queries.
    /// </summary>
    private static string FormatSqlValue(object? value)
    {
        return value switch
        {
            null => "NULL",
            DateTime dt => $"TIMESTAMP '{dt:yyyy-MM-dd HH:mm:ss.ffffff}'",
            string str => $"'{str.Replace("'", "''")}'",
            bool b => b ? "true" : "false",
            _ => value.ToString() ?? "NULL"
        };
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
                p.Name.Equals(column.name, StringComparison.OrdinalIgnoreCase)
                || p.Name.Equals(TypeConversionUtilities.ConvertSnakeCaseToPascalCase(column.name), StringComparison.OrdinalIgnoreCase)
            );

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
                        $"Failed to map column '{column.name}' (type: {column.type}) to property '{property.Name}' (type: {property.PropertyType.Name}). Value: {value}",
                        ex
                    );
                }
            }
        }

        return instance;
    }
}
