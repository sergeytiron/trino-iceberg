using System.Globalization;

namespace AthenaTrinoClient.Formatting;

/// <summary>
/// Converts formattable strings into SQL queries with properly escaped and formatted parameter values.
/// </summary>
public class SqlParameterFormatter
{
    /// <summary>
    /// Converts a FormattableString into a SQL query with all parameters inlined as literals.
    /// </summary>
    public string ConvertFormattableStringToParameterizedQuery(FormattableString query)
    {
        if (query.ArgumentCount == 0)
        {
            return query.Format;
        }

        var arguments = query.GetArguments();
        var formatted = new object[arguments.Length];
        
        for (int i = 0; i < arguments.Length; i++)
        {
            formatted[i] = FormatSqlValue(arguments[i]);
        }

        return string.Format(query.Format, formatted);
    }

    /// <summary>
    /// Formats a value as a SQL literal.
    /// </summary>
    private static string FormatSqlValue(object? value)
    {
        return value switch
        {
            null => "NULL",
            DateTime dt => $"TIMESTAMP '{dt:yyyy-MM-dd HH:mm:ss.ffffff}'",
            string str => $"'{str.Replace("'", "''")}'",
            bool b => b ? "true" : "false",
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            Guid g => $"'{g}'",
            _ => value.ToString() ?? "NULL"
        };
    }
}
