using System.Globalization;

namespace AthenaTrinoClient.Formatting;

/// <summary>
/// Converts formattable strings into SQL queries with properly escaped and formatted parameter values.
/// Handles special case for DateTime parameters preceded by TIMESTAMP keyword.
/// </summary>
public class SqlParameterFormatter
{
    /// <summary>
    /// Converts a FormattableString into a SQL query with all parameters inlined as literals.
    /// </summary>
    public string ConvertFormattableStringToParameterizedQuery(FormattableString query)
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
            // Check if this parameter is preceded by TIMESTAMP keyword
            var placeholder = $"{{{i}}}";
            var placeholderIndex = format.IndexOf(placeholder);
            var precedingText = placeholderIndex > 10
                ? format.Substring(placeholderIndex - 10, 10)
                : format.Substring(0, placeholderIndex);
            var followsTimestamp = precedingText.TrimEnd().EndsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase);

            inlinedArguments[i] = FormatSqlValue(arguments[i], followsTimestamp);
        }

        return string.Format(format, inlinedArguments);
    }

    /// <summary>
    /// Formats a value as a SQL literal for inline use in queries.
    /// </summary>
    /// <param name="value">The value to format.</param>
    /// <param name="followsTimestamp">Whether this value immediately follows a TIMESTAMP keyword.</param>
    private static string FormatSqlValue(object? value, bool followsTimestamp)
    {
        return value switch
        {
            null => "NULL",
            DateTime dt when followsTimestamp => $"'{dt:yyyy-MM-dd HH:mm:ss.ffffff}'",
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
