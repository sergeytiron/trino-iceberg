using System;

namespace AthenaTrinoClient.Formatting;

/// <summary>
/// Interface for converting formattable strings into parameterized SQL queries.
/// </summary>
public interface ISqlParameterFormatter
{
    /// <summary>
    /// Converts a FormattableString into a SQL query with all parameters inlined as literals.
    /// </summary>
    /// <param name="query">The parameterized SQL query using FormattableString interpolation.</param>
    /// <returns>The formatted SQL string.</returns>
    string ConvertFormattableStringToParameterizedQuery(FormattableString query);
}
