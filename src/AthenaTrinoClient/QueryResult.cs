using System.Collections.Generic;
using Trino.Client.Model.StatementV1;

namespace AthenaTrinoClient;

/// <summary>
/// Represents the result of a query execution.
/// </summary>
public class QueryResult
{
    public IEnumerable<List<object>> Rows { get; }
    public IList<TrinoColumn> Columns { get; }

    public QueryResult(IEnumerable<List<object>> rows, IList<TrinoColumn> columns)
    {
        Rows = rows;
        Columns = columns;
    }
}
