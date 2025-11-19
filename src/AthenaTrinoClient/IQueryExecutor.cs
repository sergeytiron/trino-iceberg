using System.Threading;
using System.Threading.Tasks;
using Trino.Client;

namespace AthenaTrinoClient;

/// <summary>
/// Interface for executing Trino queries.
/// </summary>
public interface IQueryExecutor
{
    /// <summary>
    /// Executes a SQL statement and returns the RecordExecutor for processing results.
    /// </summary>
    /// <param name="session">The client session.</param>
    /// <param name="sql">The SQL statement to execute.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A QueryResult containing the rows and columns.</returns>
    Task<QueryResult> Execute(
        ClientSession session,
        string sql,
        CancellationToken cancellationToken = default
    );
}
