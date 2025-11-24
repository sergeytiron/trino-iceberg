using Trino.Client;

namespace AthenaTrinoClient.Execution;

/// <summary>
/// Default implementation of IQueryExecutor.
/// </summary>
public class QueryExecutor : IQueryExecutor
{
    /// <inheritdoc />
    public async Task<QueryResult> Execute(
        ClientSession session,
        string sql,
        CancellationToken cancellationToken = default
    )
    {
        var executor = await RecordExecutor.Execute(
            logger: null,
            queryStatusNotifications: null,
            session: session,
            statement: sql,
            queryParameters: null,
            bufferSize: Constants.DefaultBufferSizeBytes,
            isQuery: true,
            cancellationToken: cancellationToken
        );

        return new QueryResult(executor, executor.Records.Columns);
    }
}
