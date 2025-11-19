using System.Collections.Generic;
using Trino.Client;

namespace AthenaTrinoClient;

/// <summary>
/// Interface for mapping query results to typed objects.
/// </summary>
public interface IQueryResultMapper
{
    /// <summary>
    /// Deserializes query results into a list of typed objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each row into.</typeparam>
    /// <param name="rows">The rows to deserialize.</param>
    /// <param name="columns">The column metadata from the query results.</param>
    /// <returns>A list of deserialized objects of type T.</returns>
    List<T> DeserializeResults<T>(
        IEnumerable<List<object>> rows,
        IList<Trino.Client.Model.StatementV1.TrinoColumn> columns
    );
}
