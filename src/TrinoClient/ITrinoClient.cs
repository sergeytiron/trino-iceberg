namespace TrinoClient;

public interface ITrinoClient
{
    List<T> Query<T>(FormattableString query, CancellationToken cancellationToken = default);
    UnloadResponse Unload(FormattableString query, string s3RelativePath, CancellationToken cancellationToken = default);
}
