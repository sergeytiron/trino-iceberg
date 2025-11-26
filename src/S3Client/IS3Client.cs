using S3Client.Models;

namespace S3Client;

/// <summary>
/// Interface for S3-compatible storage operations.
/// </summary>
public interface IS3Client : IDisposable
{
    /// <summary>
    /// Uploads a local file to S3.
    /// </summary>
    /// <param name="localFilePath">The path to the local file to upload.</param>
    /// <param name="key">The S3 object key (path) where the file will be stored.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>A task representing the asynchronous upload operation.</returns>
    Task UploadFileAsync(string localFilePath, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Downloads an object from S3 to a local file.
    /// </summary>
    /// <param name="key">The S3 object key (path) to download.</param>
    /// <param name="localFilePath">The local file path where the object will be saved.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>A task representing the asynchronous download operation.</returns>
    Task DownloadFileAsync(string key, string localFilePath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists objects in S3 at the specified prefix (path).
    /// </summary>
    /// <param name="prefix">The prefix (folder path) to list objects from. Use null or empty to list all objects.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>A list of S3 object metadata.</returns>
    Task<List<S3ObjectInfo>> ListFilesAsync(string? prefix = null, CancellationToken cancellationToken = default);
}
