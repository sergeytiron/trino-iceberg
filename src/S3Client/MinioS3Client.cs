using Amazon.S3;
using Amazon.S3.Model;
using S3Client.Models;

namespace S3Client;

/// <summary>
/// S3 client implementation configured for MinIO compatibility.
/// </summary>
public sealed class MinioS3Client : IS3Client
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;

    /// <summary>
    /// Creates a new MinIO S3 client.
    /// </summary>
    /// <param name="endpoint">The MinIO endpoint URI (e.g., http://localhost:9000).</param>
    /// <param name="accessKey">The access key (MinIO root user).</param>
    /// <param name="secretKey">The secret key (MinIO root password).</param>
    /// <param name="bucketName">The default bucket name to use for operations.</param>
    public MinioS3Client(Uri endpoint, string accessKey, string secretKey, string bucketName)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(accessKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(secretKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        _bucketName = bucketName;

        var config = new AmazonS3Config
        {
            ServiceURL = endpoint.ToString(),
            ForcePathStyle = true, // Required for MinIO
            UseHttp = endpoint.Scheme == "http",
        };

        _s3Client = new AmazonS3Client(accessKey, secretKey, config);
    }

    /// <summary>
    /// Creates a new MinIO S3 client with an existing IAmazonS3 instance.
    /// </summary>
    /// <param name="s3Client">The S3 client instance to use.</param>
    /// <param name="bucketName">The default bucket name to use for operations.</param>
    public MinioS3Client(IAmazonS3 s3Client, string bucketName)
    {
        ArgumentNullException.ThrowIfNull(s3Client);
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        _s3Client = s3Client;
        _bucketName = bucketName;
    }

    /// <inheritdoc />
    public async Task UploadFileAsync(string localFilePath, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localFilePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (!File.Exists(localFilePath))
        {
            throw new FileNotFoundException("The specified file does not exist.", localFilePath);
        }

        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = key,
            FilePath = localFilePath,
        };

        await _s3Client.PutObjectAsync(request, cancellationToken);
    }

    /// <inheritdoc />
    public async Task DownloadFileAsync(string key, string localFilePath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(localFilePath);

        var request = new GetObjectRequest { BucketName = _bucketName, Key = key };

        using var response = await _s3Client.GetObjectAsync(request, cancellationToken);

        // Ensure the directory exists
        var directory = Path.GetDirectoryName(localFilePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await response.WriteResponseStreamToFileAsync(localFilePath, append: false, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<List<S3ObjectInfo>> ListFilesAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default
    )
    {
        var results = new List<S3ObjectInfo>();

        var request = new ListObjectsV2Request { BucketName = _bucketName, Prefix = prefix };

        ListObjectsV2Response response;
        do
        {
            response = await _s3Client.ListObjectsV2Async(request, cancellationToken);

            foreach (var obj in response.S3Objects)
            {
                results.Add(
                    new S3ObjectInfo(Key: obj.Key, Size: obj.Size, LastModified: obj.LastModified, ETag: obj.ETag)
                );
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated);

        return results;
    }

    /// <inheritdoc />
    public async Task CopyObjectAsync(
        string sourceKey,
        string destinationKey,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationKey);

        var request = new CopyObjectRequest
        {
            SourceBucket = _bucketName,
            SourceKey = sourceKey,
            DestinationBucket = _bucketName,
            DestinationKey = destinationKey,
        };

        await _s3Client.CopyObjectAsync(request, cancellationToken);
    }

    /// <inheritdoc />
    public async Task DeleteObjectAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        var request = new DeleteObjectRequest { BucketName = _bucketName, Key = key };

        await _s3Client.DeleteObjectAsync(request, cancellationToken);
    }

    /// <inheritdoc />
    public async Task DeleteObjectsAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var keysList = keys.ToList();
        if (keysList.Count == 0)
        {
            return;
        }

        var request = new DeleteObjectsRequest
        {
            BucketName = _bucketName,
            Objects = keysList.Select(k => new KeyVersion { Key = k }).ToList(),
        };

        await _s3Client.DeleteObjectsAsync(request, cancellationToken);
    }

    /// <summary>
    /// Disposes the underlying S3 client.
    /// </summary>
    public void Dispose()
    {
        _s3Client.Dispose();
    }
}
