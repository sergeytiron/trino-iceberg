namespace S3Client.Models;

/// <summary>
/// Represents metadata for an object stored in S3.
/// </summary>
/// <param name="Key">The object key (path) within the bucket.</param>
/// <param name="Size">The size of the object in bytes.</param>
/// <param name="LastModified">The date and time the object was last modified.</param>
/// <param name="ETag">The entity tag (hash) of the object.</param>
public record S3ObjectInfo(string Key, long Size, DateTime LastModified, string? ETag);
