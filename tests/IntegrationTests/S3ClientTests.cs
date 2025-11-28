using S3Client;

namespace IntegrationTests;

/// <summary>
/// Integration tests for MinioS3Client against a real MinIO instance from the test stack.
/// Tests upload, download, and list operations.
/// </summary>
public class S3ClientTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly TrinoIcebergStackFixture _fixture;
    private TrinoIcebergStack Stack => _fixture.Stack;

    private string _tempDirectory = null!;
    private MinioS3Client _s3Client = null!;

    public S3ClientTests(ITestOutputHelper output, TrinoIcebergStackFixture fixture)
    {
        _output = output;
        _fixture = fixture;
    }

    public ValueTask InitializeAsync()
    {
        // Create a unique temp directory for each test class run
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"s3client-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDirectory);

        // Create S3 client connected to MinIO from the test stack
        _s3Client = new MinioS3Client(
            endpoint: new Uri(Stack.MinioEndpoint),
            accessKey: "minioadmin",
            secretKey: "minioadmin",
            bucketName: "warehouse"
        );

        _output.WriteLine($"MinIO endpoint: {Stack.MinioEndpoint}");
        _output.WriteLine($"Temp directory: {_tempDirectory}");

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _s3Client?.Dispose();

        // Clean up temp directory
        if (Directory.Exists(_tempDirectory))
        {
            try
            {
                Directory.Delete(_tempDirectory, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task UploadFile_UploadsSuccessfully()
    {
        // Arrange
        var testContent = $"Hello from S3ClientTests! Timestamp: {DateTime.UtcNow:O}";
        var localFile = Path.Combine(_tempDirectory, "upload-test.txt");
        await File.WriteAllTextAsync(localFile, testContent, TestContext.Current.CancellationToken);

        var s3Key = $"test-uploads/{Guid.NewGuid():N}/upload-test.txt";

        // Act
        await _s3Client.UploadFileAsync(localFile, s3Key, TestContext.Current.CancellationToken);

        // Assert - verify by listing
        var files = await _s3Client.ListFilesAsync("test-uploads/", TestContext.Current.CancellationToken);
        Assert.Contains(files, f => f.Key == s3Key);

        _output.WriteLine($"Uploaded file to: {s3Key}");
    }

    [Fact]
    public async Task DownloadFile_DownloadsCorrectContent()
    {
        // Arrange - upload a file first
        var expectedContent = $"Download test content: {Guid.NewGuid()}";
        var uploadFile = Path.Combine(_tempDirectory, "to-download.txt");
        await File.WriteAllTextAsync(uploadFile, expectedContent, TestContext.Current.CancellationToken);

        var s3Key = $"test-downloads/{Guid.NewGuid():N}/to-download.txt";
        await _s3Client.UploadFileAsync(uploadFile, s3Key, TestContext.Current.CancellationToken);

        var downloadFile = Path.Combine(_tempDirectory, "downloaded.txt");

        // Act
        await _s3Client.DownloadFileAsync(s3Key, downloadFile, TestContext.Current.CancellationToken);

        // Assert
        Assert.True(File.Exists(downloadFile));
        var actualContent = await File.ReadAllTextAsync(downloadFile, TestContext.Current.CancellationToken);
        Assert.Equal(expectedContent, actualContent);

        _output.WriteLine($"Downloaded file from: {s3Key}");
    }

    [Fact]
    public async Task ListFiles_ReturnsUploadedFiles()
    {
        // Arrange - upload multiple files with a unique prefix
        var testPrefix = $"test-list/{Guid.NewGuid():N}/";
        var fileNames = new[] { "file1.txt", "file2.txt", "subdir/file3.txt" };

        foreach (var fileName in fileNames)
        {
            var localFile = Path.Combine(_tempDirectory, Path.GetFileName(fileName));
            await File.WriteAllTextAsync(localFile, $"Content of {fileName}", TestContext.Current.CancellationToken);
            await _s3Client.UploadFileAsync(localFile, testPrefix + fileName, TestContext.Current.CancellationToken);
        }

        // Act
        var files = await _s3Client.ListFilesAsync(testPrefix, TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(3, files.Count);

        foreach (var file in files)
        {
            _output.WriteLine($"  {file.Key}: {file.Size} bytes, modified {file.LastModified:O}");
            Assert.True(file.Key.StartsWith(testPrefix), $"Key '{file.Key}' should start with '{testPrefix}'");
            Assert.True(file.Size > 0, $"Size should be > 0 but was {file.Size}");
        }
    }

    [Fact]
    public async Task ListFiles_EmptyPrefix_ListsAllFiles()
    {
        // Arrange - upload a file to ensure bucket is not empty
        var localFile = Path.Combine(_tempDirectory, "any-file.txt");
        await File.WriteAllTextAsync(localFile, "test", TestContext.Current.CancellationToken);
        var s3Key = $"test-all/{Guid.NewGuid():N}/any-file.txt";
        await _s3Client.UploadFileAsync(localFile, s3Key, TestContext.Current.CancellationToken);

        // Act
        var files = await _s3Client.ListFilesAsync(null, TestContext.Current.CancellationToken);

        // Assert - should have at least the file we uploaded
        Assert.NotEmpty(files);
        Assert.Contains(files, f => f.Key == s3Key);

        _output.WriteLine($"Total files in bucket: {files.Count}");
    }

    [Fact]
    public async Task ListFiles_NonExistentPrefix_ReturnsEmptyList()
    {
        // Arrange
        var nonExistentPrefix = $"definitely-does-not-exist-{Guid.NewGuid():N}/";

        // Act
        var files = await _s3Client.ListFilesAsync(nonExistentPrefix, TestContext.Current.CancellationToken);

        // Assert
        Assert.Empty(files);
    }

    [Fact]
    public async Task UploadFile_NonExistentLocalFile_ThrowsFileNotFoundException()
    {
        // Arrange
        var nonExistentFile = Path.Combine(_tempDirectory, "does-not-exist.txt");

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() =>
            _s3Client.UploadFileAsync(nonExistentFile, "any-key", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task DownloadFile_NonExistentKey_ThrowsException()
    {
        // Arrange
        var nonExistentKey = $"does-not-exist/{Guid.NewGuid():N}/file.txt";
        var downloadPath = Path.Combine(_tempDirectory, "should-not-exist.txt");

        // Act & Assert
        await Assert.ThrowsAsync<Amazon.S3.AmazonS3Exception>(() =>
            _s3Client.DownloadFileAsync(nonExistentKey, downloadPath, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task DownloadFile_CreatesDirectoryIfNotExists()
    {
        // Arrange - upload a file first
        var content = "nested directory test";
        var uploadFile = Path.Combine(_tempDirectory, "nested-test.txt");
        await File.WriteAllTextAsync(uploadFile, content, TestContext.Current.CancellationToken);

        var s3Key = $"test-nested/{Guid.NewGuid():N}/nested-test.txt";
        await _s3Client.UploadFileAsync(uploadFile, s3Key, TestContext.Current.CancellationToken);

        // Download to a nested path that doesn't exist
        var nestedDownloadPath = Path.Combine(_tempDirectory, "new", "nested", "path", "downloaded.txt");

        // Act
        await _s3Client.DownloadFileAsync(s3Key, nestedDownloadPath, TestContext.Current.CancellationToken);

        // Assert
        Assert.True(File.Exists(nestedDownloadPath));
        var actualContent = await File.ReadAllTextAsync(nestedDownloadPath, TestContext.Current.CancellationToken);
        Assert.Equal(content, actualContent);
    }

    [Fact]
    public async Task RoundTrip_UploadAndDownload_PreservesContent()
    {
        // Arrange - create a file with binary-like content
        var originalContent = new byte[1024];
        new Random(42).NextBytes(originalContent);

        var uploadFile = Path.Combine(_tempDirectory, "binary-test.bin");
        await File.WriteAllBytesAsync(uploadFile, originalContent, TestContext.Current.CancellationToken);

        var s3Key = $"test-roundtrip/{Guid.NewGuid():N}/binary-test.bin";

        // Act
        await _s3Client.UploadFileAsync(uploadFile, s3Key, TestContext.Current.CancellationToken);

        var downloadFile = Path.Combine(_tempDirectory, "binary-downloaded.bin");
        await _s3Client.DownloadFileAsync(s3Key, downloadFile, TestContext.Current.CancellationToken);

        // Assert
        var downloadedContent = await File.ReadAllBytesAsync(downloadFile, TestContext.Current.CancellationToken);
        Assert.Equal(originalContent, downloadedContent);

        _output.WriteLine($"Round-trip verified for {originalContent.Length} bytes");
    }
}
