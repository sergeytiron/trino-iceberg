using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace TrinoIcebergTests;

/// <summary>
/// Testcontainers implementation of the Trino + Nessie + MinIO stack
/// </summary>
public class TrinoIcebergStack : IAsyncDisposable
{
    private readonly INetwork _network;
    private readonly IContainer _minioContainer;
    private readonly IContainer _mcInitContainer;
    private readonly IContainer _nessieContainer;
    private readonly IContainer _trinoContainer;

    public string MinioEndpoint => $"http://localhost:{_minioContainer.GetMappedPublicPort(9000)}";
    public string MinioConsoleEndpoint => $"http://localhost:{_minioContainer.GetMappedPublicPort(9001)}";
    public string NessieEndpoint => $"http://localhost:{_nessieContainer.GetMappedPublicPort(19120)}";
    public string TrinoEndpoint => $"http://localhost:{_trinoContainer.GetMappedPublicPort(8080)}";

    public TrinoIcebergStack()
    {
        // Create a dedicated network for the containers
        _network = new NetworkBuilder()
            .WithName($"trino-test-{Guid.NewGuid():N}")
            .Build();

        // MinIO container
        _minioContainer = new ContainerBuilder()
            .WithImage("minio/minio:latest")
            .WithName($"minio-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithNetworkAliases("minio")
            .WithEnvironment("MINIO_ROOT_USER", "minioadmin")
            .WithEnvironment("MINIO_ROOT_PASSWORD", "minioadmin")
            .WithCommand("server", "/data", "--console-address", ":9001")
            .WithPortBinding(9000, true)
            .WithPortBinding(9001, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                .ForPort(9000)
                .ForPath("/minio/health/live")))
            .Build();

        // MinIO client (mc) init container - creates bucket
        _mcInitContainer = new ContainerBuilder()
            .WithImage("minio/mc:latest")
            .WithName($"mc-init-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithEntrypoint("/bin/sh", "-c")
            .WithCommand(
                "until (/usr/bin/mc alias set local http://minio:9000 minioadmin minioadmin) do " +
                "echo 'Waiting for MinIO...' && sleep 2; done && " +
                "/usr/bin/mc mb -p local/warehouse || true")
            .Build();

        // Nessie catalog container
        _nessieContainer = new ContainerBuilder()
            .WithImage("ghcr.io/projectnessie/nessie:latest")
            .WithName($"nessie-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithNetworkAliases("nessie")
            .WithEnvironment("QUARKUS_PROFILE", "prod")
            .WithEnvironment("NESSIE_VERSION_STORE_TYPE", "IN_MEMORY")
            .WithPortBinding(19120, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                .ForPort(19120)
                .ForPath("/api/v2/config")))
            .Build();

        // Trino container with embedded configuration
        // Configuration files are copied into the container instead of bind mounting
        // This makes the tests more portable and eliminates path resolution issues
        var trinoBuilder = new ContainerBuilder()
            .WithImage("trinodb/trino:478")
            .WithName($"trino-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithNetworkAliases("trino")
            .WithPortBinding(8080, true);

        // Try to find config directory using environment variable or relative path
        var envTrinoConfigDir = Environment.GetEnvironmentVariable("TRINO_CONFIG_DIR");
        string trinoConfigDir;
        
        if (!string.IsNullOrWhiteSpace(envTrinoConfigDir) && Directory.Exists(envTrinoConfigDir))
        {
            trinoConfigDir = envTrinoConfigDir;
        }
        else
        {
            // Navigate from bin/Debug/net10.0 back to tests folder, then to trino/etc
            var baseDir = AppContext.BaseDirectory;
            trinoConfigDir = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", "trino", "etc"));
        }
        
        if (!Directory.Exists(trinoConfigDir))
        {
            throw new DirectoryNotFoundException(
                $"Trino config directory not found. Tried environment variable TRINO_CONFIG_DIR='{envTrinoConfigDir}', " +
                $"and fallback path: {trinoConfigDir}. Set TRINO_CONFIG_DIR to the path containing Trino configuration files.");
        }

        // Copy all config files into the container instead of bind mounting
        foreach (var file in Directory.GetFiles(trinoConfigDir, "*", SearchOption.AllDirectories))
        {
            var relativePath = Path.GetRelativePath(trinoConfigDir, file);
            // Get the container directory path (parent directory of the target file)
            var relativeDir = Path.GetDirectoryName(relativePath) ?? "";
            var containerDir = string.IsNullOrEmpty(relativeDir) 
                ? "/etc/trino" 
                : $"/etc/trino/{relativeDir.Replace("\\", "/")}";
            trinoBuilder = trinoBuilder.WithResourceMapping(file, containerDir);
        }
        
        _trinoContainer = trinoBuilder
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                .ForPort(8080)
                .ForPath("/v1/info")
                .ForStatusCode(System.Net.HttpStatusCode.OK)))
            .Build();
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        // Start network
        await _network.CreateAsync(cancellationToken).ConfigureAwait(false);

        // Start MinIO first
        await _minioContainer.StartAsync(cancellationToken).ConfigureAwait(false);

        // Initialize MinIO bucket (one-shot container, wait for completion)
        await _mcInitContainer.StartAsync(cancellationToken).ConfigureAwait(false);
        
        // Wait for mc-init to complete by checking its exit code
        var exitCode = await _mcInitContainer.GetExitCodeAsync(cancellationToken).ConfigureAwait(false);
        if (exitCode != 0)
        {
            var logs = await _mcInitContainer.GetLogsAsync(ct: cancellationToken).ConfigureAwait(false);
            throw new InvalidOperationException($"MinIO bucket initialization failed with exit code {exitCode}. Logs: {logs.Stdout}{Environment.NewLine}{logs.Stderr}");
        }

        // Start Nessie
        await _nessieContainer.StartAsync(cancellationToken).ConfigureAwait(false);

        // Start Trino last
        await _trinoContainer.StartAsync(cancellationToken).ConfigureAwait(false);
        
        // Trino's HTTP endpoint responds before the server is fully initialized
        // Wait for Trino to complete internal initialization
        await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
    }

    public async Task<string> ExecuteTrinoQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL query cannot be null or empty", nameof(sql));
        }

        var execResult = await _trinoContainer.ExecAsync(
            new[] { "trino", "--execute", sql },
            cancellationToken).ConfigureAwait(false);

        // Trino writes results to stdout and some messages to stderr
        var output = execResult.Stdout;
        if (!string.IsNullOrEmpty(execResult.Stderr))
        {
            output += Environment.NewLine + execResult.Stderr;
        }

        return output;
    }

    public async ValueTask DisposeAsync()
    {
        // Dispose in reverse order of startup
        // Continue cleanup even if individual disposals fail
        try { await _trinoContainer.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
        try { await _nessieContainer.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
        try { await _mcInitContainer.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
        try { await _minioContainer.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
        try { await _network.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
    }
}
