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

        // Trino container with catalog configuration
        var trinoConfigDir = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "trino", "etc");
        
        _trinoContainer = new ContainerBuilder()
            .WithImage("trinodb/trino:478")
            .WithName($"trino-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithNetworkAliases("trino")
            .WithPortBinding(8080, true)
            .WithBindMount(trinoConfigDir, "/etc/trino", AccessMode.ReadOnly)
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
        
        // Give mc-init time to create the bucket
        await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken).ConfigureAwait(false);

        // Start Nessie
        await _nessieContainer.StartAsync(cancellationToken).ConfigureAwait(false);

        // Start Trino last
        await _trinoContainer.StartAsync(cancellationToken).ConfigureAwait(false);
        
        // Give Trino additional time to fully initialize (health check only means server is responding)
        await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
    }

    public async Task<string> ExecuteTrinoQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        var execResult = await _trinoContainer.ExecAsync(
            new[] { "trino", "--execute", sql },
            cancellationToken).ConfigureAwait(false);

        // Return combined output (Trino writes to both stdout and stderr)
        return execResult.Stdout + execResult.Stderr;
    }

    public async ValueTask DisposeAsync()
    {
        await _trinoContainer.DisposeAsync().ConfigureAwait(false);
        await _nessieContainer.DisposeAsync().ConfigureAwait(false);
        await _mcInitContainer.DisposeAsync().ConfigureAwait(false);
        await _minioContainer.DisposeAsync().ConfigureAwait(false);
        await _network.DisposeAsync().ConfigureAwait(false);
    }
}
