using System.Net;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace TrinoIcebergTests;

/// <summary>
/// Testcontainers implementation of the Trino + Nessie + MinIO stack
/// </summary>
public class TrinoIcebergStack : IAsyncDisposable
{
    private const string MinioRootUser = "minioadmin";
    private const string MinioRootPassword = "minioadmin";
    private const string MinioS3Port = "9000";
    private const string MinioConsolePort = "9001";
    private const string NessiePort = "19120";
    private const string TrinoPort = "8080";
    private const string WarehouseBucketName = "warehouse";

    private readonly INetwork _network;
    private readonly IContainer _minioContainer;
    private readonly IContainer _nessieContainer;
    private readonly IContainer _trinoContainer;

    public string MinioEndpoint => $"http://localhost:{_minioContainer.GetMappedPublicPort(int.Parse(MinioS3Port))}";
    public string MinioConsoleEndpoint => $"http://localhost:{_minioContainer.GetMappedPublicPort(int.Parse(MinioConsolePort))}";
    public string NessieEndpoint => $"http://localhost:{_nessieContainer.GetMappedPublicPort(int.Parse(NessiePort))}";
    public string TrinoEndpoint => $"http://localhost:{_trinoContainer.GetMappedPublicPort(int.Parse(TrinoPort))}";

    public TrinoIcebergStack()
    {
        // Create a dedicated network for the containers
        _network = new NetworkBuilder()
            .WithName($"trino-test-{Guid.NewGuid():N}")
            .Build();

        // MinIO container with mc client included for bucket initialization
        _minioContainer = BuildMinioContainer(_network);

        // Nessie catalog container
        _nessieContainer = BuildNessieContainer(_network);

        // Trino container with hardcoded configuration
        // All config files are embedded as strings in C# code - no external files needed
        _trinoContainer = BuildTrinoContainer(_network);
    }

    private IContainer BuildMinioContainer(INetwork network)
    {
        return new ContainerBuilder()
            .WithImage("minio/minio:latest")
            .WithName($"minio-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("minio")
            .WithEnvironment("MINIO_ROOT_USER", MinioRootUser)
            .WithEnvironment("MINIO_ROOT_PASSWORD", MinioRootPassword)
            .WithCommand("server", "/data", "--console-address", $":{MinioConsolePort}")
            .WithPortBinding(int.Parse(MinioS3Port), true)
            .WithPortBinding(int.Parse(MinioConsolePort), true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                .ForPort(ushort.Parse(MinioS3Port))
                .ForPath("/minio/health/live")))
            .Build();
    }

    private IContainer BuildNessieContainer(INetwork network)
    {
        return new ContainerBuilder()
            .WithImage("ghcr.io/projectnessie/nessie:latest")
            .WithName($"nessie-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("nessie")
            .WithEnvironment("QUARKUS_PROFILE", "prod")
            .WithEnvironment("NESSIE_VERSION_STORE_TYPE", "IN_MEMORY")
            .WithPortBinding(int.Parse(NessiePort), true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                .ForPort(ushort.Parse(NessiePort))
                .ForPath("/api/v2/config")))
            .Build();
    }

    private IContainer BuildTrinoContainer(INetwork network)
    {
        return new ContainerBuilder()
            .WithImage("trinodb/trino:478")
            .WithName($"trino-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("trino")
            .WithPortBinding(int.Parse(TrinoPort), true)
            .WithResourceMapping(TrinoConfigurationProvider.GetConfigPropertiesBytes(), "/etc/trino/config.properties")
            .WithResourceMapping(TrinoConfigurationProvider.GetNodePropertiesBytes(), "/etc/trino/node.properties")
            .WithResourceMapping(TrinoConfigurationProvider.GetLogPropertiesBytes(), "/etc/trino/log.properties")
            .WithResourceMapping(TrinoConfigurationProvider.GetJvmConfigBytes(), "/etc/trino/jvm.config")
            .WithResourceMapping(TrinoConfigurationProvider.GetIcebergCatalogPropertiesBytes(), "/etc/trino/catalog/iceberg.properties")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("SERVER STARTED"))
            .Build();
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        // Start network
        await _network.CreateAsync(cancellationToken).ConfigureAwait(false);

        // Start MinIO first
        await _minioContainer.StartAsync(cancellationToken).ConfigureAwait(false);

        // Initialize MinIO bucket using exec instead of a separate container
        // The MinIO container includes the mc client
        await InitializeMinIOBucketAsync(cancellationToken);

        // Start Nessie
        await _nessieContainer.StartAsync(cancellationToken).ConfigureAwait(false);

        // Start Trino last
        await _trinoContainer.StartAsync(cancellationToken).ConfigureAwait(false);
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
        try { await _minioContainer.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
        try { await _network.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
    }

    private async Task InitializeMinIOBucketAsync(CancellationToken cancellationToken)
    {
        var createBucketCommand = $"mc alias set local http://localhost:{MinioS3Port} {MinioRootUser} {MinioRootPassword} && mc mb -p local/{WarehouseBucketName} || true";
        
        var createBucketResult = await _minioContainer.ExecAsync(
            new[] { "sh", "-c", createBucketCommand },
            cancellationToken).ConfigureAwait(false);
        
        if (createBucketResult.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"MinIO bucket initialization failed with exit code {createBucketResult.ExitCode}. " +
                $"Stdout: {createBucketResult.Stdout}{Environment.NewLine}Stderr: {createBucketResult.Stderr}");
        }
    }
}
