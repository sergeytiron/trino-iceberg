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

        // MinIO container with mc client included for bucket initialization
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

        // Trino container with hardcoded configuration
        // All config files are embedded as strings in C# code - no external files needed
        _trinoContainer = new ContainerBuilder()
            .WithImage("trinodb/trino:478")
            .WithName($"trino-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithNetworkAliases("trino")
            .WithPortBinding(8080, true)
            .WithResourceMapping(GetTrinoConfigBytes("config.properties"), "/etc/trino/config.properties")
            .WithResourceMapping(GetTrinoConfigBytes("node.properties"), "/etc/trino/node.properties")
            .WithResourceMapping(GetTrinoConfigBytes("log.properties"), "/etc/trino/log.properties")
            .WithResourceMapping(GetTrinoConfigBytes("jvm.config"), "/etc/trino/jvm.config")
            .WithResourceMapping(GetTrinoConfigBytes("catalog/iceberg.properties"), "/etc/trino/catalog/iceberg.properties")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                .ForPort(8080)
                .ForPath("/v1/info")
                .ForStatusCode(System.Net.HttpStatusCode.OK)))
            .Build();
    }

    /// <summary>
    /// Returns hardcoded Trino configuration files as byte arrays
    /// </summary>
    private static byte[] GetTrinoConfigBytes(string fileName)
    {
        var content = fileName switch
        {
            "config.properties" => """
                coordinator=true
                node-scheduler.include-coordinator=true
                http-server.http.port=8080
                query.max-memory=1GB
                query.max-memory-per-node=512MB
                discovery.uri=http://trino:8080
                """,
            
            "node.properties" => """
                node.environment=dev
                node.id=trino-local
                node.data-dir=/data/trino
                """,
            
            "log.properties" => """
                io.trino=INFO
                """,
            
            "jvm.config" => """
                -server
                -Xms512M
                -Xmx1G
                -XX:+UseG1GC
                -XX:G1HeapRegionSize=32M
                -XX:+ExplicitGCInvokesConcurrent
                -XX:+HeapDumpOnOutOfMemoryError
                -XX:+ExitOnOutOfMemoryError
                -Djdk.attach.allowAttachSelf=true
                -Djava.util.logging.config.file=/etc/trino/log.properties
                """,
            
            "catalog/iceberg.properties" => """
                connector.name=iceberg
                
                # Use Nessie catalog
                iceberg.catalog.type=nessie
                iceberg.nessie-catalog.uri=http://nessie:19120/api/v2
                iceberg.nessie-catalog.default-warehouse-dir=s3://warehouse/
                
                # Use native S3 for MinIO
                fs.native-s3.enabled=true
                s3.endpoint=http://minio:9000
                s3.path-style-access=true
                s3.region=us-east-1
                s3.aws-access-key=minioadmin
                s3.aws-secret-key=minioadmin
                
                # Optional Iceberg defaults
                iceberg.file-format=PARQUET
                """,
            
            _ => throw new ArgumentException($"Unknown config file: {fileName}")
        };
        
        return System.Text.Encoding.UTF8.GetBytes(content);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        // Start network
        await _network.CreateAsync(cancellationToken).ConfigureAwait(false);

        // Start MinIO first
        await _minioContainer.StartAsync(cancellationToken).ConfigureAwait(false);

        // Initialize MinIO bucket using exec instead of a separate container
        // The MinIO container includes the mc client
        var createBucketResult = await _minioContainer.ExecAsync(
            new[] { "sh", "-c", "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb -p local/warehouse || true" },
            cancellationToken).ConfigureAwait(false);
        
        if (createBucketResult.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"MinIO bucket initialization failed with exit code {createBucketResult.ExitCode}. " +
                $"Stdout: {createBucketResult.Stdout}{Environment.NewLine}Stderr: {createBucketResult.Stderr}");
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
        try { await _minioContainer.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
        try { await _network.DisposeAsync().ConfigureAwait(false); } catch { /* Ignore disposal errors */ }
    }
}
