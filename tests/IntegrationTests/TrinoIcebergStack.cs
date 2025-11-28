using System.Data;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Trino.Data.ADO.Server;

namespace IntegrationTests;

/// <summary>
/// Testcontainers implementation of the Trino + Nessie + MinIO stack
/// </summary>
public class TrinoIcebergStack : IAsyncDisposable
{
    private const string MinioRootUser = "minioadmin";
    private const string MinioRootPassword = "minioadmin";
    private const int MinioS3Port = 9000;
    private const int MinioConsolePort = 9001;
    private const int NessiePort = 19120;
    private const int TrinoPort = 8080;
    private const string WarehouseBucketName = "warehouse";

    // Container image versions
    private const string MinioImageVersion = "minio/minio:RELEASE.2025-09-07T16-13-09Z";
    private const string NessieImageVersion = "ghcr.io/projectnessie/nessie:0.105.7";
    private const string TrinoImageVersion = "trinodb/trino:478";

    private readonly INetwork _network;
    private readonly IContainer _minioContainer;
    private readonly IContainer _nessieContainer;
    private readonly IContainer _trinoContainer;
    private readonly Action<string>? _logger;

    /// <summary>Gets the MinIO S3 API endpoint URL</summary>
    public string MinioEndpoint => $"http://localhost:{_minioContainer.GetMappedPublicPort(MinioS3Port)}";

    /// <summary>Gets the Trino query engine endpoint URL</summary>
    public string TrinoEndpoint => $"http://localhost:{_trinoContainer.GetMappedPublicPort(TrinoPort)}";

    /// <summary>
    /// Initializes a new instance of the Trino + Nessie + MinIO stack.
    /// </summary>
    /// <param name="logger">Optional logger for diagnostic messages</param>
    public TrinoIcebergStack(Action<string>? logger = null)
    {
        _logger = logger;

        // Create a dedicated network for the containers
        _network = new NetworkBuilder().WithName($"trino-test-{Guid.NewGuid():N}").Build();

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
            .WithImage(MinioImageVersion)
            .WithName($"minio-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("minio")
            .WithEnvironment("MINIO_ROOT_USER", MinioRootUser)
            .WithEnvironment("MINIO_ROOT_PASSWORD", MinioRootPassword)
            .WithCommand("server", "/data", "--console-address", $":{MinioConsolePort}")
            .WithPortBinding(MinioS3Port, true)
            .WithPortBinding(MinioConsolePort, true)
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(r => r.ForPort(MinioS3Port).ForPath("/minio/health/live"))
            )
            .Build();
    }

    private IContainer BuildNessieContainer(INetwork network)
    {
        return new ContainerBuilder()
            .WithImage(NessieImageVersion)
            .WithName($"nessie-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("nessie")
            .WithEnvironment("QUARKUS_PROFILE", "prod")
            .WithEnvironment("NESSIE_VERSION_STORE_TYPE", "IN_MEMORY")
            .WithPortBinding(NessiePort, true)
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(r => r.ForPort(NessiePort).ForPath("/api/v2/config"))
            )
            .Build();
    }

    private IContainer BuildTrinoContainer(INetwork network)
    {
        return new ContainerBuilder()
            .WithImage(TrinoImageVersion)
            .WithName($"trino-{Guid.NewGuid():N}")
            .WithNetwork(network)
            .WithNetworkAliases("trino")
            .WithPortBinding(TrinoPort, true)
            .WithResourceMapping(TrinoConfigurationProvider.GetConfigPropertiesBytes(), "/etc/trino/config.properties")
            .WithResourceMapping(TrinoConfigurationProvider.GetNodePropertiesBytes(), "/etc/trino/node.properties")
            .WithResourceMapping(TrinoConfigurationProvider.GetLogPropertiesBytes(), "/etc/trino/log.properties")
            .WithResourceMapping(TrinoConfigurationProvider.GetJvmConfigBytes(), "/etc/trino/jvm.config")
            .WithResourceMapping(
                TrinoConfigurationProvider.GetIcebergCatalogPropertiesBytes(),
                "/etc/trino/catalog/iceberg.properties"
            )
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("SERVER STARTED"))
            .Build();
    }

    /// <summary>
    /// Starts all containers in the stack in the correct dependency order:
    /// Network → (MinIO + Nessie in parallel) → MinIO bucket initialization → Trino
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        // Start network
        await _network.CreateAsync(cancellationToken);

        // Start MinIO and Nessie in parallel - they don't depend on each other
        await Task.WhenAll(
            _minioContainer.StartAsync(cancellationToken),
            _nessieContainer.StartAsync(cancellationToken)
        );

        // Initialize MinIO bucket and start Trino in parallel
        // Trino JVM startup takes ~30s and doesn't need the bucket until first query
        // Bucket init is fast (~2s) and will complete before Trino is ready to query
        await Task.WhenAll(
            InitializeMinIOBucketAsync(cancellationToken),
            _trinoContainer.StartAsync(cancellationToken)
        );

        // Execute init scripts after Trino is ready (auto-discovered from Scripts/create and Scripts/insert)
        await ExecuteInitScriptsAsync(cancellationToken);
    }

    /// <summary>
    /// Executes init scripts from Scripts/create and Scripts/insert folders (in that order).
    /// Scripts are copied to the Trino container and executed via Trino CLI.
    /// </summary>
    private async Task ExecuteInitScriptsAsync(CancellationToken cancellationToken)
    {
        const string containerScriptsDir = "/tmp/init-scripts";

        // Discover scripts from convention-based folders (create first, then insert)
        var scriptFolders = new[] { "Scripts/create", "Scripts/insert" };
        var scripts = scriptFolders
            .Where(Directory.Exists)
            .SelectMany(folder => Directory.GetFiles(folder, "*.sql").OrderBy(f => f))
            .ToList();

        if (scripts.Count == 0)
        {
            _logger?.Invoke("No init scripts found in Scripts/create or Scripts/insert");
            return;
        }

        foreach (var filePath in scripts)
        {
            // Copy the SQL file to the Trino container
            var fileName = Path.GetFileName(filePath);
            var containerScriptPath = $"{containerScriptsDir}/{fileName}";

            var scriptContent = await File.ReadAllBytesAsync(filePath, cancellationToken);
            // 644 = Owner RW, Group R, Others R (0644 octal = 420 decimal)
            const uint fileMode644 = 420;
            await _trinoContainer.CopyAsync(scriptContent, containerScriptPath, fileMode644, ct: cancellationToken);
            _logger?.Invoke($"Copied {fileName} to container at {containerScriptPath}");

            // Execute the script using Trino CLI
            var trinoCliCommand = $"trino --server localhost:{TrinoPort} --file {containerScriptPath}";
            var result = await _trinoContainer.ExecAsync(["sh", "-c", trinoCliCommand], cancellationToken);

            if (result.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Init script execution failed for {fileName} with exit code {result.ExitCode}.{Environment.NewLine}"
                        + $"Command: {trinoCliCommand}{Environment.NewLine}"
                        + $"Stdout: {result.Stdout}{Environment.NewLine}"
                        + $"Stderr: {result.Stderr}"
                );
            }

            _logger?.Invoke($"Executed init script: {fileName}");
        }
    }

    /// <summary>
    /// Executes a SQL statement against Trino using ADO.NET.
    /// Use this for DDL (CREATE, DROP) and DML (INSERT, UPDATE, DELETE) operations.
    /// </summary>
    /// <param name="sql">The SQL statement to execute</param>
    /// <param name="schema">Optional schema name (default: null uses catalog default)</param>
    /// <returns>Number of rows affected for DML, or -1 for DDL</returns>
    public int ExecuteNonQuery(string sql, string? schema = null)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL query cannot be null or empty", nameof(sql));
        }

        var properties = new TrinoConnectionProperties
        {
            Server = new Uri(TrinoEndpoint),
            Catalog = "iceberg",
            Schema = schema!,
        };

        using var connection = new TrinoConnection(properties);
        connection.Open();
        using var command = new TrinoCommand(connection, sql);
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// Executes multiple SQL statements using ADO.NET with connection reuse.
    /// Statements are executed in parallel for better performance.
    /// </summary>
    /// <param name="sqlStatements">The SQL statements to execute</param>
    /// <param name="schema">Optional schema name (default: null uses catalog default)</param>
    public void ExecuteBatch(IEnumerable<string> sqlStatements, string? schema = null)
    {
        var properties = new TrinoConnectionProperties
        {
            Server = new Uri(TrinoEndpoint),
            Catalog = "iceberg",
            Schema = schema!,
        };

        using var connection = new TrinoConnection(properties);
        connection.Open();

        var tasks = new List<Task>();
        foreach (var sql in sqlStatements)
        {
            tasks.Add(
                Task.Run(() =>
                {
                    using var command = new TrinoCommand(connection, sql);
                    return command.ExecuteNonQuery();
                })
            );
        }

        Task.WaitAll([.. tasks]);
    }

    /// <summary>
    /// Creates a new S3 bucket in MinIO.
    /// </summary>
    /// <param name="bucketName">The name of the bucket to create</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task CreateBucketAsync(string bucketName, CancellationToken cancellationToken = default)
    {
        var createBucketCommand =
            $"mc alias set local http://localhost:{MinioS3Port} {MinioRootUser} {MinioRootPassword} && mc mb -p local/{bucketName} || true";

        var createBucketResult = await _minioContainer.ExecAsync(["sh", "-c", createBucketCommand], cancellationToken);

        if (createBucketResult.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"MinIO bucket creation failed with exit code {createBucketResult.ExitCode}. Stdout: {createBucketResult.Stdout}{Environment.NewLine}Stderr: {createBucketResult.Stderr}"
            );
        }
    }

    /// <summary>
    /// Disposes all containers and the network in reverse order of startup.
    /// Continues cleanup even if individual disposals fail.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Dispose in reverse order of startup
        // Continue cleanup even if individual disposals fail
        await DisposeContainerAsync(_trinoContainer, "Trino");
        await DisposeContainerAsync(_nessieContainer, "Nessie");
        await DisposeContainerAsync(_minioContainer, "MinIO");
        await DisposeNetworkAsync();
    }

    private async Task DisposeContainerAsync(IContainer container, string name)
    {
        try
        {
            await container.DisposeAsync();
            _logger?.Invoke($"{name} container disposed successfully");
        }
        catch (Exception ex)
        {
            var message = $"Error disposing {name} container: {ex.Message}";
            _logger?.Invoke(message);
            // Continue cleanup even if disposal fails
        }
    }

    private async Task DisposeNetworkAsync()
    {
        try
        {
            await _network.DisposeAsync();
            _logger?.Invoke("Network disposed successfully");
        }
        catch (Exception ex)
        {
            var message = $"Error disposing network: {ex.Message}";
            _logger?.Invoke(message);
            // Continue cleanup even if disposal fails
        }
    }

    private async Task InitializeMinIOBucketAsync(CancellationToken cancellationToken)
    {
        var createBucketCommand =
            $"mc alias set local http://localhost:{MinioS3Port} {MinioRootUser} {MinioRootPassword} && mc mb -p local/{WarehouseBucketName} || true";

        var createBucketResult = await _minioContainer.ExecAsync(["sh", "-c", createBucketCommand], cancellationToken);

        if (createBucketResult.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"MinIO bucket initialization failed with exit code {createBucketResult.ExitCode}. Stdout: {createBucketResult.Stdout}{Environment.NewLine}Stderr: {createBucketResult.Stderr}"
            );
        }
    }
}
