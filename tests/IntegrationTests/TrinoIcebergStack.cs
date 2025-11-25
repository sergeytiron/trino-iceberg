using System.Data;
using DotNet.Testcontainers.Builders;
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
    private const string MinioImageVersion = "minio/minio:latest";
    private const string NessieImageVersion = "ghcr.io/projectnessie/nessie:latest";
    private const string TrinoImageVersion = "trinodb/trino:478";

    private readonly INetwork _network;
    private readonly IContainer _minioContainer;
    private readonly IContainer _nessieContainer;
    private readonly IContainer _trinoContainer;
    private readonly Action<string>? _logger;

    /// <summary>Gets the MinIO S3 API endpoint URL</summary>
    public string MinioEndpoint => $"http://localhost:{_minioContainer.GetMappedPublicPort(MinioS3Port)}";

    /// <summary>Gets the MinIO console endpoint URL</summary>
    public string MinioConsoleEndpoint => $"http://localhost:{_minioContainer.GetMappedPublicPort(MinioConsolePort)}";

    /// <summary>Gets the Nessie catalog endpoint URL</summary>
    public string NessieEndpoint => $"http://localhost:{_nessieContainer.GetMappedPublicPort(NessiePort)}";

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
            .WithResourceMapping(
                TrinoConfigurationProvider.GetMemoryCatalogPropertiesBytes(),
                "/etc/trino/catalog/memory.properties"
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
        await _network.CreateAsync(cancellationToken).ConfigureAwait(false);

        // Start MinIO and Nessie in parallel - they don't depend on each other
        await Task.WhenAll(
            _minioContainer.StartAsync(cancellationToken),
            _nessieContainer.StartAsync(cancellationToken)
        ).ConfigureAwait(false);

        // Initialize MinIO bucket using exec instead of a separate container
        // The MinIO container includes the mc client
        await InitializeMinIOBucketAsync(cancellationToken);

        // Start Trino last - depends on both MinIO and Nessie being ready
        await _trinoContainer.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a SQL query against Trino and returns the result.
    /// </summary>
    /// <param name="sql">The SQL query to execute</param>
    /// <param name="timeout">Maximum time to wait for query execution (default: 30 seconds)</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>The query output including stdout and stderr</returns>
    /// <exception cref="ArgumentException">Thrown when SQL is null or empty</exception>
    /// <exception cref="TimeoutException">Thrown when the query exceeds the timeout</exception>
    /// <exception cref="OperationCanceledException">Thrown when the operation is canceled via the provided cancellation token</exception>
    public async Task<string> ExecuteTrinoQueryAsync(
        string sql,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL query cannot be null or empty", nameof(sql));
        }

        timeout ??= TimeSpan.FromSeconds(30);

        using var timeoutCts = new CancellationTokenSource(timeout.Value);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            var execResult = await _trinoContainer
                .ExecAsync(["trino", "--execute", sql], linkedCts.Token)
                .ConfigureAwait(false);

            // Trino writes results to stdout and some messages to stderr
            var output = execResult.Stdout;
            if (!string.IsNullOrEmpty(execResult.Stderr))
            {
                output += Environment.NewLine + execResult.Stderr;
            }

            return output;
        }
        catch (OperationCanceledException)
            when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Query execution timed out after {timeout.Value.TotalSeconds} seconds. SQL: {sql}"
            );
        }
    }

    /// <summary>
    /// Executes a SQL statement against Trino using ADO.NET (faster than CLI exec).
    /// Use this for fixture setup and bulk operations.
    /// </summary>
    /// <param name="sql">The SQL statement to execute</param>
    /// <param name="schema">Optional schema name (default: null uses catalog default)</param>
    /// <returns>Number of rows affected for DML, or -1 for DDL/queries</returns>
    public int ExecuteSqlFast(string sql, string? schema = null)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL query cannot be null or empty", nameof(sql));
        }

        var properties = new TrinoConnectionProperties
        {
            Server = new Uri(TrinoEndpoint),
            Catalog = "iceberg",
            Schema = schema!
        };

        using var connection = new TrinoConnection(properties);
        connection.Open();
        using var command = new TrinoCommand(connection, sql);
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// Executes multiple SQL statements sequentially using ADO.NET (faster than CLI exec).
    /// Reuses a single connection for all statements.
    /// </summary>
    /// <param name="sqlStatements">The SQL statements to execute</param>
    /// <param name="schema">Optional schema name (default: null uses catalog default)</param>
    public void ExecuteSqlBatchFast(IEnumerable<string> sqlStatements, string? schema = null)
    {
        var properties = new TrinoConnectionProperties
        {
            Server = new Uri(TrinoEndpoint),
            Catalog = "iceberg",
            Schema = schema!
        };

        using var connection = new TrinoConnection(properties);
        connection.Open();

        foreach (var sql in sqlStatements)
        {
            if (string.IsNullOrWhiteSpace(sql)) continue;
            using var command = new TrinoCommand(connection, sql);
            command.ExecuteNonQuery();
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
        await DisposeContainerAsync(_trinoContainer, "Trino").ConfigureAwait(false);
        await DisposeContainerAsync(_nessieContainer, "Nessie").ConfigureAwait(false);
        await DisposeContainerAsync(_minioContainer, "MinIO").ConfigureAwait(false);
        await DisposeNetworkAsync().ConfigureAwait(false);
    }

    private async Task DisposeContainerAsync(IContainer container, string name)
    {
        try
        {
            await container.DisposeAsync().ConfigureAwait(false);
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
            await _network.DisposeAsync().ConfigureAwait(false);
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

        var createBucketResult = await _minioContainer
            .ExecAsync(["sh", "-c", createBucketCommand], cancellationToken)
            .ConfigureAwait(false);

        if (createBucketResult.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"MinIO bucket initialization failed with exit code {createBucketResult.ExitCode}. "
                    + $"Stdout: {createBucketResult.Stdout}{Environment.NewLine}Stderr: {createBucketResult.Stderr}"
            );
        }
    }
}
