namespace TrinoIcebergTests;

internal static class TrinoConfigurationProvider
{
    public static byte[] GetConfigPropertiesBytes() =>
        System.Text.Encoding.UTF8.GetBytes(GetConfigProperties());

    public static byte[] GetNodePropertiesBytes() =>
        System.Text.Encoding.UTF8.GetBytes(GetNodeProperties());

    public static byte[] GetLogPropertiesBytes() =>
        System.Text.Encoding.UTF8.GetBytes(GetLogProperties());

    public static byte[] GetJvmConfigBytes() =>
        System.Text.Encoding.UTF8.GetBytes(GetJvmConfig());

    public static byte[] GetIcebergCatalogPropertiesBytes() =>
        System.Text.Encoding.UTF8.GetBytes(GetIcebergCatalogProperties());

    private static string GetConfigProperties() => """
        coordinator=true
        node-scheduler.include-coordinator=true
        http-server.http.port=8080
        query.max-memory=1GB
        query.max-memory-per-node=512MB
        discovery.uri=http://trino:8080
        """;

    private static string GetNodeProperties() => """
        node.environment=dev
        node.id=trino-local
        node.data-dir=/data/trino
        """;

    private static string GetLogProperties() => """
        io.trino=INFO
        """;

    private static string GetJvmConfig() => """
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
        """;

    private static string GetIcebergCatalogProperties() => """
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
        """;
}