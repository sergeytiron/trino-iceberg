namespace TrinoIcebergTests;

internal static class TrinoConfigurationProvider
{
    public static byte[] GetConfigPropertiesBytes() =>
        """
            coordinator=true
            node-scheduler.include-coordinator=true
            http-server.http.port=8080
            query.max-memory=1GB
            query.max-memory-per-node=512MB
            discovery.uri=http://trino:8080
            """u8.ToArray();

    public static byte[] GetNodePropertiesBytes() =>
        """
            node.environment=dev
            node.id=trino-local
            node.data-dir=/data/trino
            """u8.ToArray();

    public static byte[] GetLogPropertiesBytes() => """
            io.trino=INFO
            """u8.ToArray();

    public static byte[] GetJvmConfigBytes() =>
        """
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
            """u8.ToArray();

    public static byte[] GetIcebergCatalogPropertiesBytes() =>
        """
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
            """u8.ToArray();
}
