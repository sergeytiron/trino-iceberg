namespace IntegrationTests;

internal static class TrinoConfigurationProvider
{
    public static byte[] GetConfigPropertiesBytes() =>
        """
            coordinator=true
            node-scheduler.include-coordinator=true
            http-server.http.port=8080
            query.max-memory=512MB
            discovery.uri=http://trino:8080

            # Faster query execution for testing
            query.min-expire-age=0s
            query.client.timeout=5m
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
            -Xms256M
            -Xmx256M
            -XX:+UseSerialGC
            -XX:+ExitOnOutOfMemoryError
            -XX:TieredStopAtLevel=1
            -Djdk.attach.allowAttachSelf=true
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

            # Performance optimizations for testing
            iceberg.metadata-cache.enabled=true
            iceberg.expire-snapshots.min-retention=0s
            iceberg.remove-orphan-files.min-retention=0s

            # Reduce S3 overhead
            s3.streaming.part-size=8MB
            """u8.ToArray();
}
