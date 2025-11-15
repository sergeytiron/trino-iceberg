#!/bin/bash
# Quick validation script for the Trino + Nessie + MinIO stack

set -e

echo "=== Checking services status ==="
docker compose ps

echo -e "\n=== Testing Trino connectivity ==="
echo "SELECT 'Hello from Trino ' || version();" | docker exec -i trino trino

echo -e "\n=== Creating demo schema and table ==="
cat <<'SQL' | docker exec -i trino trino
CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/');
CREATE TABLE IF NOT EXISTS iceberg.demo.test_numbers (id int, value varchar) WITH (format = 'PARQUET');
INSERT INTO iceberg.demo.test_numbers VALUES (1, 'one'), (2, 'two'), (3, 'three');
SELECT * FROM iceberg.demo.test_numbers ORDER BY id;
SQL

echo -e "\n=== Checking MinIO bucket contents ==="
docker run --rm --network trino-iceberg_default --entrypoint sh minio/mc -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 && \
   mc ls -r local/warehouse/ | head -10"

echo -e "\n✅ All tests passed! Stack is working correctly."
echo "Trino UI: http://localhost:8080"
echo "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "Nessie API: http://localhost:19120/api/v2"
