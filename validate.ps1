#Requires -Version 5.1

$ErrorActionPreference = 'Stop'

Write-Host "Validating Trino + Iceberg stack..."

# Check if Docker is running
try {
    & docker info | Out-Null
} catch {
    Write-Host "Docker is not running. Please start Docker and try again."
    exit 1
}

# Check if containers are running
$containers = @("trino", "nessie", "minio")
foreach ($container in $containers) {
    $running = & docker ps --filter "name=$container" --filter "status=running" | Select-String $container
    if (-not $running) {
        Write-Host "Container $container is not running."
        exit 1
    }
}

Write-Host "All containers are running."

# Validate Trino connectivity
Write-Host "Validating Trino connectivity..."
try {
    & docker exec trino trino --execute "SELECT 1" | Out-Null
} catch {
    Write-Host "Failed to connect to Trino."
    exit 1
}

Write-Host "Trino is accessible."

# Validate Iceberg catalog
Write-Host "Validating Iceberg catalog..."

# Create schema if not exists
& docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/')"

# Create table
& docker exec trino trino --execute "CREATE TABLE iceberg.demo.numbers (n int) WITH (format='PARQUET')"

# Insert data
& docker exec trino trino --execute "INSERT INTO iceberg.demo.numbers VALUES (1),(2),(3)"

# Query data
$result = & docker exec trino trino --execute "SELECT * FROM iceberg.demo.numbers ORDER BY n"
$expected = "1`n2`n3"

if ($result -ne $expected) {
    Write-Host "Query result mismatch. Expected: $expected, Got: $result"
    exit 1
}

# Clean up
& docker exec trino trino --execute "DROP TABLE iceberg.demo.numbers"
& docker exec trino trino --execute "DROP SCHEMA iceberg.demo"

Write-Host "Validation successful!"