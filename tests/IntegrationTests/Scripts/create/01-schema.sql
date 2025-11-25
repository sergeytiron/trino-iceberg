-- Create the test schema (with S3 location for Iceberg tables)
CREATE SCHEMA IF NOT EXISTS iceberg.common_test_data WITH (location = 's3://warehouse/common_test_data/');
