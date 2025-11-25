-- Populate shared_data with test records
INSERT INTO iceberg.common_test_data.shared_data VALUES
    (1, 'test', 'Alice', 30, true, 'alice', 'alice@example.com', '555-0001', 'Hello World', NULL, NULL, NULL),
    (2, 'data', 'Bob', 25, false, 'bob', NULL, '555-0002', 'It''s a test', NULL, NULL, NULL),
    (3, NULL, 'Charlie', 35, true, 'charlie', 'charlie@example.com', NULL, 'Quote: "test"', NULL, NULL, NULL),
    (100, 'test', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 9223372036854775807, 3.14159, 99.99),
    (200, 'data', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Populate category_data for aggregation tests
INSERT INTO iceberg.common_test_data.category_data VALUES (100, 'A'), (200, 'B'), (150, 'A'), (300, 'B');

-- Populate employee_data for snake_case mapping tests
INSERT INTO iceberg.common_test_data.employee_data VALUES
    (1, 'John', 'Doe', DATE '2020-01-15'),
    (2, 'Jane', 'Smith', DATE '2019-03-22');
