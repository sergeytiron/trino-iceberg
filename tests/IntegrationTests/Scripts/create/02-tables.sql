-- shared_data: multi-purpose table combining several test patterns
-- - Rows 1-3: people pattern (id, name, age, active)
-- - Rows 1-3: users pattern (id, username)
-- - Rows 1-3: contacts pattern (id, name, email, phone) with NULLs
-- - Rows 1-3: messages pattern (id, content) with escaping
-- - Row 100: measurements pattern (numeric extremes)
CREATE TABLE IF NOT EXISTS iceberg.common_test_data.shared_data (
    id int,
    value varchar,
    name varchar,
    age int,
    active boolean,
    username varchar,
    email varchar,
    phone varchar,
    content varchar,
    value_int bigint,
    value_double double,
    value_decimal decimal(10,2)
);

-- category_data: for aggregation tests
CREATE TABLE IF NOT EXISTS iceberg.common_test_data.category_data (
    amount bigint,
    category varchar
);

-- employee_data: for snake_case column mapping + date type tests
CREATE TABLE IF NOT EXISTS iceberg.common_test_data.employee_data (
    employee_id int,
    first_name varchar,
    last_name varchar,
    hire_date date
);

-- events_time_travel: for time-travel query tests (INSERTs happen during tests)
CREATE TABLE IF NOT EXISTS iceberg.common_test_data.events_time_travel (
    event_id bigint,
    event_type varchar,
    event_time timestamp
);

-- scalar_test: for QueryScalar tests with various data types
CREATE TABLE IF NOT EXISTS iceberg.common_test_data.scalar_test (
    id int,
    int_value int,
    string_value varchar,
    bool_value boolean,
    guid_value varchar,
    datetime_value timestamp,
    decimal_value decimal(10,2),
    category varchar
);
