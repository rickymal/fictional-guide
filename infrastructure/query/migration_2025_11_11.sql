DROP TABLE IF EXISTS move_registry;
DROP TABLE IF EXISTS validation_errors;
DROP TABLE IF EXISTS schema_registry;

CREATE TABLE schema_registry (
    id UUID PRIMARY KEY DEFAULT uuid(),
    namespace VARCHAR(255) NOT NULL,
    schema_avro TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT current_timestamp,
    updated_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE move_registry (
    id UUID PRIMARY KEY DEFAULT uuid(),
    schema_fk UUID,
    old_bucket VARCHAR(30),
    new_bucket VARCHAR(30),
    namespace VARCHAR(300),
    summary TEXT,
    created_at TIMESTAMP DEFAULT current_timestamp,
    FOREIGN KEY (schema_fk) REFERENCES schema_registry(id)
);

DROP VIEW IF EXISTS metric;

create view metric as (
    select new_bucket, count(*) as total
    from move_registry
    group by new_bucket
);

CREATE TABLE validation_errors (
    validation_run_id VARCHAR,
    validation_timestamp TIMESTAMP DEFAULT current_timestamp,
    failed_field VARCHAR,
    error_message VARCHAR,
    expected_type VARCHAR,
    received_value VARCHAR,
    raw_record_json VARCHAR,
    created_at TIMESTAMP DEFAULT current_timestamp
);
