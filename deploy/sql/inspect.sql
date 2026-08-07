-- Read the finished work orders back out of Kafka:
--   docker compose exec jobmanager ./bin/sql-client.sh -f /opt/sql/inspect.sql
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE work_orders_in (
    asset_id         STRING,
    alarm_text       STRING,
    matched_section  STRING,
    doc_ref          STRING,
    category         STRING,
    severity         INT,
    score            FLOAT,
    match_confidence STRING,
    priority         STRING,
    queue            STRING,
    sla_due          TIMESTAMP(3),
    ai_work_order    STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'work-orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'inspect',
    'format'                       = 'json',
    'scan.startup.mode'            = 'earliest-offset'
);

-- The triage view: what broke, which manual section grounded the answer, and how urgent.
-- ai_work_order is the raw JSON from the planner model; widen the projection to read it.
SELECT priority, queue, asset_id, doc_ref, matched_section, score, match_confidence, sla_due
FROM work_orders_in;
