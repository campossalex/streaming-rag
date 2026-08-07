-- =====================================================================
-- Intermediate and output Kafka topics
--
-- DDL only. Loaded into EVERY job's session with `sql-client.sh -i`, because Flink's default
-- catalog is in-memory and per-session: a table created by one submission is invisible to the
-- next. Nothing here starts a job -- CREATE TABLE and CREATE MODEL are metadata.
--
-- Rendered by /opt/submit.sh; placeholders come from .env so no key is written to disk.
-- =====================================================================

-- ------------------------------------------------------------- topics
-- Stage 1 output: the alarm joined to its manual section, plus the derived planning fields.
CREATE TABLE alarm_context (
    alarm_id         STRING,
    asset_id         STRING,
    line_id          STRING,
    -- The generator's own label for what it simulated. Not used by the pipeline; it is here
    -- so you can see at a glance whether the retrieved doc_ref matches the fault that was
    -- actually raised, without re-reading the alarm text.
    fault_code       STRING,
    alarm_text       STRING,
    vibration_mms    DOUBLE,
    temp_c           INT,
    pressure_bar     INT,
    criticality      STRING,
    shift            STRING,
    matched_section  STRING,
    doc_ref          STRING,
    category         STRING,
    severity         INT,
    score            FLOAT,
    match_confidence STRING,
    priority         STRING,
    queue            STRING,
    sla_due          TIMESTAMP(3),
    t_alarm          TIMESTAMP(3),
    t_context        TIMESTAMP(3),
    planner_input    STRING,
    -- Needed only because the JDBC upsert below uses ON CONFLICT DO NOTHING, and 2.3 requires
    -- event time to decide which record is "first". Nothing here windows or joins on time.
    -- Kept off machine_alarms deliberately: that table feeds the correlated VECTOR_SEARCH, and
    -- there is no reason to perturb the one part of this pipeline that was hard to make plan.
    WATERMARK FOR t_context AS t_context - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'alarm-context',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-rag',
    'format'                       = 'json',
    'scan.startup.mode'            = 'latest-offset'
);

-- Stage 2 output: the finished work order.
CREATE TABLE work_orders (
    alarm_id         STRING,
    asset_id         STRING,
    line_id          STRING,
    fault_code       STRING,
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
    t_alarm          TIMESTAMP(3),
    t_context        TIMESTAMP(3),
    t_action         TIMESTAMP(3),
    ai_work_order    STRING,
    WATERMARK FOR t_action AS t_action - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'work-orders',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    -- This table is now read as well as written: the incident upsert and the final status row
    -- both source from it. A Kafka source defaults to committed offsets, which needs a group id
    -- and a startup mode -- without them the job fails at submit with
    -- "Property group.id is required when using committed offset for offsets initializer".
    'properties.group.id'          = 'flink-rag-orders',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json'
);
