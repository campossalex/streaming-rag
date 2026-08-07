-- =====================================================================
-- Sinks: the Postgres incident store
--
-- DDL only. Loaded into EVERY job's session with `sql-client.sh -i`, because Flink's default
-- catalog is in-memory and per-session: a table created by one submission is invisible to the
-- next. Nothing here starts a job -- CREATE TABLE and CREATE MODEL are metadata.
--
-- Rendered by /opt/submit.sh; placeholders come from .env so no key is written to disk.
-- =====================================================================

-- ------------------------------------------------- the incident store (Postgres)
-- THE COLUMN SETS BELOW ARE DELIBERATELY DISJOINT. Both tables target the same Postgres row
-- and both upsert on incident_id, which is only safe because a Flink JDBC sink emits
--   INSERT ... ON CONFLICT (pk) DO UPDATE SET <only the columns declared here>
-- so neither stage can null the other's work, and neither can touch the human-owned columns
-- (assignee, manual_action, resolution_notes) that the planner UI writes. Verified on 2.3
-- before this was built. Adding a column to one of these without checking the other is the
-- way to break it.
--
-- ON CONFLICT DO NOTHING on every INSERT below is a Flink 2.3 requirement, not a Postgres one:
-- an append-only stream has no upsert key, and 2.3 refuses to plan a keyed sink without being
-- told how to resolve a same-key collision within the stream. Each statement emits exactly one
-- row per incident, so there is nothing to collide; DO NOTHING is the option that keeps no state.
CREATE TABLE pg_incident_context (
    incident_id      STRING,
    asset_id         STRING,
    line_id          STRING,
    fault_code       STRING,
    alarm_text       STRING,
    vibration_mms    DOUBLE,
    temp_c           INT,
    pressure_bar     INT,
    criticality      STRING,
    `shift`          STRING,
    doc_ref          STRING,
    matched_section  STRING,
    category         STRING,
    severity         INT,
    score            DOUBLE,
    match_confidence STRING,
    priority         STRING,
    queue            STRING,
    sla_due          TIMESTAMP(3),
    t_alarm          TIMESTAMP(3),
    t_context        TIMESTAMP(3),
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = '${PG_URL}',
    'table-name' = 'incident',
    'username'   = '${PG_USER}',
    'password'   = '${PG_PASSWORD}'
);

CREATE TABLE pg_incident_action (
    incident_id   STRING,
    ai_work_order STRING,
    t_action      TIMESTAMP(3),
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = '${PG_URL}',
    'table-name' = 'incident',
    'username'   = '${PG_USER}',
    'password'   = '${PG_PASSWORD}'
);

-- Append-only, no primary key, so this is a plain INSERT with no conflict handling at all.
-- The planner UI appends to the same table when a human moves an incident along.
CREATE TABLE pg_incident_status (
    incident_id STRING,
    status      STRING,
    actor       STRING,
    note        STRING,
    `at`        TIMESTAMP(3)
) WITH (
    'connector'  = 'jdbc',
    'url'        = '${PG_URL}',
    'table-name' = 'incident_status',
    'username'   = '${PG_USER}',
    'password'   = '${PG_PASSWORD}'
);
