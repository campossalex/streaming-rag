-- Incident store for the streaming RAG pipeline.
--
-- Run automatically by the postgres image from /docker-entrypoint-initdb.d on first start.
-- That is the compose-native equivalent of the `sudo -i -u postgres psql -f` step the k8s
-- scenarios use. It runs ONCE, when the data volume is empty -- editing this file does not
-- re-apply it to an existing volume. `./run.sh clean` drops the volume; `./run.sh down` does not.
--
-- Conventions match the other lab-day scenarios: public schema, snake_case, VARCHAR keys,
-- plain TIMESTAMP (never TIMESTAMPTZ, because Flink's TIMESTAMP(3) carries no zone), JSON kept
-- as TEXT rather than JSONB, and enum-like columns as VARCHAR with the values in a comment.

-- ===================================================================== incident
-- One row per alarm. Written from three directions, which is why the column groups below are
-- kept strictly separate:
--
--   1. The Flink table `pg_incident_context` declares ONLY the machine-context columns.
--   2. The Flink table `pg_incident_action`  declares ONLY the planner columns.
--   3. The planner UI writes ONLY the human columns.
--
-- A Flink JDBC sink emits `INSERT ... ON CONFLICT (pk) DO UPDATE SET <declared columns>`, so a
-- sink can never null a column it does not declare. That is what makes two upserts into one row
-- safe, and what keeps a human's edit safe from a late pipeline write. Adding a column to one of
-- those Flink DDLs without thinking about this is the way to break it.
CREATE TABLE IF NOT EXISTS public.incident (
    incident_id       VARCHAR(64) PRIMARY KEY,

    -- ---- alarm, from the generator ----
    asset_id          VARCHAR(32),
    line_id           VARCHAR(16),
    fault_code        VARCHAR(24),
    alarm_text        TEXT,
    vibration_mms     DOUBLE PRECISION,
    temp_c            INT,
    pressure_bar      INT,
    criticality       VARCHAR(20),   -- line_critical | standby | redundant
    shift             VARCHAR(2),    -- A | B | C

    -- ---- retrieval, from Milvus ----
    doc_ref           VARCHAR(16),
    matched_section   VARCHAR(128),
    category          VARCHAR(24),   -- mechanical | hydraulic | electrical | thermal | safety | pneumatic
    severity          SMALLINT,      -- 1 routine .. 4 stop the line
    score             DOUBLE PRECISION,
    match_confidence  VARCHAR(8),    -- high | medium | low

    -- ---- triage, derived in Flink SQL ----
    priority          VARCHAR(4),    -- P1 | P2 | P3 | P4
    queue             VARCHAR(24),
    sla_due           TIMESTAMP,

    -- ---- planner, from the chat model ----
    ai_work_order     TEXT,          -- raw JSON exactly as the model returned it

    -- ---- latency ----
    -- Both the raw timestamps and the derived figures are stored. The lag columns are what the
    -- dashboards read; the timestamps are what lets you recompute them when a number looks wrong.
    t_alarm           TIMESTAMP,     -- generator created the alarm
    t_context         TIMESTAMP,     -- stage 1 emitted, after embed + retrieve + enrich
    t_action          TIMESTAMP,     -- stage 2 emitted, after the planner call

    -- Derived here rather than in Flink SQL. The sibling scenarios compute their lag columns in
    -- the job with EXTRACT(EPOCH FROM ts) * 1000 + EXTRACT(MILLISECOND FROM ts), because Flink
    -- has no millisecond-epoch function and TIMESTAMPDIFF only resolves to seconds. Postgres
    -- subtracts timestamps exactly, and a generated column cannot drift from the timestamps it
    -- is derived from -- which is the failure the sibling scenarios guard against by storing
    -- both and recomputing when a number looks wrong.
    retrieval_lag_ms  BIGINT GENERATED ALWAYS AS
        ((EXTRACT(EPOCH FROM (t_context - t_alarm)) * 1000)::BIGINT) STORED,
    planning_lag_ms   BIGINT GENERATED ALWAYS AS
        ((EXTRACT(EPOCH FROM (t_action - t_context)) * 1000)::BIGINT) STORED,
    total_lag_ms      BIGINT GENERATED ALWAYS AS
        ((EXTRACT(EPOCH FROM (t_action - t_alarm)) * 1000)::BIGINT) STORED,

    -- ---- human, written only by the planner UI ----
    -- Deliberately absent from both Flink JDBC table definitions.
    assignee          VARCHAR(40),
    manual_action     TEXT,          -- what a planner wrote when the model could not
    resolution_notes  TEXT,

    created_at        TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================== incident_status
-- Append-only transition log. Every status change lands here, whether the pipeline or a human
-- caused it, and nothing is ever updated or deleted.
--
-- No foreign key to incident, on purpose. Each pipeline status is written by an INDEPENDENT
-- Flink statement with its own Kafka consumer, so a RECEIVED row can arrive microseconds before
-- the incident row that stage 1 upserts. An FK would turn that harmless race into a failing job.
CREATE TABLE IF NOT EXISTS public.incident_status (
    id           BIGSERIAL PRIMARY KEY,
    incident_id  VARCHAR(64) NOT NULL,
    status       VARCHAR(20) NOT NULL,
    -- RECEIVED | CONTEXTUALIZED | NEEDS_CONTEXT | TRIAGED | ACTION_DEFINED | NEEDS_REVIEW
    --   -- above written by the pipeline, below by the planner UI --
    -- ASSIGNED | IN_PROGRESS | RESOLVED | CLOSED | REJECTED
    actor        VARCHAR(40) NOT NULL,   -- 'pipeline', or the planner's name
    note         TEXT,
    at           TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_incident_status_incident ON public.incident_status (incident_id, at DESC);
CREATE INDEX IF NOT EXISTS idx_incident_status_at ON public.incident_status (at DESC);
CREATE INDEX IF NOT EXISTS idx_incident_created ON public.incident (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_incident_priority ON public.incident (priority, sla_due);

-- ========================================================== incident_current
-- Current state, derived rather than stored.
--
-- The alternative -- a `status` column on incident kept in step by a trigger -- loses a race:
-- the pipeline's status rows and its incident upsert arrive from different Kafka consumers, so
-- an UPDATE fired by the first status row can hit zero rows. Deriving means the timeline is the
-- only source of truth and there is nothing to keep in agreement.
--
-- needs_human is the flag the planner UI filters on: retrieval found nothing usable, or the
-- model declined to name an action.
CREATE OR REPLACE VIEW public.incident_current AS
SELECT
    i.*,
    s.status,
    s.at AS status_at,
    (s.status IN ('NEEDS_CONTEXT', 'NEEDS_REVIEW')) AS needs_human,
    (i.sla_due IS NOT NULL
       AND now() > i.sla_due
       AND s.status NOT IN ('RESOLVED', 'CLOSED', 'REJECTED')) AS sla_breached
FROM public.incident i
LEFT JOIN LATERAL (
    SELECT status, at
    FROM public.incident_status s
    WHERE s.incident_id = i.incident_id
    ORDER BY at DESC, id DESC
    LIMIT 1
) s ON TRUE;

GRANT ALL ON ALL TABLES IN SCHEMA public TO root;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO root;
