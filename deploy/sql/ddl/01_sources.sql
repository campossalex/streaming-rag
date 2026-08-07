-- =====================================================================
-- Source: machine alarms from Kafka
--
-- DDL only. Loaded into EVERY job's session with `sql-client.sh -i`, because Flink's default
-- catalog is in-memory and per-session: a table created by one submission is invisible to the
-- next. Nothing here starts a job -- CREATE TABLE and CREATE MODEL are metadata.
--
-- Rendered by /opt/submit.sh; placeholders come from .env so no key is written to disk.
-- =====================================================================

-- ------------------------------------------------ machine alarms (from Kafka)
-- Produced by deploy/datagen/alarms.py, running as the `alarms` compose service. This was a
-- `datagen` table until the event vocabulary outgrew what CASE expressions can express:
-- multiple phrasings per fault, per-subsystem asset eligibility, shift derived from the clock,
-- and repeating fault bursts are all ordinary Python and all unreadable as SQL.
--
-- The pipeline below is unchanged by the swap. It still sees one row per alarm with the same
-- column names; only where they come from moved.
--
-- alarm_time arrives as a string in Flink's SQL timestamp format ('yyyy-MM-dd HH:mm:ss.SSS').
-- The generator formats it that way on purpose: the json format's default
-- timestamp-format.standard is 'SQL', and an ISO-8601 'T' separator parses to NULL silently,
-- taking every downstream SLA with it.
--
-- No watermark and no event-time attribute: nothing here windows or joins on time, and
-- declaring one would only add a source of "no output because watermarks never advanced".
CREATE TABLE machine_alarms (
    alarm_id      STRING,
    asset_id      STRING,
    line_id       STRING,
    fault_code    STRING,
    alarm_text    STRING,
    vibration_mms DOUBLE,
    temp_c        INT,
    pressure_bar  INT,
    criticality   STRING,
    shift         STRING,
    alarm_time    TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'machine_alarms',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id'          = 'flink-rag-alarms',
    'format'                       = 'json',
    -- latest-offset, not earliest: a restart must not replay the whole topic through the
    -- embedding and chat models. Replaying 10k backlogged alarms is a real bill.
    'scan.startup.mode'            = 'latest-offset',
    -- One malformed record should not kill the job mid-demo.
    'json.ignore-parse-errors'     = 'true'
);
