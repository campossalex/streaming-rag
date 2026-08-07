-- =====================================================================
-- JOB 4 of 4: status
--
--   machine_alarms -> RECEIVED
--   alarm-context  -> CONTEXTUALIZED | NEEDS_CONTEXT, then TRIAGED
--   work-orders    -> ACTION_DEFINED | NEEDS_REVIEW
--
-- The audit trail, appended to incident_status. Four statements in one job because they are one
-- concern: how an incident got to where it is. The planner UI appends to the same table when a
-- human moves it along.
--
-- Keeping this out of job 3 matters for a reason beyond tidiness: incident_status has no primary
-- key, so these are plain INSERTs with no conflict handling, while job 3's are keyed upserts.
-- Different failure modes, different job.
--
-- Submit on its own with:  ./run.sh submit status
-- =====================================================================

SET 'pipeline.name' = 'rag-4-status';

EXECUTE STATEMENT SET
BEGIN

-- ---- the status timeline ----
-- RECEIVED is stamped with the alarm's own time, not the time this statement ran, so an incident
-- that waits in Kafka does not get a flattering timeline.
INSERT INTO pg_incident_status
SELECT alarm_id, 'RECEIVED', 'pipeline', CAST(NULL AS STRING), alarm_time
FROM machine_alarms;

-- Retrieval either found something usable or it did not. `low` is not a weak match to be papered
-- over -- it is the measured signal that nothing in the manual covers this alarm, and it is what
-- routes the incident to a human.
INSERT INTO pg_incident_status
SELECT
    alarm_id,
    CASE WHEN match_confidence = 'low' THEN 'NEEDS_CONTEXT' ELSE 'CONTEXTUALIZED' END,
    'pipeline',
    CONCAT(COALESCE(doc_ref, 'no match'), ' at ', CAST(ROUND(CAST(score AS DOUBLE), 3) AS STRING)),
    t_context
FROM alarm_context;

-- Nudged one millisecond past t_context so the timeline sorts deterministically. Both rows are
-- stamped from the same emit, and with identical timestamps the order of CONTEXTUALIZED and
-- TRIAGED came out as a coin flip. Triage genuinely happens after retrieval -- severity comes
-- from the retrieved section -- so this makes the display match the causality.
INSERT INTO pg_incident_status
SELECT alarm_id, 'TRIAGED', 'pipeline', CONCAT(priority, ' to ', queue),
       t_context + INTERVAL '0.001' SECOND
FROM alarm_context;

-- The planner's grounding guard is the signal here: told to answer only from the retrieved
-- section, it returns "insufficient documentation" rather than inventing a part number. That
-- string is the contract between the prompt and this CASE -- change one and change the other.
INSERT INTO pg_incident_status
SELECT
    alarm_id,
    CASE WHEN ai_work_order LIKE '%insufficient documentation%'
         THEN 'NEEDS_REVIEW' ELSE 'ACTION_DEFINED' END,
    'pipeline',
    CASE WHEN ai_work_order LIKE '%insufficient documentation%'
         THEN 'planner could not ground an action' ELSE NULL END,
    t_action
FROM work_orders;

END;
