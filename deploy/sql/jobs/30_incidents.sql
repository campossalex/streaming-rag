-- =====================================================================
-- JOB 3 of 4: incidents
--
--   alarm-context -> incident   (alarm, retrieval, triage)
--   work-orders   -> incident   (planner output)
--
-- Two statements, one job, because they are two halves of the same row and there is nothing to
-- gain from separating them further.
--
-- Both read Kafka topics that jobs 1 and 2 already write -- neither re-enters ML_PREDICT. That
-- is deliberate: sourcing these from the model pipeline instead would double the OpenAI bill
-- per alarm and nobody would notice until the invoice.
--
-- Submit on its own with:  ./run.sh submit incidents
-- =====================================================================

SET 'pipeline.name' = 'rag-3-incidents';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO pg_incident_context
SELECT
    alarm_id, asset_id, line_id, fault_code, alarm_text, vibration_mms, temp_c, pressure_bar,
    criticality, `shift`, doc_ref, matched_section, category, severity,
    CAST(score AS DOUBLE), match_confidence, priority, queue, sla_due, t_alarm, t_context
FROM alarm_context
ON CONFLICT DO NOTHING;

INSERT INTO pg_incident_action
SELECT alarm_id, ai_work_order, t_action
FROM work_orders
ON CONFLICT DO NOTHING;

END;
