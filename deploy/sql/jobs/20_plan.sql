-- =====================================================================
-- JOB 2 of 4: plan
--
--   alarm-context -> ML_PREDICT(chat) -> work-orders
--
-- Separate from job 1 on purpose, and not only for readability: this is the expensive stage and
-- the throughput ceiling of the whole pipeline. Running it as its own job means you can stop it,
-- restart it, or point it at a different model without touching retrieval -- and you can watch
-- retrieval keep working while it is down.
--
-- One chat call per row of alarm-context. See docker-compose.yml for the concurrency and
-- timeout settings; they are set from measured failure, not taste.
--
-- Submit on its own with:  ./run.sh submit plan
-- =====================================================================

SET 'pipeline.name' = 'rag-2-plan';

INSERT INTO work_orders
SELECT
    alarm_id, asset_id, line_id, fault_code, alarm_text, matched_section, doc_ref, category,
    severity, score, match_confidence, priority, queue, sla_due,
    t_alarm, t_context, LOCALTIMESTAMP AS t_action,
    `output` AS ai_work_order
FROM ML_PREDICT(
    TABLE alarm_context,
    MODEL ai_work_order_planner,
    DESCRIPTOR(planner_input)
);
