-- =====================================================================
-- JOB 1 of 4: retrieve
--
--   machine_alarms -> ML_PREDICT(embed) -> VECTOR_SEARCH(Milvus) -> enrich -> alarm-context
--
-- This is the job worth explaining. Everything else moves rows around; this one turns an
-- operator's sentence into a vector, finds the manual section that answers it, and derives the
-- planning fields from what came back.
--
-- Submit on its own with:  ./run.sh submit retrieve
-- =====================================================================

SET 'pipeline.name' = 'rag-1-retrieve';

-- VECTOR_SEARCH argument order, verified against the 2.3.0 planner (not documentation) with
-- './run.sh smoke':
--   (vector_table, DESCRIPTOR(index_column), input_table.vector_column, top_k [, CONFIG])
-- i.e. the DESCRIPTOR second and the QUERY VECTOR third, as FLIP-540 and the Alibaba docs
-- describe. Some Apache doc pages show the reverse; the shipped planner rejects that form
-- outright with "Cannot apply 'VECTOR_SEARCH' to arguments of type ...", which also prints
-- the supported signature. In a correlated LATERAL the failure is less obvious: the
-- correlation is silently lost and planning fails later with "There are not enough rules to
-- produce a node with desired properties".
--
-- The LATERAL must carry NO table alias. Writing `) AS ctx(id, content, category, vec, score)`
-- -- or even a bare `AS ctx` -- makes the 2.3.0 planner drop the correlation, so
-- PhysicalVectorSearchTableFunctionRule (which only matches FlinkLogicalCorrelate) never
-- fires, the Milvus scan is left with no physical implementation, and planning dies with
-- "There are not enough rules to produce a node with desired properties". Unaliased, the
-- search table's own column names (content, category, section, doc_ref, severity, ...) plus
-- `score` are in scope. Keep the left-hand subquery's alias `a`; only the lateral is affected.
--
-- The appended `score` column is DOUBLE, hence the CAST.
--
-- ENRICHMENT: severity comes from the retrieved manual section, criticality from the alarm
-- event. Priority is the product of the two -- i.e. the planning fields below are only
-- derivable BECAUSE of the retrieval. That is the point of the whole pipeline.
--
-- TOP_K is 1 on purpose. Job 2 runs ML_PREDICT once per row emitted here, so TOP_K = n
-- multiplies chat-model spend by n. Retrieving more context needs a windowed LISTAGG to
-- collapse the hits back to one row first, which is a different (and much bigger) pipeline.

INSERT INTO alarm_context
SELECT
    a.alarm_id,
    a.asset_id,
    a.line_id,
    a.fault_code,
    a.alarm_text,
    a.vibration_mms,
    a.temp_c,
    a.pressure_bar,
    a.criticality,
    a.shift,
    section,
    doc_ref,
    category,
    severity,
    CAST(score AS FLOAT),

    -- Thresholds are measured, not guessed, and they were RE-measured when the corpus grew
    -- from 14 sections to 314. At 14 the split was clean: correct matches 0.398-0.685, alarms
    -- the manual does not cover 0.158-0.315. At 314 every query finds something plausible, so
    -- the floor rose: covered 0.444-0.685, uncovered 0.414-0.470. The old 0.35 boundary sat
    -- below both and stopped separating anything -- every uncovered fault read as confident.
    --
    -- 0.47 is the best single cut over the measured data: it catches 10/10 uncovered faults and
    -- wrongly flags 3/35 covered ones as low. Erring toward asking a human is the right
    -- direction for the mistake to fall.
    --
    -- Re-run the exhaustive check after ANY corpus change. Absolute COSINE scores are a
    -- property of the corpus, not of the query.
    CASE
        WHEN score >= 0.55 THEN 'high'
        WHEN score >= 0.47 THEN 'medium'
        ELSE 'low'
    END AS match_confidence,

    -- A severity-4 manual section is a stop-the-line fault wherever it happens; below that,
    -- how much the asset matters decides how fast someone has to walk.
    CASE
        WHEN severity >= 4                                     THEN 'P1'
        WHEN severity = 3 AND a.criticality = 'line_critical'  THEN 'P1'
        WHEN severity = 3                                      THEN 'P2'
        WHEN severity = 2 AND a.criticality = 'line_critical'  THEN 'P2'
        WHEN severity = 2                                      THEN 'P3'
        ELSE 'P4'
    END AS priority,

    CASE category
        WHEN 'safety'     THEN 'safety-officer'
        WHEN 'electrical' THEN 'electrical-team'
        WHEN 'hydraulic'  THEN 'fluid-power-team'
        WHEN 'pneumatic'  THEN 'fluid-power-team'
        WHEN 'thermal'    THEN 'facilities-team'
        ELSE 'mechanical-team'
    END AS queue,

    CASE
        WHEN severity >= 4                                    THEN a.alarm_time + INTERVAL '1' HOUR
        WHEN severity = 3 AND a.criticality = 'line_critical' THEN a.alarm_time + INTERVAL '1' HOUR
        WHEN severity = 3                                     THEN a.alarm_time + INTERVAL '4' HOUR
        WHEN severity = 2 AND a.criticality = 'line_critical' THEN a.alarm_time + INTERVAL '4' HOUR
        WHEN severity = 2                                     THEN a.alarm_time + INTERVAL '24' HOUR
        ELSE a.alarm_time + INTERVAL '72' HOUR
    END AS sla_due,

    a.alarm_time AS t_alarm,
    -- Stamped as the row leaves stage 1, so the gap from t_alarm covers embedding, the Milvus
    -- round trip and the enrichment -- everything before the expensive planner call.
    LOCALTIMESTAMP AS t_context,

    -- The prompt the planner model sees. Telemetry is presented as labelled structured facts
    -- rather than folded into the embedded alarm text, so the model can reason over the
    -- numbers without those numbers having polluted the retrieval.
    CONCAT(
        'ALARM on ',      a.asset_id,                          '. ',
        'Report: ',       a.alarm_text,                        ' ',
        'Readings: vibration ', CAST(a.vibration_mms AS STRING), ' mm/s RMS, ',
        'temperature ',   CAST(a.temp_c AS STRING),            ' C, ',
        'air/hydraulic pressure ', CAST(a.pressure_bar AS STRING), ' bar. ',
        'Asset role: ',   a.criticality,                       '. ',
        'Shift: ',        a.shift,                             '. ',
        'MANUAL SECTION ', doc_ref, ' "', section, '": ', content
    ) AS planner_input
FROM (
    SELECT
        alarm_id, asset_id, line_id, fault_code, alarm_text, vibration_mms, temp_c, pressure_bar,
        criticality, shift, alarm_time,
        `output` AS alarm_vector
    FROM ML_PREDICT(
        TABLE machine_alarms,
        MODEL embedding_model,
        DESCRIPTOR(alarm_text)
    )
) AS a,
LATERAL TABLE(
    VECTOR_SEARCH(
        TABLE maintenance_manual,
        DESCRIPTOR(vec),
        a.alarm_vector,
        ${TOP_K}
    )
);
