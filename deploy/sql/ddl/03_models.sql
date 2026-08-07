-- =====================================================================
-- Models: embedding and the work-order planner
--
-- DDL only. Loaded into EVERY job's session with `sql-client.sh -i`, because Flink's default
-- catalog is in-memory and per-session: a table created by one submission is invisible to the
-- next. Nothing here starts a job -- CREATE TABLE and CREATE MODEL are metadata.
--
-- Rendered by /opt/submit.sh; placeholders come from .env so no key is written to disk.
-- =====================================================================

-- ------------------------------------------------------------- models
CREATE MODEL embedding_model
INPUT  (`input`  STRING)
OUTPUT (`output` ARRAY<FLOAT>)
WITH (
    'provider' = 'openai',
    'endpoint' = '${OPENAI_BASE}/embeddings',
    'api-key'  = '${OPENAI_API_KEY}',
    'model'    = '${EMBED_MODEL}'
);

-- The grounding guard in the last sentence is deliberate: without it the model happily invents
-- a plausible part number when retrieval misses, which is exactly the failure a RAG demo needs
-- to be seen NOT doing.
CREATE MODEL ai_work_order_planner
INPUT  (`input`  STRING)
OUTPUT (`output` STRING)
WITH (
    'provider'      = 'openai',
    'endpoint'      = '${OPENAI_BASE}/chat/completions',
    'api-key'       = '${OPENAI_API_KEY}',
    'model'         = '${CHAT_MODEL}',
    'system-prompt' = 'You are a maintenance planner in a manufacturing plant. You are given a machine alarm with its telemetry, and one section of the service manual retrieved for it. Return a JSON object with exactly these fields: {"diagnosis": "one sentence naming the likely fault", "action": "the corrective step, using the manual wording", "parts": ["part numbers named in the manual section, empty if none"], "safety": "any lockout, isolation or do-not-do warning from the manual, or none", "estimated_minutes": integer, "line_stop_likely": true or false}. Use only the manual section provided. If it does not cover the alarm, set diagnosis to "insufficient documentation", leave parts empty and do not guess a part number. Return only the raw JSON object. Do not wrap it in markdown code fences and do not add any explanation.'
);
