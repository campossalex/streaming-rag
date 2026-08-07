-- =====================================================================
-- The Milvus corpus (service manual)
--
-- DDL only. Loaded into EVERY job's session with `sql-client.sh -i`, because Flink's default
-- catalog is in-memory and per-session: a table created by one submission is invisible to the
-- next. Nothing here starts a job -- CREATE TABLE and CREATE MODEL are metadata.
--
-- Rendered by /opt/submit.sh; placeholders come from .env so no key is written to disk.
-- =====================================================================

-- ------------------------------------------- the Milvus corpus (service manual)
-- The camelCase option keys are supported on purpose: they match the VVP 3 / Alibaba
-- connectors so existing DDL is a drop-in.
--
-- Every column declared here is requested from Milvus as an output field and looked up BY NAME,
-- so these names must match deploy/seed/seed_milvus.py exactly. section, doc_ref and severity
-- are not embedded -- they ride along on the hit and drive the enrichment below.
CREATE TABLE maintenance_manual (
    id       BIGINT NOT NULL,
    content  STRING,
    category STRING,
    section  STRING,
    doc_ref  STRING,
    severity INT,
    vec      ARRAY<FLOAT>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'      = 'milvus',
    'endpoint'       = '${MILVUS_ENDPOINT}',
    'port'           = '19530',
    'databaseName'   = 'default',
    'collectionName' = '${MILVUS_COLLECTION}',
    'search.metric'  = 'COSINE'
);
