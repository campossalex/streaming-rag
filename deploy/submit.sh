#!/usr/bin/env bash
# Submits the pipeline as FOUR separate Flink jobs, sharing one set of DDL.
#
#   /opt/submit.sh              submit all four, in order
#   /opt/submit.sh retrieve     submit one, by name
#
# Why four jobs rather than one STATEMENT SET
# -------------------------------------------
# It used to be a single job with eight INSERTs. That works, but it is hard to explain and
# harder to operate: one plan, one lifecycle, one failure. Split, each stage can be stopped,
# restarted, or re-pointed on its own -- you can take the expensive planner job down and watch
# retrieval carry on filling its topic.
#
# Why the DDL is passed with -i
# -----------------------------
# Flink's default catalog is in-memory and scoped to a SQL client session. A table created by
# one submission does not exist for the next, so every job would otherwise need its own copy of
# every CREATE TABLE. `sql-client.sh -i` loads init files into the session before running the
# -f script, which is exactly this problem. Every job gets every DDL file: they are metadata
# only, cost nothing, and this way a job can never fail because someone forgot to list a table.
set -euo pipefail

: "${OPENAI_API_KEY:?OPENAI_API_KEY is not set - copy .env.example to .env and fill it in}"

# The collection name has ONE source of truth: .env. The seeder reads MILVUS_COLLECTION and so
# does the rendered DDL, so the two cannot drift into "collection not found" at query time.
: "${MILVUS_COLLECTION:=maintenance_manual}"
export MILVUS_COLLECTION

# Postgres incident store. Defaults match docker-compose.yml so .env need not carry them, but
# .env wins if it does -- same precedence as every other setting here.
: "${PG_URL:=jdbc:postgresql://postgres:5432/incidents}"
: "${PG_USER:=root}"
: "${PG_PASSWORD:=admin1}"
export PG_URL PG_USER PG_PASSWORD

# Everything below is referenced unquoted by the banner and by envsubst. Under 'set -u' a
# single missing one aborts this script BEFORE the first submit_one call, so nothing reaches
# the cluster and the only clue is "line NNN: VAR: unbound variable" -- no mention of .env,
# and the Flink UI simply shows no jobs, which reads as "submit did nothing".
#
# TOP_K is 1 by design rather than a placeholder: job 2 calls ML_PREDICT once per row job 1
# emits, so a larger K multiplies chat-model spend by K. See jobs/10_retrieve.sql.
: "${TOP_K:=1}"
# These two default to the compose service names, same as the Postgres block above.
: "${MILVUS_ENDPOINT:=milvus-standalone}"
: "${KAFKA_BOOTSTRAP:=kafka:9092}"
export TOP_K MILVUS_ENDPOINT KAFKA_BOOTSTRAP

# The models get no default on purpose. A wrong endpoint or model name fails late, inside a
# running job, as a 404 the pipeline reports as a restart loop -- so say so here instead.
: "${OPENAI_BASE:?OPENAI_BASE is not set in .env - run './run.sh use-openai', 'use-ollama' or 'use-launchpad <url>'}"
: "${EMBED_MODEL:?EMBED_MODEL is not set in .env - run './run.sh use-openai', 'use-ollama' or 'use-launchpad <url>'}"
: "${CHAT_MODEL:?CHAT_MODEL is not set in .env - run './run.sh use-openai', 'use-ollama' or 'use-launchpad <url>'}"

readonly SQL_DIR=/opt/sql
readonly SUBST='$OPENAI_API_KEY $OPENAI_BASE $EMBED_MODEL $CHAT_MODEL $MILVUS_ENDPOINT $MILVUS_COLLECTION $KAFKA_BOOTSTRAP $TOP_K $PG_URL $PG_USER $PG_PASSWORD'

WORK="$(mktemp -d /tmp/pipeline.XXXXXX)"
# The SQL client appends every executed statement to a history file. CREATE MODEL carries the
# API key as a literal, so the default history persists the key inside the container. Give it a
# throwaway file and delete everything on exit.
HISTFILE_TMP="$(mktemp /tmp/sqlhist.XXXXXX)"
trap 'rm -rf "$WORK" "$HISTFILE_TMP"' EXIT

# Render every DDL and job file once, into a private directory.
mkdir -p "$WORK/ddl" "$WORK/jobs"
for f in "$SQL_DIR"/ddl/*.sql; do
    envsubst "$SUBST" < "$f" > "$WORK/ddl/$(basename "$f")"
done
for f in "$SQL_DIR"/jobs/*.sql; do
    envsubst "$SUBST" < "$f" > "$WORK/jobs/$(basename "$f")"
done

# -i takes ONE file. Not a comma-separated list -- passing one makes the client try to open the
# whole joined string as a single filename and die with FileNotFoundError naming all five paths.
# Concatenating in sorted order gives the same result with no ambiguity about ordering.
INIT="$WORK/init.sql"
for f in "$WORK"/ddl/*.sql; do
    printf -- '-- ===== %s =====\n' "$(basename "$f")" >> "$INIT"
    cat "$f" >> "$INIT"
    printf '\n' >> "$INIT"
done

# Job name -> file. Order matters on a cold start: a job whose source topic has no data yet
# still starts fine, but submitting in pipeline order makes the Flink UI read top to bottom.
job_file() {
    case "$1" in
        retrieve)  echo "$WORK/jobs/10_retrieve.sql" ;;
        plan)      echo "$WORK/jobs/20_plan.sql" ;;
        incidents) echo "$WORK/jobs/30_incidents.sql" ;;
        status)    echo "$WORK/jobs/40_status.sql" ;;
        *)         return 1 ;;
    esac
}

FAILED=""

submit_one() {
    local name="$1" file out
    if ! file="$(job_file "$name")"; then
        echo "unknown job '$name' (expected: retrieve, plan, incidents, status)" >&2
        FAILED="$FAILED $name"
        return
    fi

    echo
    echo "--- submitting job: ${name} ---"
    # The SQL client echoes each statement as it executes, which prints the rendered API key to
    # the terminal and into scrollback. Redact it from the stream.
    out="$(/opt/flink/bin/sql-client.sh -hist "$HISTFILE_TMP" -i "$INIT" -f "$file" 2>&1 \
            | sed -e "s|${OPENAI_API_KEY}|<redacted>|g")"
    printf '%s\n' "$out" | grep -vE '^(WARNING|[A-Z][a-z]{2} [0-9]{2}, [0-9]{4})' || true

    # Judge by evidence of a submitted job, not by exit status. sql-client exits 0 after
    # "[ERROR] Could not execute SQL statement", AND after an uncaught SqlClientException that
    # never reaches the SQL at all -- both were reported as success before this check existed.
    #
    # Match BOTH spellings. Older clients print "Job ID: <hex>"; Flink 2.3 prints
    # "Job has been submitted with JobID <hex>" -- no colon, no space. Grepping only the first
    # made this check useless against 2.3: it can never match, so every job looks failed even
    # when the dispatcher accepted it. Anchoring on the 32-hex id is what makes it evidence.
    if ! printf '%s\n' "$out" | grep -qE 'Job ID: *[0-9a-f]{32}|JobID [0-9a-f]{32}'; then
        echo "  !! ${name} did not produce a Job ID -- nothing was submitted" >&2
        FAILED="$FAILED $name"
    fi
}

echo "Submitting pipeline (models: embed=${EMBED_MODEL}, chat=${CHAT_MODEL}, top_k=${TOP_K})"

if [[ $# -gt 0 ]]; then
    for name in "$@"; do submit_one "$name"; done
else
    for name in retrieve plan incidents status; do submit_one "$name"; done
fi

echo
if [[ -n "$FAILED" ]]; then
    echo "FAILED to submit:${FAILED}" >&2
    exit 1
fi
echo "all requested jobs submitted"
