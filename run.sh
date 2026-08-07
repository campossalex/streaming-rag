#!/usr/bin/env bash
#
# run.sh - one-command automation for the Flink 2.3 + Milvus streaming RAG stack.
#
#   ./run.sh all          build everything, start it, seed the corpus, submit the pipeline
#   ./run.sh help         list every command
#
# Docker is the only prerequisite. The connector is compiled inside a container, so no
# local JDK, Maven or Python is required.
#
set -euo pipefail

# --------------------------------------------------------------------------------------
# Locations and names. Change these only if you rename things in docker-compose.yml.
# --------------------------------------------------------------------------------------
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

readonly ENV_FILE=".env"
readonly ENV_TEMPLATE=".env.example"
readonly MILVUS_CONTAINER="milvus-standalone"
readonly KAFKA_CONTAINER="kafka"
# Where the connector must live inside the Flink containers. Anything in lib/ is loaded by
# the parent classloader at JVM start, which is what connector factory discovery needs.
readonly JAR_IN_CONTAINER="/opt/flink/lib/milvus-connector.jar"
readonly FLINK_IMAGE="flink-milvus:2.3.0"
readonly BUILDER_IMAGE="maven:3.9-eclipse-temurin-17"
readonly MAVEN_CACHE_VOLUME="flink-milvus-m2"
readonly FLINK_UI="http://localhost:8081"
readonly MILVUS_UI="http://localhost:9091/webui/"
readonly CONSOLE_UI="http://localhost:8090"
readonly ATTU_UI="http://localhost:8000"
readonly PLANNER_UI="http://localhost:9052"
readonly GRAFANA_UI="http://localhost:3000"
readonly ALARMS_IMAGE="milvus-alarms:1.0"
readonly PG_CONTAINER="postgres"

# --------------------------------------------------------------------------------------
# Output helpers
# --------------------------------------------------------------------------------------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
    C_RESET=$'\033[0m'; C_BLUE=$'\033[34m'; C_GREEN=$'\033[32m'
    C_YELLOW=$'\033[33m'; C_RED=$'\033[31m'; C_BOLD=$'\033[1m'
else
    C_RESET=""; C_BLUE=""; C_GREEN=""; C_YELLOW=""; C_RED=""; C_BOLD=""
fi

step() { printf '\n%s==> %s%s\n' "${C_BLUE}${C_BOLD}" "$*" "${C_RESET}"; }
ok()   { printf '%s  ok%s %s\n' "${C_GREEN}" "${C_RESET}" "$*"; }
info() { printf '     %s\n' "$*"; }
warn() { printf '%s  warning%s %s\n' "${C_YELLOW}" "${C_RESET}" "$*" >&2; }
die()  { printf '\n%s  error%s %s\n' "${C_RED}${C_BOLD}" "${C_RESET}" "$*" >&2; exit 1; }

# --------------------------------------------------------------------------------------
# Preflight
# --------------------------------------------------------------------------------------
compose() { docker compose "$@"; }

# A flattened download (files saved individually instead of extracting the archive) is the
# most common setup failure: the build fails with an lstat error on a missing directory,
# which does not point at the cause. Check the tree before touching Docker.
readonly REQUIRED_PATHS=(
    "docker-compose.yml"
    "pom.xml"
    "deploy/Dockerfile"
    "deploy/submit.sh"
    "deploy/sql/ddl/01_sources.sql"
    "deploy/sql/jobs/10_retrieve.sql"
    "deploy/sql/inspect.sql"
    "deploy/seed/Dockerfile"
    "deploy/seed/seed_milvus.py"
    "deploy/seed/requirements.txt"
    "deploy/datagen/Dockerfile"
    "deploy/datagen/alarms.py"
    "deploy/datagen/requirements.txt"
    "deploy/postgres/init.sql"
    "deploy/planner/app.py"
    "deploy/grafana/dashboards/incidents.json"
    "src/main/java/org/apache/flink/connector/milvus/MilvusDynamicTableFactory.java"
    "src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory"
    "$ENV_TEMPLATE"
)

check_project_layout() {
    local missing=()
    local path
    for path in "${REQUIRED_PATHS[@]}"; do
        [[ -e "$path" ]] || missing+=("$path")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        printf '\n%s  error%s the project tree is incomplete. Missing:\n' "${C_RED}${C_BOLD}" "${C_RESET}" >&2
        printf '       %s\n' "${missing[@]}" >&2
        cat >&2 <<'HINT'

       This usually means the files were downloaded individually rather than
       extracted from the archive, which flattens the directory structure.
       Extract the tarball and run from the directory it creates:

         tar xzf flink-connector-milvus.tar.gz
         cd flink-connector-milvus
         chmod +x run.sh
         ./run.sh all

HINT
        exit 1
    fi
    ok "project layout complete ($(find src -name '*.java' | wc -l | tr -d ' ') java sources)"
}

cmd_preflight() {
    step "Checking prerequisites"

    check_project_layout

    command -v docker >/dev/null 2>&1 || die "docker is not installed. See https://docs.docker.com/get-docker/"
    docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable. Start Docker Desktop, or add yourself to the docker group."
    docker compose version >/dev/null 2>&1 || die "Docker Compose V2 is required ('docker compose', not 'docker-compose'). See https://docs.docker.com/compose/"
    ok "docker $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo '?'), compose $(docker compose version --short 2>/dev/null || echo '?')"

    # Milvus alone wants ~2 GB of images; the whole stack plus the Maven cache needs more.
    local avail
    avail=$(df -Pk . | awk 'NR==2 {print int($4/1024/1024)}')
    if [[ "${avail:-0}" -lt 12 ]]; then
        warn "only ${avail} GiB free on this filesystem; the images and Maven cache want roughly 12 GiB"
    else
        ok "${avail} GiB disk available"
    fi

    # Docker Desktop's default memory allocation is often too small for Milvus plus Flink
    # plus Kafka. The symptom is an OOM-killed Milvus, which surfaces as a healthcheck
    # timeout and looks like a Milvus bug rather than a resource limit.
    local mem_bytes mem_gib
    mem_bytes=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
    mem_gib=$(( mem_bytes / 1024 / 1024 / 1024 ))
    if [[ "$mem_gib" -gt 0 && "$mem_gib" -lt 8 ]]; then
        warn "Docker has only ${mem_gib} GiB of memory; this stack wants 8 GiB or more"
        info "Docker Desktop: Settings -> Resources -> Memory"
    elif [[ "$mem_gib" -gt 0 ]]; then
        ok "${mem_gib} GiB of memory available to Docker"
    fi

    # Port collisions are the most common silent failure: compose reports a cryptic bind error.
    local busy=()
    local p
    for p in 8081 19530 9091 9092 9000 9001 8090 8000 5432 3000 9052; do
        if (exec 3<>"/dev/tcp/127.0.0.1/${p}") 2>/dev/null; then
            exec 3>&- 2>/dev/null || true
            busy+=("$p")
        fi
    done
    if [[ ${#busy[@]} -gt 0 ]]; then
        warn "these ports are already in use: ${busy[*]}"
        info "either stop whatever owns them, or remap the 'ports:' entries in docker-compose.yml"
    else
        ok "ports 8081, 19530, 9091, 9092, 9000, 9001, 8090, 8000, 5432, 3000, 9052 are free"
    fi
}

# --------------------------------------------------------------------------------------
# Configuration: this is where the OpenAI token goes.
# --------------------------------------------------------------------------------------
cmd_config() {
    step "Configuring ${ENV_FILE}"

    if [[ ! -f "$ENV_FILE" ]]; then
        [[ -f "$ENV_TEMPLATE" ]] || die "${ENV_TEMPLATE} is missing; are you in the project root?"
        cp "$ENV_TEMPLATE" "$ENV_FILE"
        chmod 600 "$ENV_FILE"
        ok "created ${ENV_FILE} from the template (mode 600)"
    else
        info "${ENV_FILE} already exists, leaving it alone"
    fi

    # An inherited environment variable wins, which is how CI should supply the key.
    if [[ -n "${OPENAI_API_KEY:-}" ]]; then
        set_env_var OPENAI_API_KEY "$OPENAI_API_KEY"
        ok "took OPENAI_API_KEY from the environment"
    fi

    local current
    current=$(read_env_var OPENAI_API_KEY)

    if [[ -n "$current" && "$current" != "sk-replace-me" ]]; then
        validate_openai_key "$current" "$(read_env_var OPENAI_BASE)" \
            || die "the key in ${ENV_FILE} is not usable. Fix it with './run.sh set-key'."
    fi

    if [[ "$current" == "sk-replace-me" || -z "$current" ]]; then
        if [[ "${NON_INTERACTIVE:-0}" == "1" ]]; then
            die "OPENAI_API_KEY is not set. Either export OPENAI_API_KEY, or edit ${ENV_FILE}, or run './run.sh use-ollama' for a fully local setup with no key."
        fi
        printf '\n  The pipeline calls an embedding model and a chat model.\n'
        printf '  Paste an OpenAI API key, or press Enter to switch to local models via Ollama.\n'
        printf '  Key (not echoed): '
        local entered=""
        read -rs entered || true
        printf '\n'
        if [[ -z "$entered" ]]; then
            cmd_use_ollama
            return
        fi
        # Invisible trailing whitespace from a paste is a silent 401.
        entered="${entered//[$' \t\r\n']/}"
        if ! validate_openai_key "$entered" "$(read_env_var OPENAI_BASE)"; then
            die "that key was not accepted, so it has not been stored. Re-run './run.sh set-key'."
        fi
        set_env_var OPENAI_API_KEY "$entered"
        chmod 600 "$ENV_FILE"
        ok "stored the key in ${ENV_FILE} (mode 600, gitignored)"
    else
        ok "OPENAI_API_KEY is set (${current:0:7}...)"
    fi

    info "embedding model: $(read_env_var EMBED_MODEL)  dim: $(read_env_var EMBED_DIM)"
    info "chat model:      $(read_env_var CHAT_MODEL)"
    info "endpoint:        $(read_env_var OPENAI_BASE)"
}

read_env_var() {
    [[ -f "$ENV_FILE" ]] || { echo ""; return; }
    grep -E "^${1}=" "$ENV_FILE" | tail -1 | cut -d= -f2- || true
}

set_env_var() {
    local key="$1" value="$2"
    if grep -qE "^${key}=" "$ENV_FILE"; then
        # A key can contain / and &, so use a delimiter that cannot appear in it.
        local tmp
        tmp=$(mktemp)
        awk -v k="$key" -v v="$value" -F= '
            $1 == k && !done { print k "=" v; done=1; next }
            { print }
        ' "$ENV_FILE" > "$tmp"
        mv "$tmp" "$ENV_FILE"
    else
        printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
    fi
    chmod 600 "$ENV_FILE"
}

# A wrong key otherwise surfaces minutes later as a traceback from the seeder. Check the
# shape, then check it against the API, at the moment it is entered.
validate_openai_key() {
    local key="$1" base="$2"

    if [[ "$base" == *"api.openai.com"* && "$key" != sk-* ]]; then
        warn "this key does not start with 'sk-', so it is not an OpenAI API key"
        info "OpenAI keys look like sk-proj-... or sk-..."
        info "a common cause: double-clicking the key on screen selects only one"
        info "hyphen-delimited segment, so 'sk-proj-ABC...' copies as just 'ABC...'."
        info "Use the copy button on https://platform.openai.com/api-keys instead."
        return 1
    fi

    command -v curl >/dev/null 2>&1 || { info "curl not available; skipping the live key check"; return 0; }

    local code
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 15 \
              -H "Authorization: Bearer ${key}" "${base}/models" 2>/dev/null || echo "000")
    case "$code" in
        200) ok "the API accepted the key" ; return 0 ;;
        401) warn "the API rejected the key with 401 Unauthorized" ; return 1 ;;
        429) ok "key is valid (429 rate limited, which still means authenticated)" ; return 0 ;;
        000) info "could not reach ${base}; skipping the live key check" ; return 0 ;;
        *)   warn "unexpected HTTP ${code} from ${base}/models" ; return 0 ;;
    esac
}

cmd_check_key() {
    step "Checking the configured API key"
    local key base
    key=$(read_env_var OPENAI_API_KEY)
    base=$(read_env_var OPENAI_BASE)
    [[ -n "$key" ]] || die "OPENAI_API_KEY is not set in ${ENV_FILE}"
    info "key ${key:0:8}... (${#key} chars) against ${base}"
    if validate_openai_key "$key" "$base"; then
        ok "key is usable"
    else
        die "fix the key with './run.sh set-key', then re-run './run.sh seed'"
    fi
}

# Prompts for a key and stores it, without going through the whole config flow.
cmd_set_key() {
    step "Setting the API key"
    [[ -f "$ENV_FILE" ]] || cp "$ENV_TEMPLATE" "$ENV_FILE"
    printf '  Paste the key (not echoed): '
    local entered=""
    read -rs entered || true
    printf '\n'
    [[ -n "$entered" ]] || die "nothing entered"
    # Strip stray whitespace from the paste; a trailing space or CR is invisible and fatal.
    entered="${entered//[$' \t\r\n']/}"
    local base
    base=$(read_env_var OPENAI_BASE); base=${base:-https://api.openai.com/v1}
    validate_openai_key "$entered" "$base" \
        || die "not storing a key the API rejects. Re-run './run.sh set-key' with the full key."
    set_env_var OPENAI_API_KEY "$entered"
    ok "stored in ${ENV_FILE} (mode 600)"
    info "the containers read ${ENV_FILE} at start, so re-run './run.sh seed' now"
}

cmd_use_ollama() {
    step "Switching to local models (Ollama)"
    set_env_var OPENAI_API_KEY "ollama"
    set_env_var OPENAI_BASE "http://ollama:11434/v1"
    set_env_var EMBED_MODEL "nomic-embed-text"
    set_env_var EMBED_DIM "768"
    set_env_var CHAT_MODEL "qwen2.5:3b-instruct"
    ok "${ENV_FILE} points at Ollama; no API key and no outbound traffic"

    compose --profile local-llm up -d ollama
    info "pulling models (a few GB, one time)"
    compose exec -T ollama ollama pull nomic-embed-text
    compose exec -T ollama ollama pull qwen2.5:3b-instruct
    ok "models pulled"
    warn "EMBED_DIM changed to 768, so the Milvus collection must be rebuilt: ./run.sh seed"
}

# --------------------------------------------------------------------------------------
# Build: stage 1 of deploy/Dockerfile compiles the connector, stage 2 bakes it into Flink.
# --------------------------------------------------------------------------------------
cmd_build() {
    step "Building the connector and the Flink image"
    info "stage 1 runs Maven in a container; stage 2 copies the shaded jar into /opt/flink/lib"
    # Only jobmanager carries the build definition; taskmanager reuses the same tag.
    # Building the whole project would ask buildx to export one tag from two targets.
    compose build jobmanager
    docker image inspect "$FLINK_IMAGE" >/dev/null 2>&1 \
        || die "the build reported success but ${FLINK_IMAGE} does not exist locally. Try 'docker compose build --no-cache jobmanager'."
    # The seeder is in the 'manual' profile, so a plain 'compose build' skips it and
    # 'compose run seeder' only builds when the tag is absent. Without this, edits to
    # deploy/seed/ are silently ignored for as long as an old milvus-seeder:1.0 exists.
    compose --profile manual build seeder
    # The alarm generator and the planner UI are ordinary services, but 'compose up' reuses an
    # existing image rather than rebuilding, so edits to their sources need this too.
    compose build alarms planner
    ok "${FLINK_IMAGE} built, with the connector at ${JAR_IN_CONTAINER}"
}

# --------------------------------------------------------------------------------------
# Bring the stack up and wait for it to be genuinely ready.
# --------------------------------------------------------------------------------------
wait_for_health() {
    local container="$1" timeout="${2:-180}" waited=0 status
    printf '     waiting for %s to report healthy ' "$container"
    while true; do
        status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
                    "$container" 2>/dev/null || echo "missing")
        case "$status" in
            healthy) printf ' ok\n'; return 0 ;;
            missing) printf '\n'; die "container ${container} does not exist. Check './run.sh ps' and './run.sh logs'." ;;
            none)    printf '\n'; warn "${container} declares no healthcheck; continuing"; return 0 ;;
        esac
        if [[ $waited -ge $timeout ]]; then
            printf '\n'
            docker logs --tail 40 "$container" 2>&1 | sed 's/^/       /' || true
            die "${container} was not healthy after ${timeout}s (last status: ${status})"
        fi
        printf '.'
        sleep 5
        waited=$((waited + 5))
    done
}

# compose reports "dependency failed to start: container X exited (1)" and nothing else.
# The reason is always in that container's log, so print it rather than making the user hunt.
dump_broken_containers() {
    local svc state
    for svc in $(compose config --services 2>/dev/null); do
        state=$(compose ps -a --status exited --status dead --format '{{.Service}}' 2>/dev/null \
                    | grep -Fx "$svc" || true)
        [[ -n "$state" ]] || continue
        printf '\n%s----- last 40 log lines from %s -----%s\n' "${C_YELLOW}" "$svc" "${C_RESET}" >&2
        compose logs --no-color --tail 40 "$svc" 2>&1 | sed 's/^/  /' >&2
    done
}

cmd_up() {
    step "Starting Milvus, Kafka and Flink"
    # taskmanager uses pull_policy: never, so a missing image would otherwise surface as an
    # opaque container-create failure.
    docker image inspect "$FLINK_IMAGE" >/dev/null 2>&1 \
        || die "${FLINK_IMAGE} does not exist yet. Run './run.sh build' first."

    if ! compose up -d; then
        dump_broken_containers
        die "one or more containers failed to start. The log above shows why. Full logs: './run.sh logs <service>'."
    fi
    # Milvus has a 90s start_period, so this is the long one.
    wait_for_health "$MILVUS_CONTAINER" 240
    wait_for_health "$KAFKA_CONTAINER" 120

    printf '     waiting for the Flink REST endpoint '
    local waited=0
    until compose exec -T jobmanager curl -sf http://localhost:8081/overview >/dev/null 2>&1; do
        if [[ $waited -ge 120 ]]; then
            printf '\n'
            compose logs --tail 40 jobmanager | sed 's/^/       /'
            die "the Flink JobManager REST API never came up"
        fi
        printf '.'; sleep 5; waited=$((waited + 5))
    done
    printf ' ok\n'

    # Both pipeline stages are submitted together, so the second stage's Kafka *source* starts
    # before the first stage's sink has produced anything. Broker-side auto-creation does not
    # help: the consumer asks for metadata first and the job dies with
    # "Failed to get metadata for topics [alarm-context] / UnknownTopicOrPartition", then
    # restart-loops. Creating them up front is the fix. Idempotent, so 'up' stays re-runnable.
    printf '     creating Kafka topics '
    local topic
    for topic in machine_alarms alarm-context work-orders; do
        compose exec -T "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka:9092 --create --if-not-exists \
            --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null 2>&1 \
            || warn "could not create topic ${topic}; the pipeline may restart-loop on it"
        printf '.'
    done
    printf ' ok\n'

    ok "stack is up"
    info "Flink UI    ${FLINK_UI}"
    info "Milvus UI   ${MILVUS_UI}"
    info "Incidents   ${PLANNER_UI}"
    info "Dashboards  ${GRAFANA_UI}"

    # Always re-seed, so a run never depends on what the previous one left in Milvus.
    #
    # Milvus persists collection *data* in ./volumes but does not restore the in-memory
    # load state on boot, and a search against an unloaded -- or half-wiped -- collection
    # fails at runtime, not at submit: the pipeline restart-loops on "collection not found"
    # long after 'up' reported success. Seeding here drops, recreates, inserts, flushes and
    # loads, so the collection is in a known-good state whatever happened before.
    #
    # Set SKIP_SEED=1 to keep an existing collection (e.g. one you have hand-modified).
    if [[ "${SKIP_SEED:-0}" == "1" ]]; then
        warn "SKIP_SEED=1, so the corpus is whatever the last run left behind"
        info "if searches fail with 'collection not found' or return nothing, run './run.sh seed'"
        return 0
    fi
    cmd_seed
}

# --------------------------------------------------------------------------------------
# Corpus bootstrap
# --------------------------------------------------------------------------------------
cmd_seed() {
    step "Bootstrapping the corpus in Milvus"
    info "drops any existing collection, re-embeds the documents, creates a COSINE HNSW index,"
    info "then loads the collection into memory -- so this never inherits a previous run's state"
    compose run --rm seeder
    ok "corpus loaded; Milvus will now serve searches"
}

# --------------------------------------------------------------------------------------
# Loading a freshly built jar without rebuilding the whole image.
# --------------------------------------------------------------------------------------
cmd_reload_jar() {
    step "Rebuilding the connector and hot-loading it"

    info "compiling with Maven in a container (cache volume ${MAVEN_CACHE_VOLUME})"
    docker run --rm \
        -v "$PWD":/build \
        -v "${MAVEN_CACHE_VOLUME}":/root/.m2 \
        -w /build \
        "$BUILDER_IMAGE" \
        mvn -B -q clean package -DskipTests

    local jar
    jar=$(find target -maxdepth 1 -name 'flink-connector-milvus-*.jar' ! -name 'original-*' | head -1)
    [[ -n "$jar" ]] || die "the build produced no jar; run './run.sh build' to see the full Maven output"
    info "built $(basename "$jar") ($(du -h "$jar" | cut -f1))"

    assert_jar_is_sane "$jar"

    # docker cp preserves the source mode. Under a umask of 077 the jar would be 0600 and
    # unreadable to the container's flink user, so the connector would silently not load.
    chmod 644 "$jar"

    # The jar must exist on the JobManager (planning and factory discovery) and on every
    # TaskManager (the search function actually runs there).
    local copied=0 cid
    for svc in jobmanager taskmanager; do
        while read -r cid; do
            [[ -n "$cid" ]] || continue
            docker cp "$jar" "${cid}:${JAR_IN_CONTAINER}"
            info "copied into ${svc} (${cid:0:12})"
            copied=$((copied + 1))
        done < <(compose ps -q "$svc")
    done
    [[ $copied -gt 0 ]] || die "no running Flink containers to copy into. Run './run.sh up' first."

    # /opt/flink/lib is scanned once, at JVM start. Resubmitting a job is not enough.
    info "restarting the Flink containers so the new jar is picked up"
    compose restart jobmanager taskmanager >/dev/null
    wait_for_health "$MILVUS_CONTAINER" 60 >/dev/null 2>&1 || true
    sleep 5
    ok "connector reloaded on ${copied} container(s)"
    warn "any running job was killed by the restart; resubmit with './run.sh submit'"
}

# Catch the packaging mistakes that produce a jar which deploys fine and does nothing.
assert_jar_is_sane() {
    local jar="$1"
    local classes spi leaks
    classes=$(unzip -l "$jar" | awk '{print $4}' | grep -cE '^org/apache/flink/connector/milvus/[A-Z]' || true)
    spi=$(unzip -p "$jar" META-INF/services/org.apache.flink.table.factories.Factory 2>/dev/null || true)
    leaks=$(unzip -l "$jar" | awk '{print $4}' | grep -cE '^(io/grpc|io/netty|com/google/protobuf|com/google/common)/' || true)

    [[ "$classes" -ge 8 ]] || die "the jar contains ${classes} connector classes; expected 8 or more. A groupId that matches an artifactSet exclude in pom.xml silently drops them."
    [[ -n "$spi" ]] || die "the jar has no META-INF/services/org.apache.flink.table.factories.Factory entry, so Flink will never discover 'connector' = 'milvus'."
    [[ "$leaks" -eq 0 ]] || die "${leaks} unrelocated gRPC/Netty/protobuf entries found; fix the shade relocations before deploying."
    ok "jar looks sane: ${classes} classes, SPI present, no unrelocated dependencies"
}

# --------------------------------------------------------------------------------------
# Submit and verify
# --------------------------------------------------------------------------------------
cmd_submit() {
    step "Submitting the streaming RAG pipeline"
    # submit previously bypassed key validation, so a key already known to be bad still
    # reached the API and failed mid-pipeline.
    local key base
    key=$(read_env_var OPENAI_API_KEY)
    base=$(read_env_var OPENAI_BASE)
    if [[ -n "$key" && "$base" == *"api.openai.com"* ]]; then
        validate_openai_key "$key" "$base" \
            || die "the key in ${ENV_FILE} is not usable, so the pipeline would fail at the first model call. Fix it with './run.sh set-key'."
    fi
    info "the SQL is rendered inside the container so the API key never hits disk"
    if [[ $# -gt 0 ]]; then
        info "submitting only: $*"
    else
        info "four jobs: retrieve -> plan -> incidents -> status"
    fi

    # Judge by BOTH the exit status and the output. This check has been wrong twice: the SQL
    # client exits 0 after "[ERROR] Could not execute SQL statement", and it also exits 0 after
    # an uncaught SqlClientException that never reaches the SQL at all. submit.sh now returns
    # non-zero when a job produced no Job ID, which is the only reliable evidence of success.
    local out rc=0
    out=$(compose exec -T jobmanager /opt/submit.sh "$@" 2>&1) || rc=$?

    if [[ $rc -ne 0 ]] || grep -qE '\[ERROR\]|Exception in thread|did not produce a Job ID' <<<"$out"; then
        printf '%s\n' "$out" \
            | grep -E '\[ERROR\]|Exception|Caused by|did not produce a Job ID|FAILED to submit' \
            | head -20 >&2
        printf '\n' >&2
        die "at least one job was not submitted. 'CannotPlanException / not enough rules' usually means the correlated VECTOR_SEARCH lost its correlation -- the LATERAL must carry no table alias, and DESCRIPTOR must be the second argument. './run.sh smoke' isolates the connector from the models. Submit one job at a time to narrow it down: './run.sh submit retrieve'."
    fi

    printf '%s\n' "$out" | grep -E '^--- submitting job|^all requested jobs' | sed 's/^/     /'
    ok "submitted; watch the jobs at ${FLINK_UI}"
    info "tail the results with './run.sh consume'"
}

# Exercises the connector on its own, with a constant probe vector and no models involved.
# If this passes, the jar is loaded, Milvus is reachable and the collection is searchable.
cmd_smoke() {
    step "Smoke-testing VECTOR_SEARCH against Milvus"
    local dim endpoint collection probe
    dim=$(read_env_var EMBED_DIM); dim=${dim:-1536}
    endpoint=$(read_env_var MILVUS_ENDPOINT); endpoint=${endpoint:-milvus-standalone}
    # Same source of truth as the seeder and the rendered pipeline DDL: .env.
    collection=$(read_env_var MILVUS_COLLECTION); collection=${collection:-maintenance_manual}
    info "probe vector of ${dim} dimensions against collection ${collection}"

    probe=$(awk -v n="$dim" 'BEGIN { for (i = 0; i < n; i++) printf "%s%s", (i ? "," : ""), "0.01" }')

    local sql
    sql=$(mktemp)
    # No RETURN trap here. Bash keeps a RETURN trap installed after the function that set it
    # returns, so it fires again on the next function return -- including main's -- where $sql
    # is out of scope and 'set -u' turns it into "sql: unbound variable" after a successful run.
    cat > "$sql" <<SQL
SET 'sql-client.execution.result-mode' = 'tableau';
-- Every column here is requested from Milvus as an output field and resolved BY NAME, so this
-- DDL doubles as a check that the collection really carries the enrichment fields the pipeline
-- expects. Drop one from seed_milvus.py and this is where it surfaces.
CREATE TABLE smoke_corpus (
    id BIGINT NOT NULL, content STRING, category STRING,
    section STRING, doc_ref STRING, severity INT, vec ARRAY<FLOAT>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'milvus',
    'endpoint' = '${endpoint}',
    'port' = '19530',
    'databaseName' = 'default',
    'collectionName' = '${collection}',
    'search.metric' = 'COSINE'
);
-- Argument order: vector table, DESCRIPTOR(index column), query vector, top_k. Verified
-- against the 2.3.0 planner, which rejects the reverse with "Cannot apply 'VECTOR_SEARCH'".
-- The CAST is required: an ARRAY[0.01, ...] literal types as DECIMAL ARRAY, not FLOAT ARRAY.
-- LATERAL is not required when the call has no correlation with another table.
SELECT doc_ref, section, category, severity, score
FROM VECTOR_SEARCH(TABLE smoke_corpus, DESCRIPTOR(vec), CAST(ARRAY[${probe}] AS ARRAY<FLOAT>), 3);
SQL
    # Do NOT use docker cp here. It gives the copied file root ownership and preserves the
    # host mode, and mktemp creates 0600 files, so the container's unprivileged flink user
    # gets "Permission denied" on read. Streaming through stdin makes the container user
    # create the file itself.
    #
    # A unique name per run matters too: a root-owned 0600 /tmp/smoke.sql left behind by an
    # older revision of this script cannot be overwritten by the flink user either, which
    # turns a stale artefact into a permanent failure.
    local remote
    remote="/tmp/smoke-$$-$(date +%s).sql"

    # Best effort cleanup of the file older revisions left behind. Ignore failures.
    compose exec -T -u root jobmanager rm -f /tmp/smoke.sql >/dev/null 2>&1 || true

    if ! compose exec -T jobmanager sh -c "cat > '$remote'" < "$sql"; then
        die "could not write the smoke test into the jobmanager container. Is it running? './run.sh ps'"
    fi

    # The exit status is useless here: sql-client.sh -f prints "[ERROR] Could not execute SQL
    # statement" and still exits 0. Judge the run by its output, or a broken query reports ok.
    local out rows
    out=$(compose exec -T jobmanager ./bin/sql-client.sh -f "$remote" 2>&1) || true
    compose exec -T jobmanager rm -f "$remote" >/dev/null 2>&1 || true
    rm -f "$sql"

    if grep -q '\[ERROR\]' <<<"$out"; then
        printf '%s\n' "$out" | grep -A3 '\[ERROR\]' | head -20 >&2
        die "the smoke test failed. 'Could not find any factory for identifier milvus' means the jar is not on the classpath: './run.sh verify'. 'collection not loaded' or an empty result means the corpus is missing: './run.sh seed'. A dimension error means EMBED_DIM in .env disagrees with the collection."
    fi

    rows=$(sed -n 's/^Received a total of \([0-9]*\) row.*/\1/p' <<<"$out" | tail -1)
    rows=${rows:-0}
    if [[ "$rows" -lt 1 ]]; then
        die "the smoke test ran but Milvus returned 0 rows. The collection is empty or not loaded: './run.sh seed'."
    fi
    printf '%s\n' "$out" | sed -n '/^+----/,/^Received a total/p'
    ok "the connector returned ${rows} rows from Milvus"
}

cmd_verify() {
    step "Verifying the deployment"

    local cid svc found=0
    for svc in jobmanager taskmanager; do
        while read -r cid; do
            [[ -n "$cid" ]] || continue
            found=$((found + 1))
            if docker exec "$cid" test -f "$JAR_IN_CONTAINER" 2>/dev/null; then
                # Counting classes needs unzip inside the container. Images built before
                # unzip was added to the Dockerfile do not have it, and the old code
                # appended its fallback to grep's output, producing "0\n0" and a bash
                # arithmetic syntax error rather than a usable message.
                if ! docker exec "$cid" sh -c 'command -v unzip >/dev/null 2>&1'; then
                    warn "${svc} (${cid:0:12}): jar present, but unzip is missing from this image so its contents cannot be checked"
                    info "rebuild to get an image that includes unzip: './run.sh build' then './run.sh up'"
                    continue
                fi
                local classes
                # tr strips any stray output so the arithmetic test always sees digits.
                classes=$(docker exec "$cid" sh -c \
                    "unzip -l '${JAR_IN_CONTAINER}' | grep -c 'connector/milvus/[A-Z]'" 2>/dev/null \
                    | tr -cd '0-9' || true)
                classes=${classes:-0}
                if [[ "$classes" -ge 8 ]]; then
                    ok "${svc} (${cid:0:12}): connector present, ${classes} classes"
                else
                    die "${svc} (${cid:0:12}): jar present but only ${classes} connector classes. Rebuild with './run.sh reload-jar'."
                fi
            else
                die "${svc} (${cid:0:12}): ${JAR_IN_CONTAINER} is missing. Run './run.sh build' then './run.sh up', or './run.sh reload-jar'."
            fi
        done < <(compose ps -q "$svc")
    done
    [[ $found -gt 0 ]] || die "no Flink containers running. Run './run.sh up'."

    # The OpenAI model provider is a separate artifact and its absence only shows up at the
    # first ML_PREDICT, long after submission appears to succeed.
    local jm
    jm=$(compose ps -q jobmanager | head -1)
    if [[ -n "$jm" ]]; then
        local missing=()
        for j in flink-model-openai.jar flink-sql-connector-kafka.jar; do
            docker exec "$jm" test -f "/opt/flink/lib/${j}" 2>/dev/null || missing+=("$j")
        done
        if [[ ${#missing[@]} -gt 0 ]]; then
            die "missing from /opt/flink/lib: ${missing[*]}. Rebuild the image with './run.sh build' then './run.sh up'."
        fi
        ok "model provider and Kafka connector present"
    fi

    if docker inspect -f '{{.State.Health.Status}}' "$MILVUS_CONTAINER" 2>/dev/null | grep -q healthy; then
        ok "Milvus is healthy"
    else
        warn "Milvus is not reporting healthy"
    fi
    ok "verification passed; run './run.sh smoke' to exercise retrieval end to end"
}

# --------------------------------------------------------------------------------------
# Observation and teardown
# --------------------------------------------------------------------------------------
cmd_consume() {
    step "Tailing the work-orders topic (Ctrl-C to stop)"
    compose exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 --topic work-orders --from-beginning
}

cmd_inspect() {
    step "Reading work-orders back through Flink SQL (Ctrl-C to stop)"
    compose exec jobmanager ./bin/sql-client.sh -f /opt/sql/inspect.sql
}

# Kafka topic browser. Redpanda Console is a client of the existing apache/kafka broker,
# so this starts a UI only -- the broker, the topics and the running pipeline are untouched.
# Both browser UIs live in the 'tools' compose profile, so neither starts with 'up'. They are
# started together because in practice you want both: Console shows what moved through Kafka,
# Attu shows the corpus those events were matched against.
#
# Attu is pinned at v2.5 while the server is v3.0.0. That looks wrong and is not -- the Attu
# release line trails Milvus (newest stable is v2.6, and only a v3.0.0-beta targets 3.0), and
# v2.5 browses a 3.0 collection fine. Confirmed against this stack; leave the pin alone unless
# it actually breaks.
wait_for_ui() {
    local name="$1" url="$2" service="$3" waited=0
    printf '     waiting for %s ' "$name"
    until curl -sf "$url" >/dev/null 2>&1; do
        if [[ $waited -ge 60 ]]; then
            printf '\n'
            compose logs --tail 20 "$service" | sed 's/^/       /'
            die "${name} did not come up. If its port is taken, remap it in docker-compose.yml."
        fi
        printf '.'; sleep 3; waited=$((waited + 3))
    done
    printf ' ok\n'
}

cmd_console() {
    step "Starting the browser UIs (Redpanda Console and Attu)"
    compose --profile tools up -d console attu

    wait_for_ui "the Kafka console" "$CONSOLE_UI" console
    wait_for_ui "Attu" "$ATTU_UI" attu

    ok "Kafka UI at ${CONSOLE_UI}"
    info "topics: machine_alarms (raw events), alarm-context (retrieval + enrichment), work-orders"
    ok "Milvus UI at ${ATTU_UI}"
    info "collection: $(read_env_var MILVUS_COLLECTION 2>/dev/null || echo maintenance_manual) -- browse the corpus and run a search by hand"
    info "stop both with: docker compose --profile tools stop console attu"
}

# Scores retrieval end to end against the generator's own ground truth. Costs nothing: it
# reads work orders the running pipeline has already produced, rather than embedding anything
# new. A pipeline that runs while matching bearing wear to the lubrication schedule is a worse
# demo than no pipeline, and liveness checks cannot catch that.
cmd_audit() {
    step "Auditing retrieval accuracy"
    local n="${1:-20}"
    info "two-sided: covered faults must retrieve their intended manual section, and faults"
    info "the manual does not cover must score low so they route to a human instead"

    local mapping
    mapping=$(docker run --rm "$ALARMS_IMAGE" python alarms.py --mapping 2>/dev/null) \
        || die "could not read the fault mapping from ${ALARMS_IMAGE}. Run './run.sh build'."

    local out
    out=$(compose exec -T "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server kafka:9092 --topic work-orders \
            --max-messages "$n" --timeout-ms 300000 2>/dev/null) || true

    printf '%s\n' "$out" | MAPPING="$mapping" python3 -c '
import json, os, sys

expect = json.loads(os.environ["MAPPING"])
rows = [json.loads(l) for l in sys.stdin if l.startswith("{")]
if not rows:
    sys.exit("no work orders on the topic. Is the pipeline running? ./run.sh ps")

header = "  %-14s%-11s%-11s%-8s%s" % ("FAULT", "RETRIEVED", "EXPECTED", "SCORE", "RESULT")
print("")
print(header)

miss = 0
covered = uncovered = 0
for r in rows:
    code = r["fault_code"]
    want = expect.get(code, "?")
    got = r["doc_ref"]
    conf = r.get("match_confidence", "")
    if want is None:
        # A fault the manual does not cover. Correct behaviour is not a particular doc_ref --
        # it is low confidence, so the incident routes to a human instead of getting a
        # confident answer about nothing. An uncovered fault scoring high is the worse bug.
        uncovered += 1
        hit = conf == "low"
        shown_want = "(no match)"
    else:
        covered += 1
        hit = want == got
        shown_want = want
    if not hit:
        miss += 1
    print("  %-14s%-11s%-11s%-8.3f%s" % (
        code, got, shown_want, r["score"],
        "ok" if hit else ("MISMATCH" if want else "SCORED %s, WANTED low" % conf)))

scores = [r["score"] for r in rows]
print("")
print("  %d/%d correct  (%d covered, %d uncovered)  score range %.3f to %.3f" % (
    len(rows) - miss, len(rows), covered, uncovered, min(scores), max(scores)))
sys.exit(1 if miss else 0)
' || die "retrieval is not landing on the right manual sections. Re-check the corpus wording, and remember COSINE thresholds in the pipeline are calibrated to the current corpus."

    ok "every alarm retrieved its intended manual section"
}

# The raw generator output, before any Flink involvement. First thing to check when the
# pipeline produces nothing: no alarms here means the problem is the generator or Kafka,
# not the connector or the models.
cmd_alarms() {
    step "Tailing the machine_alarms topic (Ctrl-C to stop)"
    info "this is the raw generator output, before embedding or retrieval"
    compose exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 --topic machine_alarms
}

# Direct access to the incident store. Everything the UI and the dashboards show is a query
# away, and this is the fastest way to check whether a problem is in the pipeline or the display.
cmd_psql() {
    if [[ $# -gt 0 ]]; then
        compose exec -T "$PG_CONTAINER" psql -U root -d incidents "$@"
    else
        compose exec "$PG_CONTAINER" psql -U root -d incidents
    fi
}

# The incident-store equivalent of './run.sh smoke': one screen that says whether the pipeline is
# populating Postgres, whether humans are being asked for the right things, and how slow it is.
cmd_incidents() {
    step "Incident store summary"
    cmd_psql -c "SELECT status, count(*) FROM incident_current GROUP BY 1 ORDER BY 2 DESC;" \
             -c "SELECT count(*) FILTER (WHERE needs_human) AS needs_human,
                        count(*) AS total,
                        round(100.0 * count(*) FILTER (WHERE needs_human) / NULLIF(count(*),0), 1) AS pct
                 FROM incident_current;" \
             -c "SELECT round(avg(retrieval_lag_ms)) AS embed_retrieve_ms,
                        round(avg(planning_lag_ms))  AS planner_ms,
                        percentile_cont(0.5)  WITHIN GROUP (ORDER BY total_lag_ms)::int AS p50_ms,
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY total_lag_ms)::int AS p95_ms
                 FROM incident WHERE total_lag_ms IS NOT NULL;"
    info "planner UI ${PLANNER_UI}   dashboards ${GRAFANA_UI}"
}

cmd_ps()   { compose ps; }
cmd_logs() { compose logs -f --tail 100 "${@:-taskmanager}"; }

cmd_down() {
    step "Stopping the stack (data preserved)"
    compose --profile tools --profile local-llm --profile manual down
    ok "stopped; './run.sh up' restarts and re-seeds the corpus from scratch"
}

cmd_clean() {
    step "Removing the stack and all data"
    compose --profile tools --profile local-llm --profile manual down -v
    rm -rf volumes target
    docker volume rm "$MAVEN_CACHE_VOLUME" >/dev/null 2>&1 || true
    ok "containers, Milvus volumes and build output removed"
}

cmd_all() {
    cmd_preflight
    cmd_config
    cmd_build
    cmd_up          # seeds as its last step; do not call cmd_seed again here
    cmd_verify
    cmd_submit
    printf '\n%sEverything is running.%s\n' "${C_GREEN}${C_BOLD}" "${C_RESET}"
    printf '  Flink UI    %s\n  Milvus UI   %s\n' "$FLINK_UI" "$MILVUS_UI"
    printf '  Incidents   %s\n  Dashboards  %s\n' "$PLANNER_UI" "$GRAFANA_UI"
    printf '  Browser UIs ./run.sh console  (Kafka %s, Attu %s)\n' "$CONSOLE_UI" "$ATTU_UI"
    printf '  Output      ./run.sh consume\n  Retrieval   ./run.sh smoke\n\n'
}

usage() {
    cat <<'USAGE'
run.sh - Flink 2.3 + Milvus streaming RAG stack

  ./run.sh all            preflight, configure, build, start, seed, verify, submit
  ./run.sh preflight      check Docker, disk space and port availability
  ./run.sh config         create .env and set the OpenAI token (prompts if needed)
  ./run.sh use-ollama     switch to local models, no API key, no outbound traffic
  ./run.sh set-key        re-enter the API key, validated before it is stored
  ./run.sh check-key      test the configured key against the API
  ./run.sh build          build the connector and the Flink image
  ./run.sh up             start Milvus, Kafka and Flink, then re-seed the corpus
  ./run.sh seed           re-embed the corpus and load the Milvus collection
  ./run.sh submit [job]   submit all four jobs, or one of:
                          retrieve | plan | incidents | status
  ./run.sh reload-jar     rebuild the connector and hot-load it into the running containers
  ./run.sh verify         confirm the connector jar is on every Flink container
  ./run.sh smoke          run VECTOR_SEARCH with a constant probe, no models involved
  ./run.sh alarms         tail the raw machine_alarms topic (generator output)
  ./run.sh incidents      summarise the incident store: statuses, human rate, latency
  ./run.sh psql [args]    open psql against the incident store
  ./run.sh audit [n]      score retrieval against the generator's ground truth
  ./run.sh console        open both browser UIs: Kafka console (8090) and Attu (8000)
  ./run.sh consume        tail the output topic from Kafka
  ./run.sh inspect        read the work-orders topic back through Flink SQL
  ./run.sh ps             service status
  ./run.sh logs [service] follow logs (default: taskmanager)
  ./run.sh down           stop, keep data
  ./run.sh clean          stop and delete all data and build output

Environment:
  OPENAI_API_KEY    if set, written into .env by 'config'
  NON_INTERACTIVE=1 never prompt; fail instead (for CI)
  NO_COLOR=1        plain output
  SKIP_SEED=1       'up' keeps the existing collection instead of re-seeding
USAGE
}

main() {
    local cmd="${1:-all}"
    shift || true
    case "$cmd" in
        all)         cmd_all ;;
        preflight)   cmd_preflight ;;
        config)      cmd_config ;;
        use-ollama)  cmd_use_ollama ;;
        set-key)     cmd_set_key ;;
        check-key)   cmd_check_key ;;
        build)       cmd_build ;;
        up)          cmd_up ;;
        seed)        cmd_seed ;;
        submit)      cmd_submit "$@" ;;
        reload-jar)  cmd_reload_jar ;;
        verify)      cmd_verify ;;
        smoke)       cmd_smoke ;;
        console)     cmd_console ;;
        alarms)      cmd_alarms ;;
        psql)        cmd_psql "$@" ;;
        incidents)   cmd_incidents ;;
        audit)       cmd_audit "$@" ;;
        consume)     cmd_consume ;;
        inspect)     cmd_inspect ;;
        ps)          cmd_ps ;;
        logs)        cmd_logs "$@" ;;
        down)        cmd_down ;;
        clean)       cmd_clean ;;
        help|-h|--help) usage ;;
        *)           printf 'unknown command: %s\n\n' "$cmd" >&2; usage; exit 1 ;;
    esac
}

main "$@"
