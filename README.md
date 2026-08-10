# Streaming RAG

A Milvus vector-search source for Apache Flink 2.2+, scoped to **corpus retrieval** — enough to run
the VVP 3 / VERA workshop pipeline on open-source Flink with the SQL unchanged.

```sql
SELECT * FROM machine_alarms a,
LATERAL TABLE(VECTOR_SEARCH(TABLE maintenance_manual, DESCRIPTOR(vec), a.alarm_vector, 1))
```

## Status

Compiles against Flink 2.3.0 and Milvus SDK 2.5.11. The type converter is unit-tested. **Run
end to end against a live Milvus v3.0.0**: `./run.sh smoke` returns ranked hits from the seeded
collection, and the full `Kafka -> ML_PREDICT(embed) -> VECTOR_SEARCH -> Kafka ->
ML_PREDICT(chat) -> Kafka` pipeline produces grounded work orders on the `work-orders` topic.

## The demo dataset

Machine alarms from a small equipment fleet, retrieved against service-manual sections and turned
into work orders.

```
ALARM  MILL-11 · "the axis losing position intermittently with no overcurrent alarm."
       1.4 mm/s · 41 °C · 163 bar · line_critical · shift B
  ↓ ML_PREDICT(embed)
HIT    SM-6.5 "Encoder signal loss" · electrical · severity 3 · score 0.584
  ↓ SQL enrichment
       P2 · electrical-team · confidence high · SLA +4h
  ↓ ML_PREDICT(chat)
ORDER  reseat connector X4, check shield bonding, replace encoder EN-1602
       parts ["EN-1602"] · 50 min · line stop likely
```

**Corpus** — **314 sections**, in two deliberately separate parts:

- **14 hand-written** sections in [seed_milvus.py](deploy/seed/seed_milvus.py), spanning
  mechanical, hydraulic, electrical, thermal, safety and pneumatic faults. These are what the
  ten simulated faults retrieve and what the audit scores. Each names a distinct part number,
  threshold and duration — that is what proves an answer came from retrieval rather than from
  the model's priors.
- **300 generated** sections in `corpus_extra.json`, built by
  [tools/generate_corpus.py](tools/generate_corpus.py) and committed, not generated at seed
  time. This is the realistic body around the curated core: mostly irrelevant to any given
  alarm, which is what a real manual is.

Set `CORPUS_EXTRA=0` to seed only the curated 14. That is not just a size knob — see below.

**Corpus size changes retrieval behaviour, and the numbers are worth knowing:**

| | 14 sections | 314 sections |
|---|---|---|
| Top-1 hits the intended section | 35/35 | 24/35 |
| Correct-match scores | 0.398–0.685 | 0.444–0.685 |
| Uncovered-fault scores | 0.158–0.315 | 0.414–0.470 |
| `low` threshold that separates them | 0.35, cleanly | 0.47, 10/10 at the cost of 3/35 |

At 14 sections retrieval is exact and the confidence gate has a wide, empty gap to sit in. At
314 every query finds *something* plausible, so the floor rises, the gap nearly closes, and
several sections legitimately compete to answer one complaint — "roller bearing pitting above
5 mm/s RMS" is a fair answer to a spindle vibration report even when "spindle bearing wear
above 7.1 mm/s RMS" is the intended one.

Two consequences the demo actually depends on:

- **The absolute-score gate is weaker at scale, not broken.** 0.47 still catches every uncovered
  fault; it costs 3 of 35 covered ones being flagged for a human unnecessarily. That is the
  right direction for the error to fall.
- **The grounding guard catches what the threshold misses.** When retrieval returns a section
  that does not fit, the planner returns "insufficient documentation" rather than a confident
  wrong answer, and the incident routes to a human as `NEEDS_REVIEW`. Two independent nets,
  which is why the human rate sits nearer 30% at 314 sections than the 15%
  `UNCOVERED_PROBABILITY` alone would give.

The honest fix for top-1 precision at this corpus size is retrieving several candidates and
reranking them, which needs a windowed `LISTAGG` to collapse the fan-out before `ML_PREDICT`.
That is a different pipeline; see the note at the end of this section.

**Events** — a Python generator, [deploy/datagen/alarms.py](deploy/datagen/alarms.py), producing
JSON to the `machine_alarms` Kafka topic. 10 faults × 35 phrasings × 6 qualifiers = 210 distinct
alarm texts. It was a Flink `datagen` table until the event model outgrew what `CASE` expressions
can express. Three details matter:

- *Telemetry is derived from the fault*, not drawn independently, so a bearing-wear alarm carries
  high vibration rather than a random value.
- *The asset is derived from the fault too*, via a per-machine subsystem list. Independent draws
  put "spindle bearing wear" on a conveyor, which is exactly the detail that makes an audience
  stop believing a demo.
- *Faults sometimes burst* — the same fault on the same machine, two to four times — so the
  output reads like a deteriorating machine rather than uniform noise.

Tune it with `ALARMS_PER_SECOND`, `BURST_PROBABILITY` and `SEED` in `docker-compose.yml`. It is
the only throttle on OpenAI spend: one alarm is one embedding plus one chat call.

**What gets embedded** — only the operator's symptom prose. The readings are passed to the LLM as
labelled structured facts instead of being folded into the embedded string, because random digits
in the probe vector blur retrieval. Embed the semantics, carry the numbers alongside.

**Enrichment** — `severity` comes from the retrieved manual section, `criticality` from the alarm
event, and `priority` is the product of the two. The queue and the SLA deadline follow. None of it
is derivable without the retrieval, which is the point worth narrating.

**Retrieval quality** — `./run.sh audit` scores this end to end against the generator's own
ground truth: each fault carries a `manual_hint` recording which section it *should* retrieve,
so correctness is checked by machine rather than by eye. It is two-sided — covered faults must
hit their section, uncovered ones must score `low`.

It samples the live topic, so at 0.1 alarms/second a run sees only a handful. For a complete
picture, run every phrasing through `VECTOR_SEARCH` directly; that is how the table above was
produced, and it is the check to repeat after any corpus or phrasing change.

```bash
./run.sh audit 25
```

**Run it after any corpus or phrasing change.** It costs nothing — it reads work orders the
pipeline has already produced — and it catches the failure liveness checks cannot: a pipeline
that runs happily while matching hydraulic faults to the pneumatic section. It found exactly that
when the generator was first written.

## The SQL: four jobs, one set of DDL

The pipeline used to be a single job with eight `INSERT`s inside one `EXECUTE STATEMENT SET`.
It worked, but it had one plan, one lifecycle and one failure — hard to explain and harder to
operate. It is now four jobs, submitted separately:

```
deploy/sql/ddl/                    loaded into every job's session
  01_sources.sql    machine_alarms, from Kafka
  02_milvus.sql     maintenance_manual, the corpus
  03_models.sql     embedding + planner models
  04_topics.sql     alarm-context, work-orders
  05_postgres.sql   the incident store sinks

deploy/sql/jobs/                   one Flink job each
  10_retrieve.sql   embed -> VECTOR_SEARCH -> enrich    the one worth explaining
  20_plan.sql       the planner model call              the expensive one
  30_incidents.sql  incident upserts
  40_status.sql     the status timeline
```

```bash
./run.sh submit              # all four, in pipeline order
./run.sh submit retrieve     # just one
```

**Why the DDL is separate and loaded with `-i`.** Flink's default catalog is in-memory and
scoped to a SQL client session, so a table created by one submission does not exist for the
next. Without this, every job would need its own copy of every `CREATE TABLE`. `sql-client.sh
-i` loads an init file into the session before running the `-f` script, which is exactly the
problem it solves. Every job gets every DDL file — they are metadata, they cost nothing, and a
job can then never fail because someone forgot to list a table.

Two things that cost time when this was built, both recorded in `deploy/submit.sh`:

- **`-i` takes one file, not a comma-separated list.** Passing a list makes the client try to
  open the joined string as a single filename and die with `FileNotFoundException` naming all
  five paths. `submit.sh` concatenates the DDL into one init file in sorted order.
- **`sql-client.sh` exits 0 on failure** — after `[ERROR] Could not execute SQL statement`, and
  also after an uncaught `SqlClientException` that never reaches the SQL at all. Both were once
  reported as a successful submit with no jobs running. Success is now judged by the presence of
  a `Job ID:` per job, and nothing else.

Operationally the split earns its keep: you can stop the expensive planner job and watch
retrieval carry on filling its topic, or restart one stage without replanning the rest.

## The incident store

Work orders no longer stream past and vanish. Postgres is the system of record, written by a
**Flink JDBC sink**, and read by a planner UI and Grafana.

```
incident          one row per alarm: alarm, retrieval, triage, planner output, latency
incident_status   append-only transition log, pipeline and human alike
incident_current  view: incident + its latest status + needs_human + sla_breached
```

**Two Flink tables target the same Postgres row with disjoint column sets.** Stage 1 knows the
alarm, the retrieval and the triage; stage 2 knows the planner output. A JDBC sink emits
`ON CONFLICT (pk) DO UPDATE SET <only the columns it declares>`, so neither stage can null the
other's work, and neither declares the human-owned columns (`assignee`, `manual_action`,
`resolution_notes`) that the UI writes. That is what makes a two-stage upsert safe here.

Status flows `RECEIVED → CONTEXTUALIZED → TRIAGED → ACTION_DEFINED`, branching to
`NEEDS_CONTEXT` when retrieval scores `low` and `NEEDS_REVIEW` when the planner returns
"insufficient documentation". A human then drives `ASSIGNED → IN_PROGRESS → RESOLVED → CLOSED`,
or `REJECTED`.

There is no `status` column. Current state is derived from the last row of the timeline, so the
log is the only source of truth and there is nothing to keep in agreement.

**Incidents that need a human are produced honestly**, not by a flag: the generator emits faults
this manual genuinely does not cover — a crane pendant dropping out, a fire door that will not
latch — which score below the measured `low` threshold and route to a person. Roughly 15%, set
by `UNCOVERED_PROBABILITY`.

```bash
./run.sh incidents        # statuses, human rate, latency, in one screen
./run.sh psql             # the store itself
```

| | |
|---|---|
| Planner UI | http://localhost:9052 |
| Dashboards | http://localhost:3000 |

## Throughput and latency

Latency is measured end to end: `t_alarm` (generator) → `t_context` (stage 1 emit) → `t_action`
(stage 2 emit). The lag columns are Postgres generated columns, so they cannot drift from the
timestamps they derive from.

Measured steady state at the shipped settings: **p50 1.6 s, p95 2.7 s end to end**, of which
~300 ms is embedding plus the Milvus round trip and ~1.4 s is the planner call.

**The generator rate is the throttle for the whole pipeline, and it is set below capacity on
purpose.** The Kafka source for `alarm-context` is chained to the planner call in the job graph
and job parallelism is 1, so with async inference at 4 concurrent calls the ceiling is about
0.16 alarms/second. `ALARMS_PER_SECOND` ships at 0.1.

Set it above the ceiling and the failure is quiet: `planning_lag_ms` climbs into the minutes and
every dashboard shows stale numbers that still look plausible. Two things make this worth
respecting:

- `./run.sh incidents` twice, a few minutes apart, is the check. If `planner_ms` grows, the rate
  is too high.
- Pushing much harder risks HTTP 429 from OpenAI, which is worse than slow. The async operator
  times out, **fails the task**, replays from checkpoint and times out again — the output topic
  stops dead rather than merely slowing. `table.exec.async-ml-predict.max-concurrent-operations`
  is set to 4 for exactly this reason; 12 reproduced the death spiral reliably.

## Facts verified against the Flink 2.3.0 jars

Worth stating because the published documentation is inconsistent on both points, and both cause
runtime rather than compile-time failures:

1. **Argument order is `(vector_table, DESCRIPTOR(index_column), input_table.vector_column, top_k, [CONFIG])`**
   — **`DESCRIPTOR` second, query vector third**, as FLIP-540, the Alibaba documentation and the
   VVP syntax all show.

   The Apache docs page and the 2.2.0 release announcement show the reverse. Do not follow them
   on 2.3.0: the shipped validator rejects that form outright, whatever the operand types, with

   ```
   Cannot apply 'VECTOR_SEARCH' to arguments of type '...'. Supported form(s):
   VECTOR_SEARCH(TABLE search_table, DESCRIPTOR(column_to_search), column_to_query, top_k, [MAP[...]])
   ```

   That message is not just a doc string. `PhysicalVectorSearchTableFunctionRule` reads
   `getOperands().get(1)` as the `DESCRIPTOR` call and `get(2)` as the query column, so
   descriptor-second is what the planner actually implements. Verified end to end against a live
   Milvus with `./run.sh smoke`.

2. **The `LATERAL` must carry no table alias.** This one costs a day if you do not know it:

   ```sql
   -- plans and runs
   FROM machine_alarms a, LATERAL TABLE(VECTOR_SEARCH(TABLE maintenance_manual, DESCRIPTOR(vec), a.alarm_vector, 1))

   -- fails to plan; `AS ctx` alone is enough to break it
   FROM machine_alarms a, LATERAL TABLE(VECTOR_SEARCH(...)) AS ctx(id, content, category, vec, score)
   ```

   With an alias the planner loses the correlation, so the lateral degenerates into a cross join,
   `PhysicalVectorSearchTableFunctionRule` (which only matches `FlinkLogicalCorrelate`) never fires,
   the Milvus scan is left with no physical implementation, and planning fails with

   ```
   CannotPlanException: There are not enough rules to produce a node with desired properties
   ```

   A correct plan contains a `Correlate`; a broken one shows `FlinkLogicalJoin(condition=[true])`
   with the vector search on the right-hand side. Unaliased, the search table's own column names
   plus `score` are in scope, so reference them bare (`content`, not `ctx.content`). Aliasing the
   left-hand input is fine — only the lateral is affected.
3. **The trailing `score` column is DOUBLE**, not FLOAT. Confirmed from
   `getRowTypeInference()` calling `createSqlType(SqlTypeName.DOUBLE)`. Emitting a `Float` for it
   compiles fine and throws `ClassCastException` in generated code at runtime.

Also confirmed: `VectorSearchFunction` and `AsyncVectorSearchFunction` are **abstract classes**
(FLIP-540 wrote them as interfaces), the providers live in
`org.apache.flink.table.connector.source.search` with static `of(...)` factories, and there is no
`PARAM_ON_TIME` — the event-time / time-travel variant is not implemented in 2.3.

If you move to a different Flink minor, re-check these:

```bash
javap -cp flink-table-common-<v>.jar \
  org.apache.flink.table.functions.VectorSearchFunction \
  org.apache.flink.table.connector.source.VectorSearchTableSource
javap -p -constants -cp flink-table-planner_2.12-<v>.jar \
  org.apache.flink.table.planner.functions.sql.ml.SqlVectorSearchTableFunction
```

## Build and install

```bash
mvn clean package
cp target/flink-connector-milvus-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
```

Then restart the cluster — jars in `lib/` are loaded at JVM start.

## Options

| Option | Required | Default | Notes |
|---|---|---|---|
| `collectionName` | yes | | Must exist and be **loaded into memory** |
| `endpoint` / `port` | yes* | / 19530 | \*or `uri` |
| `uri` | no | | Full URI; use for TLS or Zilliz Cloud |
| `databaseName` | no | `default` | |
| `userName` + `password` | no | | |
| `token` | no | | API key; alternative to user/password |
| `search.metric` | no | `COSINE` | `COSINE`, `L2`, `IP` — must match the index |
| `search.consistency-level` | no | `BOUNDED` | |
| `search.filter` | no | | Milvus boolean expression, applied as a pre-filter |
| `search.query-vector-type` | no | `float` | `float` or `double`; see below |
| `search.max-retries` | no | `3` | |
| `connect.timeout` | no | `10s` | |
| `async.thread-pool-size` | no | `8` | Only used when `CONFIG` sets `async` = `true` |

The camelCase keys mirror the Ververica and Alibaba connectors so existing DDL is a drop-in.

**`search.query-vector-type` is explicit on purpose.** The connector cannot see the SQL type of the
probe column at runtime, and reading a `BinaryArrayData` of doubles as floats misreads the bytes
*silently* — you get plausible-looking garbage vectors and quietly wrong retrieval. Default `float`
matches `ARRAY<FLOAT>`, which is what `ML_PREDICT` embedding models emit.

## Async mode

Set it per query, not on the table:

```sql
VECTOR_SEARCH(TABLE maintenance_manual, DESCRIPTOR(vec), a.vec, 1, CONFIG => MAP['async', 'true'])
```

Tune alongside `table.exec.async-vector-search.max-concurrent-operations` and
`.timeout`. Note that the Milvus v2 SDK is blocking, so async here means calls are dispatched on a
bounded pool rather than being natively non-blocking — `async.thread-pool-size` is a hard
concurrency ceiling per subtask.

## Deliberately not implemented

- **No sink.** Write your corpus with `pymilvus` or the Milvus CLI. Adding upsert, batching and
  at-least-once semantics is roughly as much work again as everything here.
- **No projection pushdown.** Every declared column is fetched, including the vector, which is
  wasted bandwidth on every hit. Drop `vec` from the DDL if queries never select it, or implement
  `SupportsProjectionPushDown`.
- **Narrow type support.** Numerics, strings and arrays thereof — what a corpus needs. Anything
  else throws at planning time with a message naming the column. Milvus supports ~16 field types;
  covering them all is where connector bug counts live, and the workshop needs four.
- **No `on_time` / time travel.** Not in Flink 2.3 either.
- **No hybrid or sparse search**, no correlated pre-filters. FLIP-540 lists these as future work.

## Docker deployment

Everything runs from Docker; no local JDK, Maven or Python needed. The connector is built inside a
multi-stage image, so `docker compose build` compiles it and drops the shaded jar into
`/opt/flink/lib`.

```bash
./run.sh all              # everything: preflight, config, build, start, seed, verify, submit
```

That is the whole thing. It prompts once for an OpenAI key (or press Enter to switch to local
models via Ollama and use no key at all), then builds the connector, starts Milvus and Kafka and
Flink, waits for each to be genuinely healthy, bootstraps the corpus, checks the connector is
loaded, and submits the pipeline. Re-running it is safe.

### Choosing a model provider

Both models — the embedder and the planner — are reached over the OpenAI wire format, so a
provider is just a base URL and a key. Three are supported, and switching is one command:

| Provider | Command | Key | Embedding width |
|---|---|---|---|
| OpenAI (default) | `./run.sh use-openai` | yours | 1536 |
| Ollama, on this machine | `./run.sh use-ollama` | none | 768 |
| A self-hosted llm-launchpad box | `./run.sh use-launchpad <url> [token]` | its `API_TOKEN`, if set | read from the box |

```bash
./run.sh use-launchpad http://my-gpu-box:5001/v1 my-api-token
```

`use-launchpad` reads the served embedding model, its dimension and the chat model from the box's
health endpoint rather than asking you to type them, because an `EMBED_DIM` that disagrees with
the endpoint produces a collection the vectors cannot be written to — and the seeder only
discovers that after dropping the old one. If the health response names no chat model it falls
back to `CHAT_MODEL=local` and says so; set it by hand if the planner later returns a 404.

Two things to know about the URL. It must end in `/v1` (the model DDL appends `/embeddings` and
`/chat/completions` to it), and it must be reachable **from inside the containers**: the seeder
and both Flink services make the calls, so `localhost` means the container itself and is
rejected. Use the box's hostname or IP, or `host.docker.internal` when it runs on this machine.
`use-launchpad` checks reachability from `jobmanager` as well when the stack is already up.

**Switching re-seeds only when it has to.** The corpus is fingerprinted by embedding model *and*
width, and `use-*` offers a re-seed only when that fingerprint moves. Re-pointing at a restarted
box serving the same model is free; changing the model is not, and a stale collection fails at
query time inside a running job, as a restart loop, long after the command that caused it
reported success.

The fingerprint covers the model name and not just the width for a reason: **a different model at
the same width produces vectors that are silently incomparable with the stored ones.** The
seeder's only guard is the dimension check, which such a switch passes cleanly while retrieval
quietly gets worse. `./run.sh audit` is what catches it.

#### Running the box on its own instance

The intended shape is llm-launchpad on a dedicated GPU instance and this stack on another, with
the box started first and its address passed in:

```bash
./run.sh use-launchpad http://<launchpad-private-ip>:5001/v1 <token>
```

- **Prefer the private/VPC address when both instances share a VPC.** A public IP sends every
  embedding and chat call out of the VPC and back, and it changes whenever the instance is
  stopped and started without an Elastic IP.
- **The launchpad instance's security group has to allow this instance on the API port.** That is
  the most likely failure, and it is invisible from a browser on your laptop. The reachability
  probe `use-launchpad` runs from `jobmanager` is the check to trust — the containers route out
  of the instance exactly as the host does, so a host that connects and a container that cannot
  points at the security group, not at compose.
- Over plain `http://`, the token travels in the clear, and so does every prompt — the operator's
  alarm text and the retrieved manual section. Restrict the port by security group, or terminate
  TLS on the box.
- Milvus and the corpus stay local to this instance. The box is only asked for embeddings and
  chat completions; nothing about the corpus leaves.

**`up` re-seeds the corpus every time**, so no run inherits the previous one's Milvus state.
This is deliberate. Milvus persists collection *data* to `./volumes`, but it does not restore
the in-memory *load state* on boot, and a search against an unloaded collection fails at
runtime rather than at submit time — the pipeline restart-loops on `collection not found` long
after the command that caused it reported success. Seeding drops, recreates, inserts, flushes
and loads, which costs one embedding call for six short documents. Set `SKIP_SEED=1` to keep an
existing collection, for instance one you have hand-modified:

```bash
SKIP_SEED=1 ./run.sh up
```

```bash
./run.sh consume          # tail the planned work orders
./run.sh alarms           # tail the raw generator output
./run.sh audit            # score retrieval against the generator's ground truth
./run.sh incidents        # incident store: statuses, human rate, latency
./run.sh psql             # query the incident store directly
./run.sh console          # Kafka UI at localhost:8090 -- browse topics and read events
./run.sh smoke            # test retrieval on its own, no model calls
./run.sh help             # every command
```

### Kafka UI

`./run.sh console` starts [Redpanda Console](https://github.com/redpanda-data/console) and
[Attu](https://github.com/zilliztech/attu) together — Console shows what moved through Kafka,
Attu shows the corpus those events were matched against. Console runs against the
existing `apache/kafka` broker. Console is a Kafka-protocol client, not a broker — **there is no
reason to swap the broker for Redpanda to get the UI**, and doing so would move the pipeline off
the combination it was validated on. It reads `alarm-context` (retrieval plus enrichment, before
the LLM) and `work-orders` (the finished work orders), decodes the JSON payloads, and lets you
filter and replay from any offset. It is in the `tools` profile, so `up` does not start it and `down` stops it.

A `Makefile` is included if you prefer `make all`.

Its targets are thin wrappers around `run.sh` — `make up` runs `./run.sh up`, and so on. That is
on purpose: the recipes used to call `docker compose` directly, and the Makefile silently fell
behind, skipping the Kafka topic pre-creation and the corpus re-seed that `run.sh up` does. Two
entry points with two implementations drift by default. `make help` lists everything. Expect
roughly 5-10 minutes on a cold start: most of it is the Maven dependency download in the builder
stage and the ~500 MB Milvus image pull.

Note that `make verify-jar` is now `make verify`, matching the `run.sh` command name.

`make init` is a guard, not a convenience. Every Flink service uses `env_file: [.env]`, and Compose
fails with a bare "env file not found" if it is missing — so the first command in a clean clone
used to error out with a message that did not point at the cause. `init` also refuses to continue
while `OPENAI_API_KEY` is still the placeholder, because that failure would otherwise surface much
later as an opaque 401 inside the ML_PREDICT operator.

### Troubleshooting

| Symptom | Cause |
|---|---|
| `Could not find any factory for identifier 'milvus'` | The shaded jar lost its classes or SPI file. Run `./run.sh verify`; see the groupId note below |
| `Could not find any factories that implement ModelProviderFactory` | `flink-model-openai` is missing from `/opt/flink/lib`. It is a **separate artifact**, not part of the Flink distribution. Rebuild the image |
| `env file .env not found` | Run `make init` first |
| Flink containers never start | They gate on `standalone` being healthy; Milvus needs ~90s. `make ps` |
| `collection not loaded` / `collection not found` | Milvus will not search an unloaded collection, and does not restore load state on boot. `up` now re-seeds every time, so this should not recur; `./run.sh seed` fixes it by hand |
| Retrieval returns odd matches | `search.metric` in the DDL disagrees with the index metric, or `EMBED_DIM` is wrong. The seeder now fails loudly on a dimension mismatch. After any provider switch, re-run `./run.sh seed` |
| 401 from the model | Bad or partially copied key. `./run.sh check-key` to test it, `./run.sh set-key` to replace it. Keys are now validated against the API before being stored. Against a self-hosted box, the key is its `API_TOKEN` |
| Model calls fail only inside the containers | The base URL is reachable from this machine but not from the compose network — typically `localhost`. Use a routable host or `host.docker.internal` |
| Job runs, no output | Nothing consumes `work-orders` yet. `./run.sh consume` |
| `failed to solve: image ...: already exists` | Two services declaring `build:` for the same `image:` tag. Only `jobmanager` builds; `taskmanager` reuses the tag |
| `pull access denied for flink-milvus` | The image was never built. `./run.sh build` |
| `verify` reports `syntax error in expression` | An image built before `unzip` was added. Rebuild: `./run.sh build && ./run.sh up` |
| `lstat .../deploy: no such file or directory` | Flattened download. Extract the archive; `./run.sh preflight` checks this |
| `dependency failed to start: container kafka exited (1)` | Kafka config problem. `./run.sh up` now prints the failing container's log; `./run.sh logs kafka` for the rest |

| Service | Port | Notes |
|---|---|---|
| Flink UI | 8081 | job graph, the VectorSearch operator, backpressure |
| Milvus | 19530 | WebUI on 9091 at `/webui/` |
| MinIO console | 9001 | `minioadmin` / `minioadmin` |
| Attu | 8000 | `./run.sh console`; browse the corpus, sanity-check retrieval |
| Kafka | 9092 | |

### Relationship to the official Milvus compose file

The `etcd`, `minio` and `standalone` services are copied from the official standalone compose file
for v3.0.0 — same images, environment, healthchecks, and the same
`${DOCKER_VOLUME_DIRECTORY:-.}/volumes` bind mounts, so `rm -rf volumes` wipes Milvus state exactly
as the Milvus docs describe (`make clean` does both). Milvus v3.0.0 needs only these three
containers: the message queue is Woodpecker, embedded, using MinIO as its WAL backend, so unlike
2.5.x there is no separate MQ service.

Deviations, all deliberate:

- `version: '3.5'` dropped — obsolete in Compose V2 and it emits a warning.
- Network renamed from `milvus` to `flink-milvus-rag`, since Flink and Kafka share it.
- `depends_on: condition: service_healthy` added so Flink does not start before Milvus is up.
- Optional services sit behind profiles so a plain `up` starts only what the pipeline needs.

If you upgrade Milvus, re-download the official file and re-apply these four changes rather than
editing versions in place — the compose layout changes between Milvus majors.

### Secrets

`CREATE MODEL` takes the API key as a literal string and Flink SQL cannot read environment
variables, which is why the pipeline is a template. `deploy/submit.sh` renders it with `envsubst`
inside the container, restricted to a named variable list, and deletes the rendered file on exit.
The key reaches disk nowhere in the repo. This is a workaround for a real gap, not a solution — for
production use a secrets manager and a catalog that supports secret references.

The same path carries a self-hosted box's `API_TOKEN`: it is just another bearer token in
`OPENAI_API_KEY`. Note it will not match the `sk-`-shaped patterns a secret scan looks for,
so treat `.env` as sensitive regardless of which provider is configured.

## Loading the connector jar into the deployment

### Three jars are required, not one

`/opt/flink/lib/` needs all of these, and each fails differently if absent:

| Jar | Provides | Symptom when missing |
|---|---|---|
| `milvus-connector.jar` (built here) | `'connector' = 'milvus'` | `Could not find any factory for identifier 'milvus'` |
| `flink-model-openai` | `CREATE MODEL ... 'provider' = 'openai'` | `Could not find any factories that implement ModelProviderFactory` |
| `flink-sql-connector-kafka` | `'connector' = 'kafka'` | `Could not find any factory for identifier 'kafka'` |

The OpenAI provider surprises people: `CREATE MODEL` is lazy, so the DDL succeeds and the
failure only appears when the first `ML_PREDICT` is planned. `./run.sh verify` checks for all
three.

### Where it has to go, and why

The jar must sit in `/opt/flink/lib/` on **the JobManager and every TaskManager**. Two separate
reasons, and missing either one produces a different confusing failure:

- The **JobManager** resolves `'connector' = 'milvus'` during planning, via Java's `ServiceLoader`
  reading `META-INF/services/org.apache.flink.table.factories.Factory`. Without the jar here you get
  `Could not find any factory for identifier 'milvus'` at submit time.
- The **TaskManagers** actually execute the search function. Without the jar here the job submits
  successfully and then fails at runtime with `ClassNotFoundException`, which looks unrelated to the
  connector.

`/opt/flink/lib/` is scanned **once, when the JVM starts**. This is the detail that wastes an
afternoon: dropping in a new jar and resubmitting the job changes nothing. The containers must
restart.

### Four ways to get it there

| Route | Use when | Restart needed |
|---|---|---|
| **1. Baked into the image** (default here) | Normal use, CI, anything shared | Rebuild + recreate |
| **2. Bind mount** | Iterating with a local Maven install | Restart containers |
| **3. `docker cp` + restart** | Iterating without rebuilding the image | Restart containers |
| **4. `ADD JAR` / `pipeline.jars`** | Trying a jar without touching the cluster | No |

**Route 1 — baked in (what `./run.sh build` does).** `deploy/Dockerfile` stage 1 runs Maven, stage 2
copies the result to `/opt/flink/lib/milvus-connector.jar`. Both Flink services use that image, so
JobManager and TaskManagers are consistent by construction. This is the route you want for anything
other than a tight edit loop.

```bash
./run.sh build && ./run.sh up
```

**Route 2 — bind mount.** Add to both Flink services in `docker-compose.yml`:

```yaml
    volumes:
      - ./target/flink-connector-milvus-1.0-SNAPSHOT.jar:/opt/flink/lib/milvus-connector.jar:ro
```

Then `mvn package && docker compose restart jobmanager taskmanager`. Convenient, but the container
now depends on a host path, so the compose file stops being portable. Note that a bind mount over a
path the image already populates *replaces* the baked-in jar, which is usually what you want here
but is worth knowing.

**Route 3 — `docker cp` into running containers.** This is what `./run.sh reload-jar` automates:

```bash
./run.sh reload-jar
```

It compiles in a throwaway Maven container against a cached `~/.m2` volume, sanity-checks the jar,
copies it into every running Flink container (including multiple TaskManager replicas), and restarts
them. Roughly 20 seconds on a warm cache versus a couple of minutes for an image rebuild. It warns
you that the restart killed any running job.

**Route 4 — `ADD JAR`, no restart.** Inside the SQL client:

```sql
ADD JAR '/tmp/flink-connector-milvus-1.0-SNAPSHOT.jar';
```

This loads into the *user* classloader for that session and ships the jar with the job, so no
restart is needed. Good for trying a build quickly. Two caveats: the jar must be readable from
inside the container (`docker cp` it to `/tmp` first), and user-classloader loading interacts badly
with connectors that keep static state or register JVM-global providers — with a shaded gRPC client
in the jar, prefer `lib/` for anything you intend to keep.

### Verifying it actually loaded

```bash
./run.sh verify    # jar present with >= 8 classes on every Flink container
./run.sh smoke     # runs VECTOR_SEARCH with a constant probe, no models involved
```

`verify` is a static check; `smoke` is the real one. It creates a Milvus table and runs a top-3
search with a synthetic probe vector sized from `EMBED_DIM`, which exercises factory discovery, the
gRPC path, the type converter and the collection state — without spending a cent on model calls. If
`smoke` passes, the connector is genuinely working and anything still broken is upstream in the
models or Kafka.

### Errors and what they mean

| Error | Meaning |
|---|---|
| `Could not find any factory for identifier 'milvus'` | Jar absent from the JobManager, or its SPI entry is missing. `./run.sh verify` |
| `ClassNotFoundException: ...MilvusVectorSearchFunction` | Jar on the JobManager but not the TaskManagers |
| Jar is present but the error persists | The containers were not restarted after the jar changed |
| `NoSuchMethodError` mentioning gRPC or Netty | Shade relocations incomplete; `./run.sh reload-jar` fails fast on this |
| `collection not loaded` / `collection not found` | Milvus side, not the connector: the collection is missing or was never loaded into memory. `up` re-seeds every time, so this should not recur; `./run.sh seed` fixes it by hand |
| `CannotPlanException: not enough rules ... FlinkLogicalTableFunctionScan` | `VECTOR_SEARCH` argument order. Query vector second, `DESCRIPTOR` third |
| `cannot create /tmp/smoke.sql: Permission denied` | A root-owned file left by an older script revision. `docker compose exec -u root jobmanager rm -f /tmp/smoke.sql`. Current versions use a unique filename |

## What the build actually caught

Three things surfaced only by compiling and packaging for real, which is why this section exists
rather than a "should work" note:

**The fat jar shipped without the connector in it.** The POM excluded `org.apache.flink:*` from
shading to keep the provided Flink jars out — and the project's own groupId *was*
`org.apache.flink`, so the shade plugin dropped this project's classes and its SPI registration.
The jar built, installed and deployed happily; `'connector' = 'milvus'` would simply have been
unknown. Fixed by moving off the `org.apache.flink` groupId and replacing the wildcard with
explicit per-artifact excludes. Verify after any POM change:

```bash
make verify-jar     # expects 10 classes, the SPI line, and 0 leaked packages
```

**Milvus SDK 2.5.x and 3.0.x disagree on the builder type.** `SearchReq.SearchReqBuilder` is
generic in 2.5.x and raw in 3.0.x, so naming that type pins you to one SDK generation. The chain in
`MilvusSearchClient` is never assigned to a named builder variable, and the optional pre-filter uses
an empty string rather than a conditional call, which keeps the source compiling against both.
Verified against 2.5.11 and 3.0.6.

**Shading was far less painful than expected**, because the Milvus SDK already relocates its own
gRPC and Netty under `io.milvus.shaded`. The relocations in the POM are still worth keeping for
commons-lang3 and gson, but the gRPC-versus-Flink collision that usually eats days did not
materialise. Treat that as specific to Milvus, not a general rule.

## Verification status

| | |
|---|---|
| Compiles against Flink 2.3.0 | yes |
| Compiles against Milvus SDK 2.5.11 and 3.0.6 | yes |
| `mvn package` with shading | yes — jar complete, SPI present, no leaked or bundled packages |
| Unit tests | 3 passing |
| Interfaces read from the real 2.3.0 jars | yes |
| **Run end-to-end against live Milvus** | **no — do this first** |

The container had no Docker daemon, so the compose stack itself is unexercised. The riskiest
remaining step is the one that cannot be checked without a running Milvus: confirm hit ordering and
scores match a direct `pymilvus` search on the same probe vector. A metric mismatch between the DDL
and the index produces plausible but wrongly ranked results, and nothing errors.

## Committing this to git

The repo is ready to initialise as-is. `.gitignore` already excludes `.env`, `volumes/` and
`target/`.

```bash
cd flink-connector-milvus
git init -b main
git add -A
git status --short          # confirm .env is NOT listed
git commit -m "Milvus vector-search connector for Flink 2.x with Docker deployment"
```

Before the first push, two checks worth doing once:

```bash
# 1. No key anywhere in the tree. Should print nothing.
grep -rn "sk-proj-\|sk-[A-Za-z0-9]\{20,\}" . --exclude-dir=.git --exclude=.env || echo "clean"

# 2. No key in what you are about to commit.
git diff --cached | grep -c "sk-" || echo "clean"
```

`curl -o LICENSE https://www.apache.org/licenses/LICENSE-2.0.txt` if you intend to keep the
`org.apache.flink` Java package or contribute this upstream.

`.github/workflows/build.yml` runs on push: it builds, runs the tests, compiles against both Milvus
SDK generations, and asserts the three packaging invariants (class count, SPI entry, no unrelocated
dependencies). Those three are not hypothetical — each one broke this project during development,
and each produced a jar that installed cleanly and did nothing. CI is the right place for them.

`.dockerignore` keeps `volumes/` and `target/` out of the build context. Without it, the context
grows with your Milvus data: 40 MB of test data alone took the context from 272 KB to 71 MB, and it
is re-uploaded to the daemon on every build.

## Before trusting it

1. Run against a live Milvus: verify hit ordering matches a direct `pymilvus` search on the same
   probe, and that scores agree. A metric mismatch between DDL and index produces plausible but
   wrong rankings — the failure mode most likely to reach production unnoticed.
2. Check the shaded jar has no gRPC/Netty/protobuf leakage:
   `unzip -l target/*.jar | grep -E "^ .*(io/grpc|io/netty|com/google/protobuf)/" | grep -v shaded`
3. Test the null-probe path (embedding model returns null) under both inner and `LEFT JOIN LATERAL`.
4. Kill a task manager mid-stream and confirm recovery reopens the client cleanly.
