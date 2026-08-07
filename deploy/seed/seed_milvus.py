"""
Creates the maintenance_manual collection, embeds the corpus, inserts it, and loads the
collection into memory. Idempotent: drops and recreates on every run.

    docker compose run --rm seeder
"""
import json
import os
import sys
from pathlib import Path

from pymilvus import DataType, MilvusClient

MILVUS_URI = os.environ.get("MILVUS_URI", "http://milvus-standalone:19530")
COLLECTION = os.environ.get("MILVUS_COLLECTION", "maintenance_manual")
OPENAI_BASE = os.environ.get("OPENAI_BASE", "https://api.openai.com/v1")
OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "ollama")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "text-embedding-3-small")
EMBED_DIM = int(os.environ.get("EMBED_DIM", "1536"))

# Sections of a machine service manual, the kind of text a maintenance engineer actually reads.
# (content, category, section, doc_ref, severity)
#
# Only `content` is embedded. section/doc_ref/severity are retrieval metadata: they ride along
# on the hit and drive the SQL enrichment downstream, but they are not semantic signal and
# would only dilute the vector.
#
# Two properties are load-bearing for the demo and must survive any edit:
#
#  1. Every section names a DISTINCT part number, threshold and duration. That is what proves
#     the LLM's answer came from the retrieved document rather than from its own priors -- if
#     two sections both said "replace the seal kit", a wrong retrieval would still look right.
#  2. Sections are written to be discriminable by embedding. Vocabulary that is unique to one
#     fault (RMS, particle count, phase current, dew point) is deliberate; it is what pulls the
#     correct section to rank 1.
#
# severity is the manual's own view of how bad the fault is, 1 (routine) to 4 (stop the line).
# It is combined with the event's `criticality` downstream to produce a work-order priority.
DOCS = [
    ("Grease the linear guides and ballscrew every 500 operating hours using NLGI 2 lithium "
     "complex, part LG-1140. Two strokes per nipple is sufficient; over-greasing forces the "
     "wiper seals off their seats. Log the hours meter reading in the machine book. Allow "
     "fifteen minutes and no production interruption.",
     "mechanical", "Lubrication schedule", "SM-3.2", 1),

    ("Spindle vibration above 7.1 mm/s RMS measured at the front housing indicates progressive "
     "bearing wear. Replace bearing kit BK-8842 as a complete set and never reuse the inner "
     "race. Lock out the spindle and let it cool below 40 C before removal. Allow ninety "
     "minutes and expect a line stop.",
     "mechanical", "Spindle bearing wear", "SM-4.3", 3),

    ("A tool changer that misses the pocket on more than one exchange in fifty has drifted out "
     "of alignment. Re-datum the carousel against gauge pin GP-70 and retighten the cam "
     "follower to 45 Nm. Do not file the pocket. Allow forty minutes with the magazine empty.",
     "mechanical", "Tool changer misalignment", "SM-9.2", 2),

    ("A conveyor belt that walks toward one frame edge is usually running on a skewed tail "
     "pulley rather than a stretched belt. Adjust the take-up bolts a quarter turn at a time "
     "and run for five minutes between adjustments. Replacement belt is CB-3300. Allow twenty "
     "five minutes.",
     "mechanical", "Conveyor belt tracking", "SM-9.6", 1),

    ("Hydraulic system pressure falling below 120 bar during the clamp stroke, so the press will "
     "not hold tonnage, usually traces to the main pump shaft seal. Fit seal kit HS-3310 and "
     "refill with ISO VG 46 hydraulic oil. Bleed the accumulator before opening any line: "
     "stored pressure remains after the pump stops and has caused injuries. Allow two hours.",
     "hydraulic", "Hydraulic pressure loss", "SM-5.1", 3),

    ("Particle counts worse than ISO 18/16/13 accelerate spool and valve wear across the whole "
     "circuit. Change return filter element RF-220, take a fresh sample after fifty operating "
     "hours, and investigate the breather if the count climbs again. Allow thirty five minutes.",
     "hydraulic", "Hydraulic oil contamination", "SM-5.4", 2),

    ("Fault E-217 latches when the servo drive measures phase current above 150 percent of "
     "rated for longer than two seconds. Do not clear and restart repeatedly. Megger the motor "
     "cable for insulation breakdown and turn the axis by hand to rule out a mechanical jam "
     "before re-energising. Spare drive module is DM-4400. Allow seventy minutes.",
     "electrical", "Servo drive overcurrent, fault E-217", "SM-6.2", 4),

    ("Intermittent position loss with no overcurrent points at the encoder feedback path rather "
     "than the drive. Reseat connector X4, check the shield is bonded at the cabinet end only, "
     "and replace encoder EN-1602 if the fault follows the axis. Allow fifty minutes.",
     "electrical", "Encoder signal loss", "SM-6.5", 3),

    ("Coolant leaving the manifold above 85 C shortens tool life sharply and will distort "
     "close-tolerance work. Confirm the pump is delivering full flow, clean the strainer, and "
     "top up to the sight glass with a 6 percent emulsion. Allow twenty minutes; production "
     "may continue at reduced feed.",
     "thermal", "Coolant temperature high", "SM-7.1", 2),

    ("A chiller that short-cycles on high head pressure almost always has a dust-blinded "
     "condenser coil. Isolate, comb the fins and wash from the clean side outward. Replacement "
     "filter mat is FM-905. Never run the chiller with the panel removed. Allow forty five "
     "minutes.",
     "thermal", "Chiller condenser fouling", "SM-7.4", 2),

    ("Any guard interlock opening mid-cycle triggers a category 1 stop and must be treated as a "
     "genuine safety event. The machine may not be returned to service by bypassing, taping or "
     "defeating the switch under any circumstance. Replace switch GI-220 like for like and "
     "record the test. Allow thirty minutes.",
     "safety", "Guard interlock open", "SM-8.1", 4),

    ("An emergency stop circuit that will not reset indicates a welded contact or a broken loop "
     "conductor, not a nuisance trip. Test continuity around the full loop including the "
     "pendant. Replace relay ES-115 as a pair and re-validate the stop time before releasing "
     "the machine. Allow sixty minutes and expect a line stop.",
     "safety", "Emergency stop circuit fault", "SM-8.3", 4),

    ("Water carryover into the air manifold, seen as a dew point above 3 C at the dryer outlet, "
     "corrodes valve bodies and washes grease out of the actuators. Drain the receiver, service "
     "the dryer and fit desiccant cartridge DC-410. Allow forty minutes.",
     "pneumatic", "Compressed air moisture", "SM-10.1", 2),

    ("An actuator that extends sluggishly with correct line pressure has usually lost its "
     "cushion seal or has a throttled exhaust. Check the flow control is not wound fully in, "
     "then fit seal set AS-560. Allow twenty five minutes; the station can be jumped out for "
     "one shift.",
     "pneumatic", "Pneumatic actuator slow to extend", "SM-10.4", 1),
]


# The bulk of the manual, generated by tools/generate_corpus.py and committed alongside this
# file. Kept separate from DOCS above on purpose: those 14 are what the simulated faults
# retrieve and what ./run.sh audit scores, so they are reviewed by hand. This is the realistic
# body around them -- mostly irrelevant to any given alarm, which is exactly the point. Missing
# file is not an error; the demo works on the curated 14 alone.
def load_extra():
    # CORPUS_EXTRA=0 seeds only the curated 14. That is not just a size knob: at 14 sections
    # retrieval is exact (35/35 top-1) and the confidence gate separates cleanly. At 314 it is
    # 24/35, because a realistic manual contains several sections that plausibly answer the same
    # complaint. Use the small corpus when demonstrating the pipeline, the large one when
    # demonstrating that retrieval quality is a real problem.
    if os.environ.get("CORPUS_EXTRA", "1") == "0":
        print("CORPUS_EXTRA=0; seeding the curated sections only")
        return []
    path = Path(__file__).with_name("corpus_extra.json")
    if not path.exists():
        print("corpus_extra.json not found; seeding the curated sections only")
        return []
    extra = [tuple(row) for row in json.loads(path.read_text())]
    print(f"loaded {len(extra)} generated sections from corpus_extra.json")
    return extra


# One request per 100 documents. The embeddings endpoint takes an array, but a few hundred
# sections in a single call risks the per-request token ceiling, and a batch that fails takes
# the whole seed with it rather than one chunk.
BATCH = 100


def embed(texts):
    import openai
    from openai import OpenAI

    client = OpenAI(base_url=OPENAI_BASE, api_key=OPENAI_KEY)
    if len(texts) > BATCH:
        out = []
        for i in range(0, len(texts), BATCH):
            chunk = texts[i:i + BATCH]
            print(f"  embedding {i + 1}-{i + len(chunk)} of {len(texts)}")
            out.extend(embed(chunk))
        return out
    try:
        response = client.embeddings.create(model=EMBED_MODEL, input=texts)
    except openai.AuthenticationError:
        prefix = OPENAI_KEY[:8] if OPENAI_KEY else "(empty)"
        sys.exit(
            f"\nThe API rejected the key (401).\n"
            f"  key starts with: {prefix}...  length: {len(OPENAI_KEY)}\n"
            f"  endpoint:        {OPENAI_BASE}\n\n"
            + ("  This key does not start with 'sk-', so it is not an OpenAI key. A common\n"
               "  cause is copying by double-click, which selects only one hyphen-delimited\n"
               "  segment: 'sk-proj-ABC...' copies as just 'ABC...'.\n\n"
               if not OPENAI_KEY.startswith("sk-") and "api.openai.com" in OPENAI_BASE else "")
            + "  Fix it with:  ./run.sh set-key      then re-run:  ./run.sh seed\n"
              "  Or go fully local with no key:  ./run.sh use-ollama\n"
        )
    except openai.NotFoundError:
        sys.exit(
            f"\nThe endpoint returned 404 for model '{EMBED_MODEL}' at {OPENAI_BASE}.\n"
            f"  If using Ollama, pull it first: docker compose exec ollama ollama pull {EMBED_MODEL}\n"
        )
    vectors = [item.embedding for item in response.data]
    actual = len(vectors[0])
    if actual != EMBED_DIM:
        sys.exit(
            f"EMBED_DIM is {EMBED_DIM} but {EMBED_MODEL} returned {actual} dimensions. "
            f"Set EMBED_DIM={actual} in .env and re-run. A dimension mismatch between the "
            f"collection and the query vector is the most common cause of retrieval failures."
        )
    return vectors


def main():
    client = MilvusClient(uri=MILVUS_URI)

    if client.has_collection(COLLECTION):
        print(f"dropping existing collection {COLLECTION}")
        client.drop_collection(COLLECTION)

    # Field NAMES here are the contract with the Flink DDL. MilvusRowConverter looks each
    # declared column up by name (entity.get(fieldNames[i])), and the table source requests
    # every declared column as an output field -- so a column in the DDL with no matching
    # field here fails at query time, not at seed time. Order is free; spelling is not.
    #
    # Types are constrained by the connector: BOOLEAN, TINYINT/SMALLINT/INT/BIGINT, FLOAT,
    # DOUBLE, VARCHAR and ARRAY of those. There is no TIMESTAMP or DECIMAL converter, which is
    # why anything date-like on a corpus row has to be a VARCHAR.
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("content", DataType.VARCHAR, max_length=4096)
    schema.add_field("category", DataType.VARCHAR, max_length=64)
    schema.add_field("section", DataType.VARCHAR, max_length=128)
    schema.add_field("doc_ref", DataType.VARCHAR, max_length=32)
    schema.add_field("severity", DataType.INT32)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=EMBED_DIM)

    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="vec",
        index_type="HNSW",
        # Must match 'search.metric' in the Flink DDL. A mismatch does not error --
        # it silently returns plausible but wrongly ranked results.
        metric_type="COSINE",
        params={"M": 16, "efConstruction": 200},
    )

    client.create_collection(
        collection_name=COLLECTION, schema=schema, index_params=index_params
    )

    corpus = DOCS + load_extra()
    print(f"embedding {len(corpus)} documents with {EMBED_MODEL} via {OPENAI_BASE}")
    # Only the prose is embedded. Prepending "SM-4.3 Spindle bearing wear" to the embedded
    # string would let a query match on the section label rather than on what the section says.
    vectors = embed([content for content, _, _, _, _ in corpus])
    client.insert(
        collection_name=COLLECTION,
        data=[
            {
                "id": i,
                "content": content,
                "category": category,
                "section": section,
                "doc_ref": doc_ref,
                "severity": severity,
                "vec": vec,
            }
            for i, ((content, category, section, doc_ref, severity), vec) in enumerate(
                zip(corpus, vectors)
            )
        ],
    )

    # get_collection_stats only counts sealed segments, so without a flush the summary below
    # reports row_count 0 on a collection that was in fact seeded correctly.
    client.flush(COLLECTION)

    # Milvus refuses searches on a collection that is not loaded into memory.
    client.load_collection(COLLECTION)
    stats = client.get_collection_stats(COLLECTION)
    print(f"seeded {COLLECTION}: {stats} (dim={EMBED_DIM}, metric=COSINE), collection loaded")


if __name__ == "__main__":
    main()
