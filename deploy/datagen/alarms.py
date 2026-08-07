"""Synthetic machine alarms -> Kafka topic `machine_alarms`.

This replaces a Flink `datagen` table that fabricated alarms with CASE expressions over
integer indices. That worked, but every change meant editing SQL, the vocabulary was capped
at one phrase per fault, and anything with real structure -- shift patterns, fault bursts,
a machine that degrades over an hour -- was out of reach.

Two properties carried over from the SQL version, because losing either makes the demo
visibly fake:

  1. The ASSET is drawn from the fault, not independently. A conveyor cannot have spindle
     bearing wear, and an audience notices immediately.
  2. The TELEMETRY is drawn from the fault too. A bearing-wear alarm reporting 1.1 mm/s of
     vibration contradicts itself on screen.

One property is new and matters for retrieval quality: each fault has SEVERAL phrasings, and
the operator's report is assembled from a qualifier plus one of them. The embedded text is
therefore not one of ten fixed strings but one of several hundred, which is what makes the
retrieval look like retrieval rather than a lookup table.

`manual_hint` on each fault is NOT sent to Kafka. It records which service-manual section the
fault is supposed to retrieve, so `./run.sh audit` can score retrieval without a human
deciding what "correct" means after the fact.

Tunables, all via environment (see docker-compose.yml):

    KAFKA_BOOTSTRAP   default kafka:9092
    ALARMS_TOPIC      default machine_alarms
    ALARMS_PER_SECOND default 1.0   -- docker-compose.yml sets 0.1; see the ceiling note below
    BURST_PROBABILITY default 0.10  -- chance an alarm becomes a repeating fault
    UNCOVERED_PROBABILITY default 0.15 -- chance of a fault the manual does not cover
    SEED              default unset -- set an integer for reproducible runs

COST AND CAPACITY: every alarm becomes one embedding call plus one chat-completion call
downstream. This generator is the only throttle on both the OpenAI spend and the throughput --
nothing downstream applies backpressure to a producer that just keeps producing.

The measured ceiling is about 0.16 alarms/second, set by the planner call at parallelism 1.
docker-compose.yml therefore ships 0.1. Above the ceiling the failure is quiet: the backlog
grows, planning_lag_ms climbs into the minutes, and the dashboards show stale numbers that
still look plausible. Far above it, OpenAI returns 429, the async operator times out and fails
the task, and the job restart-loops. './run.sh incidents' twice a few minutes apart is the check.

    python3 alarms.py
"""
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("ALARMS_TOPIC", "machine_alarms")
RATE = float(os.environ.get("ALARMS_PER_SECOND", "1.0"))
BURST_P = float(os.environ.get("BURST_PROBABILITY", "0.10"))
UNCOVERED_P = float(os.environ.get("UNCOVERED_PROBABILITY", "0.15"))
SEED = os.environ.get("SEED")

rng = random.Random(int(SEED)) if SEED else random.Random()

# Assets, and the subsystems each one actually has. A fault is only ever raised on a machine
# that could physically have it.
ASSETS = {
    "PRESS-04":  {"line": "LINE-A", "subsystems": {"hydraulic", "electrical", "safety", "pneumatic"}},
    "MILL-11":   {"line": "LINE-A", "subsystems": {"spindle", "hydraulic", "electrical", "toolchange"}},
    "LATHE-02":  {"line": "LINE-B", "subsystems": {"spindle", "electrical", "coolant"}},
    "CNC-07":    {"line": "LINE-B", "subsystems": {"spindle", "electrical", "coolant", "toolchange", "chiller"}},
    "WELD-03":   {"line": "LINE-C", "subsystems": {"electrical", "safety", "pneumatic", "chiller"}},
    "CONV-01":   {"line": "LINE-C", "subsystems": {"conveyor"}},
}

# Qualifiers are how the report reached the terminal, not what is wrong. Keeping them separate
# from the symptom is what gives the cross-product its size.
QUALIFIERS = [
    "Operator reports ",
    "Night shift logged ",
    "Recurring since Monday: ",
    "Line lead escalated ",
    "Second time this week, ",
    "Found on morning walkround: ",
]

# telemetry: (field, low, high) drawn only for the fields this fault actually disturbs.
# Everything else falls back to NOMINAL. `manual_hint` is the expected retrieval target.
FAULTS = [
    {
        "code": "VIB-HIGH", "subsystem": "spindle", "manual_hint": "SM-4.3",
        "phrases": [
            "a rising rumble from the spindle nose that gets worse as it warms up.",
            "the spindle growling under load and the finish going off on long cuts.",
            "vibration through the headstock that you can feel at the handwheel.",
            "a low whine from the spindle bearings that was not there last week.",
        ],
        "telemetry": {"vibration_mms": (6.4, 9.8)},
    },
    {
        "code": "HYD-LOW", "subsystem": "hydraulic", "manual_hint": "SM-5.1",
        "phrases": [
            "the clamp losing grip halfway through the stroke, hydraulic pressure sagging.",
            "hydraulic pressure drooping on the clamp and the part creeping.",
            "the press not holding tonnage, hydraulic gauge falling off during the cycle.",
            "clamp pressure bleeding away as soon as the pump unloads.",
        ],
        "telemetry": {"pressure_bar": (96.0, 122.0)},
    },
    {
        "code": "E-217", "subsystem": "electrical", "manual_hint": "SM-6.2",
        "phrases": [
            "the drive tripping on overcurrent and refusing to reset, motor cable warm to touch.",
            "fault E-217 latching on the axis drive every few cycles.",
            "an overcurrent trip on the drive that will not clear from the panel.",
            "the drive faulting out on overcurrent under acceleration, cable smelling hot.",
        ],
        "telemetry": {"temp_c": (58.0, 78.0)},
    },
    {
        "code": "ENC-LOSS", "subsystem": "electrical", "manual_hint": "SM-6.5",
        "phrases": [
            "the axis losing position intermittently with no overcurrent alarm.",
            "position drift on the axis that clears after a re-home.",
            "the machine losing its datum mid-program for no obvious reason.",
            "intermittent following error with the drive reporting healthy.",
        ],
        "telemetry": {},
    },
    {
        "code": "COOL-HOT", "subsystem": "coolant", "manual_hint": "SM-7.1",
        "phrases": [
            "coolant coming back off the part far hotter than usual, tools dulling fast.",
            "the coolant tank running hot and tool life dropping right off.",
            "steam off the workpiece and the coolant return almost too hot to touch.",
            "coolant temperature climbing through the shift and sizes drifting.",
        ],
        "telemetry": {"temp_c": (86.0, 97.0)},
    },
    {
        "code": "CHILL-CYC", "subsystem": "chiller", "manual_hint": "SM-7.4",
        "phrases": [
            "the chiller cutting in and out constantly and the cabinet running warm.",
            "the chiller short-cycling on high head pressure all morning.",
            "the chiller tripping out and resetting itself every few minutes.",
        ],
        "telemetry": {"temp_c": (84.0, 94.0)},
    },
    {
        "code": "GUARD-OPEN", "subsystem": "safety", "manual_hint": "SM-8.1",
        "phrases": [
            "the front guard switch dropping the cycle at random with the door shut.",
            "a guard interlock fault mid-cycle although the door never moved.",
            "the machine e-stopping on a guard fault with the gate clearly closed.",
        ],
        "telemetry": {},
    },
    {
        "code": "AIR-WET", "subsystem": "pneumatic", "manual_hint": "SM-10.1",
        "phrases": [
            "water blowing through into the air line and rusting the valve block.",
            "moisture carrying over from the dryer and sitting in the manifold.",
            "water in the air lines again, valves getting sticky with it.",
        ],
        "telemetry": {},
    },
    {
        "code": "TOOL-MISS", "subsystem": "toolchange", "manual_hint": "SM-9.2",
        "phrases": [
            "the tool carousel missing the pocket every so often on a change.",
            "the changer fumbling tools and occasionally dropping one.",
            "the magazine not indexing cleanly, tool changes hanging up.",
        ],
        "telemetry": {},
    },
    {
        "code": "BELT-TRACK", "subsystem": "conveyor", "manual_hint": "SM-9.6",
        "phrases": [
            "the belt creeping over to the drive side and rubbing the frame.",
            "the conveyor belt walking off centre and fraying at the edge.",
            "the belt tracking hard to one side no matter how it is adjusted.",
        ],
        "telemetry": {},
    },
]

# Faults the service manual does NOT cover. This is how the demo produces incidents that need a
# human, and it does it honestly rather than by setting a flag: these reports are real plant
# problems, but nothing in a machine service manual addresses them, so retrieval scores low and
# the planner's grounding guard declines to invent an action.
#
# Measured: this class of text scores 0.158 to 0.315 against the corpus, against 0.398 to 0.685
# for covered faults. The `low` threshold sits at 0.35, in the gap. If you add entries here,
# re-run './run.sh audit' -- an "uncovered" fault that accidentally scores high is a worse bug
# than a covered one that scores low, because it produces a confident answer about nothing.
#
# manual_hint is None, which is what marks them as expected-no-match for the audit.
# subsystem is None because these are not machine subsystems -- any asset can report them.
UNCOVERED_FAULTS = [
    {
        "code": "CRANE-PENDANT", "subsystem": None, "manual_hint": None,
        "phrases": [
            "the overhead crane pendant dropping out mid-lift and coming back on its own.",
            "the crane controls going dead for a second while a load is up.",
        ],
        "telemetry": {},
    },
    {
        "code": "FIRE-DOOR", "subsystem": None, "manual_hint": None,
        "phrases": [
            "the fire door on the east wall will not latch properly any more.",
            "the fire door by the stores standing off its catch again.",
        ],
        "telemetry": {},
    },
    {
        "code": "EXTRACT-FAN", "subsystem": None, "manual_hint": None,
        "phrases": [
            "the extraction fan above the weld cell rattling badly since Friday.",
            "the fume extraction over the welding bay making an awful noise.",
        ],
        "telemetry": {},
    },
    {
        "code": "SCANNER-DROP", "subsystem": None, "manual_hint": None,
        "phrases": [
            "the barcode scanner on the packing bench dropping its connection constantly.",
            "the handheld scanner at packing losing pairing every few minutes.",
        ],
        "telemetry": {},
    },
    {
        "code": "RAIL-DAMAGE", "subsystem": None, "manual_hint": None,
        "phrases": [
            "a pallet truck has clipped the guard rail by bay three and bent it.",
            "the guard rail near the loading bay knocked out of line by a forklift.",
        ],
        "telemetry": {},
    },
]

ALL_FAULTS = FAULTS + UNCOVERED_FAULTS

# Readings for a subsystem that is behaving. Faults override only what they disturb, so a
# hydraulic alarm still carries a believable spindle vibration figure.
NOMINAL = {"vibration_mms": (0.6, 2.1), "temp_c": (34.0, 47.0), "pressure_bar": (152.0, 178.0)}

# Which machines can raise a given fault, precomputed once.
ELIGIBLE = {
    f["code"]: (
        list(ASSETS) if f["subsystem"] is None
        else [a for a, m in ASSETS.items() if f["subsystem"] in m["subsystems"]]
    )
    for f in ALL_FAULTS
}


def shift_for(now):
    """A(06-14) B(14-22) C(22-06). Real plants run shifts; alarms cluster differently in each."""
    h = now.hour
    return "A" if 6 <= h < 14 else "B" if 14 <= h < 22 else "C"


def reading(fault, field):
    low, high = fault["telemetry"].get(field) or NOMINAL[field]
    return round(rng.uniform(low, high), 1)


def pick_fault():
    """Mostly covered faults, occasionally one the manual has nothing to say about."""
    if rng.random() < UNCOVERED_P:
        return rng.choice(UNCOVERED_FAULTS)
    return rng.choice(FAULTS)


def build_alarm(fault=None, asset=None):
    fault = fault or pick_fault()
    asset = asset or rng.choice(ELIGIBLE[fault["code"]])
    now = datetime.now()

    return {
        # Carried the whole way through the pipeline and used as the incident primary key in
        # Postgres. Generated here rather than downstream so the same identity survives every
        # hop, which is what lets the status timeline and the latency figures join up.
        "alarm_id": str(uuid.uuid4()),
        "asset_id": asset,
        "line_id": ASSETS[asset]["line"],
        "fault_code": fault["code"],
        # The only field that gets embedded downstream. Telemetry is passed to the model as
        # structured context instead of being folded in here -- digits in the probe vector
        # blur retrieval.
        "alarm_text": rng.choice(QUALIFIERS) + rng.choice(fault["phrases"]),
        "vibration_mms": reading(fault, "vibration_mms"),
        "temp_c": int(reading(fault, "temp_c")),
        "pressure_bar": int(reading(fault, "pressure_bar")),
        "criticality": rng.choices(
            ["line_critical", "standby", "redundant"], weights=[5, 3, 2]
        )[0],
        "shift": shift_for(now),
        # Flink's json format parses TIMESTAMP(3) with timestamp-format.standard = 'SQL' by
        # default, i.e. 'yyyy-MM-dd HH:mm:ss.SSS'. An ISO 'T' separator here silently yields
        # NULL alarm_time, and every SLA downstream goes null with it.
        "alarm_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }


def connect():
    """Kafka may still be forming its cluster when this container starts. Retry rather than
    dying, so `up` does not have to order the services perfectly."""
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=50,
                acks=1,
            )
        except NoBrokersAvailable:
            print(f"waiting for kafka at {KAFKA_BOOTSTRAP} ({attempt}/30)", flush=True)
            time.sleep(2)
    sys.exit(f"kafka at {KAFKA_BOOTSTRAP} never became available")


def dump_mapping():
    """Print fault_code -> expected manual section, for `./run.sh audit`.

    This is the ground truth for retrieval scoring. Keeping it here rather than in the audit
    command means adding a fault updates the expectation in the same edit that creates it.
    """
    print(json.dumps({f["code"]: f["manual_hint"] for f in ALL_FAULTS}))


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--mapping":
        dump_mapping()
        return

    stopping = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stopping.__setitem__("now", True))
    signal.signal(signal.SIGINT, lambda *_: stopping.__setitem__("now", True))

    producer = connect()
    interval = 1.0 / RATE if RATE > 0 else 1.0
    print(
        f"producing to {TOPIC} at {RATE}/s "
        f"({len(FAULTS)} covered + {len(UNCOVERED_FAULTS)} uncovered faults, "
        f"{sum(len(f['phrases']) for f in ALL_FAULTS)} phrasings, {len(QUALIFIERS)} qualifiers) -> "
        f"{sum(len(f['phrases']) for f in ALL_FAULTS) * len(QUALIFIERS)} distinct alarm texts; "
        f"{UNCOVERED_P:.0%} will need a human",
        flush=True,
    )

    sent = 0
    # A burst is the same fault on the same machine repeating: a real deteriorating fault
    # reported several times, not ten unrelated alarms. It gives the demo something that
    # looks like a story rather than uniform noise.
    burst = None
    burst_left = 0

    while not stopping["now"]:
        if burst_left > 0:
            alarm = build_alarm(fault=burst["fault"], asset=burst["asset"])
            burst_left -= 1
        else:
            alarm = build_alarm()
            if rng.random() < BURST_P:
                fault = next(f for f in ALL_FAULTS if f["code"] == alarm["fault_code"])
                burst = {"fault": fault, "asset": alarm["asset_id"]}
                burst_left = rng.randint(2, 4)

        # Keying by asset keeps one machine's alarms in order within a partition, which is
        # what you want if anyone later adds per-asset windowing.
        producer.send(TOPIC, key=alarm["asset_id"], value=alarm)
        sent += 1
        if sent % 25 == 0:
            print(f"sent {sent} alarms", flush=True)
        time.sleep(interval)

    print(f"stopping after {sent} alarms", flush=True)
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
