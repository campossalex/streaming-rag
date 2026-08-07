"""Generate the bulk of the service manual: deploy/seed/corpus_extra.json.

Maintainer tool. It runs on a laptop, writes a file that is committed, and never runs during
a demo. Regenerate only when you want to change the corpus:

    python3 tools/generate_corpus.py

Why generated at all
--------------------
The 14 sections in deploy/seed/seed_milvus.py are hand-written and stay that way: they are what
the ten simulated faults retrieve, and what ./run.sh audit scores against. This file produces
the several hundred sections *around* them.

That reflects a real manual -- most of it is irrelevant to any given alarm -- and it is what
makes the vector search worth demonstrating. Retrieving one of 14 rows is close to brute force;
retrieving one of ~300 semantic near-neighbours is the actual problem.

Two rules the generator must keep, because they are what the demo rests on:

  1. **Never duplicate a hero failure mode.** HERO_MODES below lists the (component, mode) pairs
     the curated sections own. Generating "spindle / bearing wear" again would put a competing
     section in the corpus and the audit would start failing for a reason that is not a bug.

  2. **Every section names a distinct part number, threshold and duration.** This is what proves
     an answer came from retrieval rather than the model's priors, and it has to hold across
     hundreds of rows, not just fourteen. The generator asserts uniqueness before writing.

Determinism is deliberate: a fixed seed means regenerating produces byte-identical output, so a
corpus change shows up as a reviewable diff instead of 300 lines of churn.
"""
import json
import random
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "deploy" / "seed" / "corpus_extra.json"
SEED = 20260807
TARGET = 300

rng = random.Random(SEED)

# The curated sections own these. Anything the generator produces must avoid them, or the corpus
# ends up with two plausible answers for one alarm and the audit fails for the wrong reason.
HERO_MODES = {
    ("spindle", "bearing wear"), ("hydraulic system", "pressure loss"),
    ("servo drive", "overcurrent"), ("encoder", "signal loss"),
    ("coolant", "high temperature"), ("chiller", "condenser fouling"),
    ("guard interlock", "fails to close"), ("compressed air", "moisture carryover"),
    ("tool changer", "misalignment"), ("conveyor belt", "tracking"),
    ("linear guides", "lubrication"), ("hydraulic oil", "contamination"),
    ("emergency stop circuit", "will not reset"), ("pneumatic actuator", "slow to extend"),
}

# Subject matter the UNCOVERED_FAULTS in deploy/datagen/alarms.py claim the manual does not
# cover. Generating a section for any of these makes the generator contradict the generator:
# the alarm says "nothing here covers a crane pendant", the corpus answers with a crane section,
# and the incident stops routing to a human. The first pass generated exactly this and the audit
# caught it -- CRANE-PENDANT scored 0.527 against a crane hoist brake section.
UNCOVERED_TOPICS = ("crane", "scanner", "fume extraction", "guard hinge", "lockout point")

# Categories are fixed to the six the pipeline already routes on. Adding a seventh would send
# those incidents to the default queue in the enrichment CASE, which is worse than not having it.
COMPONENTS = {
    "mechanical": [
        ("ballscrew", "backlash"), ("way covers", "wear"), ("turret clamp", "slippage"),
        ("gearbox", "oil leak"), ("chain drive", "elongation"), ("cam follower", "flat spotting"),
        ("rotary table", "index error"), ("spindle taper", "fretting"),
        ("counterbalance", "drift"), ("tailstock", "alignment"), ("chuck jaws", "grip loss"),
        ("bearing housing", "looseness"), ("drive coupling", "backlash"),
        ("timing belt", "tooth wear"), ("roller bearing", "pitting"),
        ("slideway", "scoring"), ("clamp cylinder", "creep"), ("indexer", "overshoot"),
        ("pallet changer", "seating error"), ("robot gripper", "grip loss"),
        ("palletiser arm", "overshoot"), ("crane hoist brake", "slip"),
        ("shaft seal", "weep"), ("flexible coupling", "misalignment"),
        ("vibration damper", "degradation"), ("turntable bearing", "roughness"),
    ],
    "hydraulic": [
        ("proportional valve", "hysteresis"), ("accumulator", "precharge loss"),
        ("return filter", "bypass"), ("directional valve", "sticking"),
        ("pressure relief valve", "chatter"), ("hydraulic hose", "abrasion"),
        ("pump coupling", "wear"), ("reservoir", "aeration"),
        ("hydraulic cooler", "fouling"), ("servo valve", "null shift"),
        ("cylinder rod seal", "weep"), ("check valve", "leak-back"),
        ("pilot line", "restriction"), ("flow divider", "imbalance"),
        ("suction strainer", "blockage"), ("case drain", "excess flow"),
    ],
    "electrical": [
        ("contactor", "welded contacts"), ("VFD", "DC bus ripple"),
        ("motor winding", "insulation degradation"), ("cable gland", "ingress"),
        ("terminal block", "loose connection"), ("PSU", "output droop"),
        ("resolver", "phase error"), ("proximity sensor", "intermittent output"),
        ("busbar", "hot joint"), ("fieldbus", "CRC errors"),
        ("brake resistor", "overheating"), ("earth bond", "high impedance"),
        ("PLC input card", "channel failure"), ("line reactor", "saturation"),
        ("motor bearing", "electrical erosion"), ("shielding", "ground loop"),
        ("relay", "contact bounce"), ("EMC filter", "leakage"),
    ],
    "thermal": [
        ("spindle cooler", "flow restriction"), ("cabinet air conditioner", "icing"),
        ("heat exchanger", "scaling"), ("oven element", "open circuit"),
        ("thermocouple", "drift"), ("cooling fan", "bearing failure"),
        ("quench tank", "temperature rise"), ("induction coil", "cooling loss"),
        ("hot runner", "zone imbalance"), ("radiator core", "blockage"),
        ("insulation panel", "degradation"), ("process water loop", "temperature swing"),
    ],
    "safety": [
        ("light curtain", "muting fault"), ("two-hand control", "asynchrony"),
        ("safety relay", "cross fault"), ("door lock solenoid", "sticking"),
        ("pressure mat", "dead zone"), ("safety PLC", "diagnostic fault"),
        ("rope pull switch", "tension loss"), ("area scanner", "contamination"),
        ("overtravel limit", "false trip"), ("guard hinge", "sag"),
        ("fume extraction", "flow loss"), ("lockout point", "unlabelled"),
    ],
    "pneumatic": [
        ("air dryer", "dew point rise"), ("FRL unit", "bowl contamination"),
        ("solenoid valve", "slow shift"), ("vacuum generator", "loss of vacuum"),
        ("air cylinder", "cushion loss"), ("quick exhaust valve", "restriction"),
        ("manifold gasket", "leak"), ("silencer", "clogging"),
        ("pressure switch", "setpoint drift"), ("air receiver", "condensate build-up"),
        ("blow-off nozzle", "misdirection"), ("regulator", "creep"),
    ],
}

# The measurable is chosen from the FAILURE MODE, not the category. Picking per category
# produced "air cylinder cushion loss ... rises above -18 degrees C" and "roller bearing pitting
# ... falls below 67 Nm" -- units that a maintenance engineer spots as nonsense on sight, which
# defeats the point of a realistic corpus. Keywords are matched against the mode text; the
# category entry is only a fallback for modes that match nothing.
MODE_MEASURES = [
    (("wear", "backlash", "elongation", "misalignment", "sag", "scoring", "fretting",
      "alignment", "index error", "overshoot", "seating error", "dead zone"),
     ("mm", 0.02, 0.9, "exceeds")),
    (("pitting", "roughness", "flat spotting", "degradation", "looseness"),
     ("mm/s RMS", 2.8, 9.5, "rises above")),
    (("grip loss", "slippage", "slip", "tension loss", "creep", "drift"),
     ("Nm", 25, 220, "falls below")),
    (("leak", "weep", "bypass", "leak-back", "oil leak", "excess flow", "leakage"),
     ("ml/min", 5, 90, "exceeds")),
    (("pressure loss", "precharge loss", "setpoint drift", "hysteresis", "null shift",
      "chatter", "sticking", "imbalance"),
     ("bar", 3, 240, "falls below")),
    (("overheating", "temperature rise", "dew point rise", "temperature swing",
      "zone imbalance", "cooling loss"),
     ("degrees C", -20, 190, "climbs past")),
    (("insulation degradation", "high impedance", "ingress"),
     ("megohm", 1, 60, "falls below")),
    (("overcurrent", "ripple", "hot joint", "output droop", "saturation", "electrical erosion"),
     ("A", 2, 180, "exceeds")),
    (("phase error", "intermittent output", "CRC errors", "channel failure", "contact bounce",
      "ground loop", "loose connection", "welded contacts"),
     ("mV", 8, 400, "exceeds")),
    (("slow shift", "asynchrony", "cross fault", "diagnostic fault", "muting fault",
      "false trip", "slow to extend"),
     ("ms", 40, 480, "exceeds")),
    (("flow restriction", "restriction", "blockage", "loss of vacuum", "flow loss", "clogging",
      "fouling", "scaling", "contamination", "aeration", "condensate build-up",
      "bowl contamination", "cushion loss"),
     ("l/min", 3, 90, "drops under")),
    (("open circuit",), ("megohm", 5, 90, "exceeds")),
    (("bearing failure",), ("mm/s RMS", 3.2, 11.0, "rises above")),
    # Icing is a cold fault. Sharing the general temperature band produced "icing ... climbs
    # past 186 degrees C", which is self-contradicting on its face.
    (("icing",), ("degrees C", -18, 2, "falls below")),
]

MEASURES = {
    "mechanical": ("mm", 0.02, 0.40, "exceeds"),
    "hydraulic": ("bar", 60, 240, "falls below"),
    "electrical": ("V", 12, 640, "sags below"),
    "thermal": ("degrees C", 35, 190, "climbs past"),
    "safety": ("ms", 40, 480, "exceeds"),
    "pneumatic": ("bar", 3, 11, "falls below"),
}


def measure_for(mode, category):
    for keys, m in MODE_MEASURES:
        if any(k in mode for k in keys):
            return m
    return MEASURES[category]


PART_PREFIX = {"mechanical": "MK", "hydraulic": "HK", "electrical": "EK",
               "thermal": "TK", "safety": "SK", "pneumatic": "PK"}

CAUSES = [
    "a worn {c} rather than the control system",
    "normal service wear reaching its limit",
    "contamination tracking in from the surrounding process",
    "a fastening that has relaxed under thermal cycling",
    "an upstream supply problem rather than the {c} itself",
    "misalignment introduced at the last rebuild",
    "fatigue at the mounting rather than the {c} body",
    "a setting that has drifted since commissioning",
]

REMEDIES = [
    "Replace {c} using kit {p}",
    "Fit replacement {p} and re-run the commissioning check",
    "Change {p} as a set; matched pairs must not be mixed with used items",
    "Renew {p} and re-datum before returning the asset to production",
    "Swap {p} and check against the acceptance figure in this chapter",
    "Install {p}, then leave the machine on a dry cycle before loading it",
]

SAFETY = [
    "Isolate and lock off before removing any cover",
    "Stored energy remains after shutdown; prove dead before starting",
    "Do not defeat the interlock to gain access",
    "Two people are required for this lift",
    "Hot surfaces remain above 60 degrees C for some time after stopping",
    "Wear eye protection: the assembly is under spring load",
    None, None,
]

VERIFY = [
    "confirm the figure has returned inside tolerance",
    "log the new reading in the machine book",
    "repeat the measurement after fifty operating hours",
    "check the trend again on the next shift",
]

DURATIONS = ["fifteen minutes", "twenty minutes", "twenty five minutes", "thirty minutes",
             "forty minutes", "forty five minutes", "an hour", "seventy minutes",
             "ninety minutes", "two hours", "half a shift"]


def prose(component, mode, category, part, measure, severity):
    """Three sentence shapes, chosen per section, so the corpus does not read as one template."""
    unit, lo, hi, verb = measure
    value = round(rng.uniform(lo, hi), 2 if unit in ("mm", "megohm") else 0)
    value = int(value) if float(value).is_integer() else value
    cause = rng.choice(CAUSES).format(c=component)
    remedy = rng.choice(REMEDIES).format(c=component, p=part)
    safety = rng.choice(SAFETY)
    verify = rng.choice(VERIFY)
    duration = rng.choice(DURATIONS)
    stop = " Expect a line stop." if severity >= 4 else ""

    shape = rng.randrange(3)
    if shape == 0:
        body = (f"{component.capitalize()} {mode} is confirmed when the measured value {verb} "
                f"{value} {unit}. The usual cause is {cause}. {remedy}, then {verify}.")
    elif shape == 1:
        body = (f"When {component} {mode} appears, the reading {verb} {value} {unit} before any "
                f"other symptom shows. Treat it as {cause}. {remedy}.")
    else:
        body = (f"On the {component}, {mode} presents as a reading that {verb} "
                f"{value} {unit}. It is {cause}. {remedy}, then {verify}.")

    tail = f" {safety}." if safety else ""
    return f"{body}{tail} Allow {duration}.{stop}"


def main():
    sections, used_parts, used_refs, used_titles = [], set(), set(), set()
    chapter = {c: 11 for c in COMPONENTS}  # curated sections use chapters 3-10; start clear of them
    counter = {c: 0 for c in COMPONENTS}

    pool = [(cat, comp, mode)
            for cat, items in COMPONENTS.items()
            for comp, mode in items
            if (comp, mode) not in HERO_MODES
            and not any(t in comp for t in UNCOVERED_TOPICS)]
    rng.shuffle(pool)

    i = 0
    while len(sections) < TARGET:
        cat, comp, mode = pool[i % len(pool)]
        variant = i // len(pool)
        i += 1

        # Later passes over the pool re-use the component with a different aspect, so the corpus
        # grows past the vocabulary size without repeating a title.
        aspect = ["", " under load", " after a cold start", " at high duty",
                  " during changeover", " on the return stroke"][variant % 6]
        title = f"{comp.capitalize()} {mode}{aspect}"
        if title in used_titles:
            continue
        used_titles.add(title)

        counter[cat] += 1
        if counter[cat] % 9 == 0:
            chapter[cat] += 1
        doc_ref = f"SM-{chapter[cat]}.{counter[cat] % 9 + 1}-{PART_PREFIX[cat]}{counter[cat]:03d}"
        if doc_ref in used_refs:
            continue
        used_refs.add(doc_ref)

        part = f"{PART_PREFIX[cat]}-{rng.randrange(1000, 9999)}"
        while part in used_parts:
            part = f"{PART_PREFIX[cat]}-{rng.randrange(1000, 9999)}"
        used_parts.add(part)

        severity = rng.choices([1, 2, 3, 4], weights=[3, 4, 2, 1])[0]
        content = prose(comp, mode, cat, part, measure_for(mode, cat), severity)
        sections.append([content, cat, title, doc_ref, severity])

    assert len(used_parts) == len(sections), "part numbers must be unique across the corpus"
    assert len(used_refs) == len(sections), "doc refs must be unique across the corpus"
    OUT.write_text(json.dumps(sections, indent=1) + "\n")

    by_cat = {}
    for _, cat, *_ in sections:
        by_cat[cat] = by_cat.get(cat, 0) + 1
    print(f"wrote {len(sections)} sections to {OUT.relative_to(OUT.parents[2])}")
    for cat, n in sorted(by_cat.items()):
        print(f"  {cat:<12}{n}")


if __name__ == "__main__":
    main()
