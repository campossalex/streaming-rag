"""Incident management for the maintenance planner.

The pipeline resolves most alarms on its own. This exists for the ones it cannot: retrieval
found nothing in the manual (NEEDS_CONTEXT), or the planner model declined to invent an action
(NEEDS_REVIEW). Those are the incidents a human has to finish, and they are what this UI puts
first -- everything else is a list you scroll past.

Two rules shape the code:

  1. **Every state change appends to incident_status.** The UI never mutates state silently.
     `incident_current` derives the current status from the last row of that log, so a status
     that is not in the log did not happen.

  2. **This writes only human-owned columns** -- assignee, manual_action, resolution_notes.
     The Flink JDBC sinks declare neither, which is what stops a late pipeline write from
     nulling a planner's work. Adding a column here means checking it is absent from both
     Flink DDLs in deploy/sql/ddl/05_postgres.sql.

Follows the shape of the sibling lab-day services: single file, one template constant rendered
with render_template_string, psycopg2 with RealDictCursor, a connection per request and no
pooling. At demo scale that is simpler than a pool and fails in more obvious ways.

    python3 app.py
"""
import json
import os

import psycopg2
import psycopg2.extras
from flask import Flask, redirect, render_template_string, request, url_for

PG = dict(
    host=os.getenv("PG_HOST", "postgres"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DATABASE", "incidents"),
    user=os.getenv("PG_USER", "root"),
    password=os.getenv("PG_PASSWORD", "admin1"),
)
PORT = int(os.getenv("PORT", "9052"))
PLANNER = os.getenv("PLANNER_NAME", "planner")

app = Flask(__name__)


# ── Data access ────────────────────────────────────────────────────────────────
def db():
    return psycopg2.connect(cursor_factory=psycopg2.extras.RealDictCursor, **PG)


def query(sql, args=()):
    with db() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def execute(sql, args=()):
    with db() as conn, conn.cursor() as cur:
        cur.execute(sql, args)


def transition(incident_id, status, note=None, **columns):
    """Append a status row and optionally set human-owned columns, in one transaction.

    Both halves must land together: a status of ASSIGNED with no assignee, or an assignee with
    no audit trail, are each worse than the change not happening.
    """
    with db() as conn, conn.cursor() as cur:
        if columns:
            sets = ", ".join(f"{c} = %s" for c in columns)
            cur.execute(
                f"UPDATE public.incident SET {sets} WHERE incident_id = %s",
                (*columns.values(), incident_id),
            )
        cur.execute(
            "INSERT INTO public.incident_status (incident_id, status, actor, note) "
            "VALUES (%s, %s, %s, %s)",
            (incident_id, status, PLANNER, note),
        )


def fmt_delta(ms):
    """Gap since the previous status, scaled to whatever unit reads naturally.

    The pipeline steps are milliseconds apart and the human ones are minutes or hours apart, so
    a single unit is unreadable at one end or the other. Showing the gap rather than doing the
    subtraction in your head is the whole point of the timeline: it is where you see that
    retrieval took 300 ms and the planner call took 1.4 s.
    """
    if ms < 1000:
        return f"+{int(round(ms))} ms"
    if ms < 60_000:
        return f"+{ms / 1000:.1f} s"
    if ms < 3_600_000:
        return f"+{ms / 60_000:.0f} min"
    if ms < 86_400_000:
        return f"+{ms / 3_600_000:.1f} h"
    return f"+{ms / 86_400_000:.1f} d"


def with_deltas(rows):
    """Annotate each status with the gap since the one before it. First row has none."""
    previous = None
    for r in rows:
        r["delta"] = None if previous is None else fmt_delta(
            (r["at"] - previous).total_seconds() * 1000)
        previous = r["at"]
    return rows


def pretty_work_order(raw):
    """The planner model returns JSON as text. Show it structured when it parses, raw when it
    does not -- a malformed response is a thing the planner needs to see, not something to hide."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    ok = True
    try:
        query("SELECT 1")
    except Exception:
        ok = False
    return {"status": "ok" if ok else "degraded", "postgres": ok}, (200 if ok else 503)


@app.get("/")
def index():
    view = request.args.get("view", "attention")

    where = {
        # The default. Anything the pipeline could not finish, oldest first, because an
        # incident nobody has looked at is the one most likely to breach.
        "attention": "WHERE needs_human AND status NOT IN ('RESOLVED','CLOSED','REJECTED')",
        "open": "WHERE status NOT IN ('RESOLVED','CLOSED','REJECTED')",
        "breached": "WHERE sla_breached",
        "all": "",
    }.get(view, "")

    rows = query(
        f"""SELECT incident_id, asset_id, line_id, fault_code, alarm_text, priority, queue,
                   doc_ref, matched_section, match_confidence, score, status, needs_human,
                   sla_breached, sla_due, assignee, total_lag_ms, t_alarm
            FROM public.incident_current {where}
            ORDER BY needs_human DESC,
                     CASE priority WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
                     sla_due NULLS LAST
            LIMIT 200"""
    )
    counts = query(
        """SELECT
             count(*) FILTER (WHERE needs_human AND status NOT IN ('RESOLVED','CLOSED','REJECTED')) AS attention,
             count(*) FILTER (WHERE status NOT IN ('RESOLVED','CLOSED','REJECTED')) AS open,
             count(*) FILTER (WHERE sla_breached) AS breached,
             count(*) AS all
           FROM public.incident_current"""
    )[0]
    return render_template_string(LIST_HTML, rows=rows, counts=counts, view=view)


@app.get("/incident/<incident_id>")
def detail(incident_id):
    found = query(
        "SELECT * FROM public.incident_current WHERE incident_id = %s", (incident_id,)
    )
    if not found:
        return "Unknown incident", 404
    timeline = with_deltas(query(
        "SELECT status, actor, note, at FROM public.incident_status "
        "WHERE incident_id = %s ORDER BY at, id",
        (incident_id,),
    ))
    return render_template_string(
        DETAIL_HTML,
        i=found[0],
        timeline=timeline,
        wo=pretty_work_order(found[0].get("ai_work_order")),
    )


@app.post("/incident/<incident_id>/action")
def act(incident_id):
    what = request.form.get("what", "")
    note = (request.form.get("note") or "").strip() or None

    if what == "assign":
        who = (request.form.get("assignee") or "").strip()
        if who:
            transition(incident_id, "ASSIGNED", f"assigned to {who}", assignee=who)
    elif what == "define":
        # The point of NEEDS_REVIEW: the model would not ground an action, so a human writes one.
        action = (request.form.get("manual_action") or "").strip()
        if action:
            transition(incident_id, "ACTION_DEFINED", "action written by hand",
                       manual_action=action)
    elif what in ("IN_PROGRESS", "REJECTED"):
        transition(incident_id, what, note)
    elif what in ("RESOLVED", "CLOSED"):
        transition(incident_id, what, note, resolution_notes=note)

    return redirect(url_for("detail", incident_id=incident_id))


# ── Templates ──────────────────────────────────────────────────────────────────
BASE_CSS = """
:root{--bg:#0f1117;--card:#171a21;--line:#262b36;--fg:#e6e8ee;--mut:#8b93a7;
      --p1:#f2555a;--p2:#f2a03d;--p3:#4a9de0;--p4:#5c6478;--warn:#f2a03d;--ok:#46b26b}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
     font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
a{color:inherit;text-decoration:none}
.wrap{max-width:1180px;margin:0 auto;padding:24px}
h1{font-size:19px;font-weight:500;margin:0 0 4px}
h2{font-size:14px;font-weight:500;margin:0 0 10px;color:var(--mut)}
.sub{color:var(--mut);margin-bottom:18px}
.tabs{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.tab{padding:6px 12px;border:1px solid var(--line);border-radius:6px;color:var(--mut)}
.tab.on{background:var(--card);color:var(--fg);border-color:#39404f}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin-bottom:14px}
table{width:100%;border-collapse:collapse}
th{text-align:left;font-weight:500;color:var(--mut);font-size:12px;
   border-bottom:1px solid var(--line);padding:8px 10px}
td{padding:9px 10px;border-bottom:1px solid var(--line);vertical-align:top}
tr:last-child td{border-bottom:none}
tr.row:hover{background:#1c2029}
.pill{display:inline-block;padding:1px 7px;border-radius:999px;font-size:11px;border:1px solid var(--line)}
.P1{color:var(--p1);border-color:var(--p1)}.P2{color:var(--p2);border-color:var(--p2)}
.P3{color:var(--p3);border-color:var(--p3)}.P4{color:var(--p4)}
.flag{color:var(--warn);border-color:var(--warn)}
.mut{color:var(--mut)}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:14px}
.kv b{display:block;color:var(--mut);font-weight:400;font-size:12px}
input,textarea,select{background:#10131a;border:1px solid var(--line);color:var(--fg);
       border-radius:6px;padding:7px 9px;font:inherit;width:100%}
textarea{min-height:74px;resize:vertical}
button{background:#2b3140;border:1px solid #39404f;color:var(--fg);border-radius:6px;
       padding:7px 13px;font:inherit;cursor:pointer}
button:hover{background:#343b4c}
form.inline{display:flex;gap:8px;align-items:flex-start;margin-top:8px}
.tl{list-style:none;margin:0;padding:0}
.tl li{padding:7px 0 7px 14px;border-left:2px solid var(--line);position:relative}
.tl li:before{content:"";position:absolute;left:-5px;top:14px;width:8px;height:8px;
              border-radius:50%;background:#39404f}
.tl li.hum:before{background:var(--warn)}
.delta{color:var(--p3);font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;
       margin:0 6px}
.empty{color:var(--mut);padding:22px;text-align:center}
"""

LIST_HTML = """<!doctype html><meta charset=utf-8><title>Incidents</title>
<style>""" + BASE_CSS + """</style>
<div class=wrap>
  <h1>Maintenance incidents</h1>
  <div class=sub>Alarms the pipeline could not finish are listed first.</div>
  <div class=tabs>
    <a class="tab {{ 'on' if view=='attention' }}" href="/?view=attention">Needs a human ({{counts.attention}})</a>
    <a class="tab {{ 'on' if view=='open' }}" href="/?view=open">Open ({{counts.open}})</a>
    <a class="tab {{ 'on' if view=='breached' }}" href="/?view=breached">SLA breached ({{counts.breached}})</a>
    <a class="tab {{ 'on' if view=='all' }}" href="/?view=all">All ({{counts.all}})</a>
  </div>
  <div class=card>
  {% if rows %}
  <table>
    <tr><th>Pri</th><th>Asset</th><th>Alarm</th><th>Retrieved</th>
        <th>Status</th><th>Assignee</th><th>Lag</th></tr>
    {% for r in rows %}
    <tr class=row onclick="location='/incident/{{r.incident_id}}'" style=cursor:pointer>
      <td><span class="pill {{r.priority}}">{{r.priority or '--'}}</span></td>
      <td>{{r.asset_id}}<div class="mut mono">{{r.line_id}}</div></td>
      <td>{{ r.alarm_text[:74] }}{% if r.alarm_text|length > 74 %}...{% endif %}
          <div class="mut mono">{{r.fault_code}}</div></td>
      <td>{% if r.doc_ref %}<span class=mono>{{r.doc_ref}}</span>
          <div class=mut>{{r.match_confidence}} &middot; {{ '%.3f'|format(r.score or 0) }}</div>
          {% else %}<span class=mut>none</span>{% endif %}</td>
      <td>{% if r.needs_human %}<span class="pill flag">{{r.status}}</span>
          {% else %}{{r.status}}{% endif %}
          {% if r.sla_breached %}<div class="mut" style=color:var(--p1)>SLA breached</div>{% endif %}</td>
      <td>{{ r.assignee or '' }}</td>
      <td class=mono>{{ r.total_lag_ms or '' }}{% if r.total_lag_ms %}ms{% endif %}</td>
    </tr>
    {% endfor %}
  </table>
  {% else %}<div class=empty>Nothing here. The pipeline is handling everything on its own.</div>{% endif %}
  </div>
</div>
"""

DETAIL_HTML = """<!doctype html><meta charset=utf-8><title>{{i.asset_id}} incident</title>
<style>""" + BASE_CSS + """</style>
<div class=wrap>
  <div class=sub><a href="/">&larr; incidents</a></div>
  <h1><span class="pill {{i.priority}}">{{i.priority or '--'}}</span>
      {{i.asset_id}} &middot; {{i.fault_code}}
      {% if i.needs_human %}<span class="pill flag">{{i.status}}</span>{% endif %}</h1>
  <div class=sub>{{i.alarm_text}}</div>

  <div class=card>
    <h2>Telemetry and triage</h2>
    <div class="grid kv">
      <div><b>Vibration</b>{{i.vibration_mms}} mm/s</div>
      <div><b>Temperature</b>{{i.temp_c}} &deg;C</div>
      <div><b>Pressure</b>{{i.pressure_bar}} bar</div>
      <div><b>Asset role</b>{{i.criticality}}</div>
      <div><b>Shift</b>{{i.shift}}</div>
      <div><b>Queue</b>{{i.queue}}</div>
      <div><b>SLA due</b>{{i.sla_due}}</div>
      <div><b>End to end</b>{{i.total_lag_ms}} ms
           <span class=mut>({{i.retrieval_lag_ms}} + {{i.planning_lag_ms}})</span></div>
    </div>
  </div>

  <div class=card>
    <h2>Retrieved from the service manual</h2>
    {% if i.doc_ref %}
      <div><span class=mono>{{i.doc_ref}}</span> &middot; {{i.matched_section}}
           <span class=mut>&middot; {{i.category}} &middot; severity {{i.severity}}
           &middot; score {{ '%.3f'|format(i.score or 0) }} ({{i.match_confidence}})</span></div>
    {% else %}<div class=mut>Nothing was retrieved.</div>{% endif %}
    {% if i.match_confidence == 'low' %}
      <div style="color:var(--warn);margin-top:8px">
        Confidence is low, which means nothing in the manual covers this alarm. Treat the
        suggestion below, if any, as unfounded.</div>
    {% endif %}
  </div>

  <div class=card>
    <h2>Planner output</h2>
    {% if wo %}
      <div class="grid kv">
        <div><b>Diagnosis</b>{{wo.diagnosis}}</div>
        <div><b>Parts</b>{{ wo.parts|join(', ') if wo.parts else 'none' }}</div>
        <div><b>Estimate</b>{{wo.estimated_minutes}} min</div>
        <div><b>Line stop likely</b>{{wo.line_stop_likely}}</div>
      </div>
      <div style=margin-top:10px><b class=mut>Action</b><div>{{wo.action}}</div></div>
      {% if wo.safety and wo.safety != 'none' %}
        <div style="margin-top:10px;color:var(--warn)"><b>Safety</b> {{wo.safety}}</div>{% endif %}
    {% else %}
      <div class=mut>No usable work order. {{ i.ai_work_order or '' }}</div>
    {% endif %}
    {% if i.manual_action %}
      <div style=margin-top:12px><b class=mut>Action written by a planner</b>
        <div>{{i.manual_action}}</div></div>
    {% endif %}
  </div>

  <div class=card>
    <h2>Work this incident</h2>
    {% if i.needs_human or not wo %}
    <form method=post action="/incident/{{i.incident_id}}/action">
      <input type=hidden name=what value=define>
      <label class=mut>The model could not ground an action. Write one:</label>
      <textarea name=manual_action placeholder="What should the engineer actually do?"></textarea>
      <div style=margin-top:8px><button>Define action</button></div>
    </form>
    {% endif %}
    <form method=post action="/incident/{{i.incident_id}}/action" class=inline>
      <input type=hidden name=what value=assign>
      <input name=assignee placeholder="Engineer name" value="{{i.assignee or ''}}">
      <button>Assign</button>
    </form>
    <form method=post action="/incident/{{i.incident_id}}/action" class=inline>
      <input name=note placeholder="Note (optional)">
      <select name=what style=width:150px>
        <option value=IN_PROGRESS>In progress</option>
        <option value=RESOLVED>Resolved</option>
        <option value=CLOSED>Closed</option>
        <option value=REJECTED>Rejected</option>
      </select>
      <button>Apply</button>
    </form>
  </div>

  <div class=card>
    <h2>Status timeline</h2>
    <ul class=tl>
      {% for t in timeline %}
      <li class="{{ 'hum' if t.actor != 'pipeline' }}">
        <b>{{t.status}}</b>
        {% if t.delta %}<span class=delta>{{t.delta}}</span>{% endif %}
        <span class=mut>{{t.at}}{% if t.actor != 'pipeline' %} &middot; {{t.actor}}{% endif %}</span>
        {% if t.note %}<div class=mut>{{t.note}}</div>{% endif %}
      </li>
      {% endfor %}
    </ul>
  </div>
</div>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
