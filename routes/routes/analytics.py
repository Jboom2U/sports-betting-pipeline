"""
routes/analytics.py
Analytics dashboard + natural language query interface.

Routes:
  GET  /analytics          — dashboard HTML
  POST /analytics/query    — NL question → SQL → results JSON
  GET  /analytics/schema   — DB + CSV schema summary (for debugging)
"""

import os
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Blueprint, Response, request, jsonify

from db.connection import db_conn

log  = logging.getLogger(__name__)
ET   = ZoneInfo("America/New_York")

analytics_bp = Blueprint("analytics", __name__)


# ── DB schema context sent to Claude for SQL generation ───────────────────────
_SCHEMA_CONTEXT = """
You are a PostgreSQL query assistant for a baseball betting analytics system.
Generate ONLY a valid SQL SELECT query — no explanation, no markdown, just the SQL.

DATABASE TABLES:

picks (
    pick_date DATE,          -- date the pick was made
    game_id TEXT,
    game TEXT,               -- e.g. "Cincinnati Reds @ St. Louis Cardinals"
    pick_type TEXT,          -- ML | TOTAL | RL
    label TEXT,              -- e.g. "Cardinals ML", "OVER 8.5"
    team TEXT,               -- picked team
    conf REAL,               -- model confidence 0.0–1.0
    tier TEXT,               -- LOCK | STRONG | LEAN | TOSSUP
    actual_result TEXT,      -- WIN | LOSS | PUSH | PENDING
    market_signal TEXT,      -- CONFIRM | DIVERGE | NEUTRAL
    reasoning TEXT
)

scored_games (
    score_date DATE,
    game_id TEXT,
    away_team TEXT,
    home_team TEXT,
    exp_away REAL,           -- expected runs away
    exp_home REAL,           -- expected runs home
    home_wp REAL,            -- home win probability
    away_wp REAL,
    ml_signal TEXT,          -- model ML pick signal
    sharp_side TEXT,         -- which side sharp money backed
    away_sp TEXT,            -- away starting pitcher name
    home_sp TEXT,
    away_sp_era REAL,
    home_sp_era REAL,
    weather_flag TEXT
)

player_prop_history (
    game_date DATE,
    player_name TEXT,
    team TEXT,
    prop_type TEXT,          -- K | HR | HITS | TB | RBI | R | SB
    line NUMERIC,            -- prop line (e.g. 4.5 for Ks Over 4.5)
    actual_value NUMERIC,    -- what the player actually did
    result TEXT,             -- HIT | MISS
    model_conf NUMERIC       -- model confidence 0.0–1.0
)

player_game_logs (
    game_date DATE,
    player_name TEXT,
    team TEXT,
    opponent TEXT,
    venue TEXT,              -- ballpark name
    pitcher_hand TEXT,       -- L | R
    ab INTEGER,
    h INTEGER,               -- hits
    hr INTEGER,              -- home runs
    rbi INTEGER,
    bb INTEGER,              -- walks
    k INTEGER,               -- strikeouts
    tb INTEGER,              -- total bases
    sb INTEGER               -- stolen bases
)

pipeline_runs (
    run_date DATE,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    status TEXT              -- running | complete | failed
)

RULES:
- Always use pick_date, score_date, game_date for date filtering (type DATE).
- Use NOW() - INTERVAL 'N days' for rolling windows.
- For win rate: ROUND(100.0 * SUM(CASE WHEN actual_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) AS win_rate_pct
- For ROI: assume -110 odds → payout multiplier 0.9091. ROI = (wins*0.9091 - losses) / total_bets * 100
- Only return SELECT statements. Never INSERT, UPDATE, DELETE, DROP.
- When asked about a player's stats, query player_game_logs or player_prop_history.
- LIMIT to 50 rows unless asked for more.
- Use ILIKE for case-insensitive name matching (e.g. player_name ILIKE '%alvarez%').
"""


def _call_claude(question: str) -> str:
    """Call Claude API to generate SQL from a natural language question."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    import urllib.request
    payload = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 512,
        "system": _SCHEMA_CONTEXT,
        "messages": [{"role": "user", "content": question}]
    }
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=data,
        headers={
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = json.loads(resp.read())
    return body["content"][0]["text"].strip()


def _run_query(sql: str) -> tuple:
    """Execute SQL, return (columns, rows) or raise."""
    # Safety check — only allow SELECT
    clean = sql.strip().lstrip(";").upper()
    if not clean.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed")

    with db_conn() as conn:
        if conn is None:
            raise RuntimeError("No DB connection available")
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return cols, rows


# ── Suggested questions ────────────────────────────────────────────────────────
SUGGESTED_QUESTIONS = [
    "How did LOCK picks perform when Kalshi confirmed the model?",
    "Win rate and ROI by tier for the last 30 days",
    "Which teams have we picked most often and what is the win rate for each?",
    "Show me all games where sharp money disagreed with our pick — did sharp win?",
    "Brady Singer strikeout props this season — hit rate vs line",
    "Yordan Alvarez home runs at Minute Maid Park last 30 days",
    "Best performing prop types by hit rate",
    "Games we lost as a heavy favorite (conf > 70%) — what went wrong?",
    "Show me picks where Kalshi diverged from the model this month",
    "Which umpires have we had the best record with?",
    "Kyle Schwarber home runs vs left-handed pitchers this season",
    "Biggest line movements this week — did the sharp side win?",
    "How has our STRONG tier performed vs LOCK tier in the last 60 days?",
    "Players with the most HR props hit in the last 30 days",
    "Ben Rice stats at Yankee Stadium this season",
]


# ── Routes ────────────────────────────────────────────────────────────────────

@analytics_bp.route("/analytics")
def analytics_dashboard():
    """Main analytics dashboard."""
    questions_js = json.dumps(SUGGESTED_QUESTIONS)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>⚾ Statalizers — Analytics</title>
<style>
:root {{
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --border: #30363d; --text: #e6edf3; --muted: #8b949e;
  --accent: #58a6ff; --green: #3fb950; --red: #f85149;
  --gold: #d29922; --blue: #388bfd;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; line-height: 1.6; }}
a {{ color: var(--accent); text-decoration: none; }}
.nav {{ background: var(--bg2); border-bottom: 1px solid var(--border); padding: .75rem 1.5rem; display: flex; align-items: center; gap: 1.5rem; }}
.nav-logo {{ font-size: 15px; font-weight: 600; color: var(--text); }}
.nav a {{ color: var(--muted); font-size: 13px; }}
.nav a:hover, .nav a.active {{ color: var(--text); }}
.container {{ max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem; }}
h1 {{ font-size: 20px; font-weight: 600; margin-bottom: .25rem; }}
.sub {{ color: var(--muted); font-size: 13px; margin-bottom: 2rem; }}
.search-wrap {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; padding: 1.25rem; margin-bottom: 1.5rem; }}
.search-row {{ display: flex; gap: .5rem; }}
.search-input {{
  flex: 1; background: var(--bg3); border: 1px solid var(--border);
  border-radius: 6px; padding: .6rem .875rem; color: var(--text); font-size: 14px;
  outline: none;
}}
.search-input:focus {{ border-color: var(--accent); }}
.search-btn {{
  background: var(--accent); color: #000; border: none; border-radius: 6px;
  padding: .6rem 1.125rem; font-size: 13px; font-weight: 600; cursor: pointer;
}}
.search-btn:hover {{ opacity: .85; }}
.search-btn:disabled {{ opacity: .5; cursor: not-allowed; }}
.chips {{ display: flex; flex-wrap: wrap; gap: 6px; margin-top: 1rem; }}
.chip {{
  font-size: 12px; padding: 3px 10px; border-radius: 20px;
  border: 1px solid var(--border); color: var(--muted); cursor: pointer;
  background: transparent; transition: all .15s;
}}
.chip:hover {{ border-color: var(--accent); color: var(--accent); }}
.result-area {{ margin-top: 1.5rem; }}
.result-card {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; padding: 1.25rem; margin-bottom: 1rem; }}
.result-q {{ font-size: 12px; color: var(--muted); margin-bottom: .75rem; display: flex; align-items: center; gap: 6px; }}
.result-answer {{ font-size: 15px; font-weight: 600; margin-bottom: .5rem; }}
.result-answer .hl {{ color: var(--green); }}
.result-sub {{ font-size: 12px; color: var(--muted); margin-bottom: 1rem; }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
th {{ color: var(--muted); font-weight: 500; text-align: left; padding: .4rem 0; border-bottom: 1px solid var(--border); font-size: 11px; }}
td {{ padding: .4rem 0; border-bottom: 1px solid var(--border22); color: var(--muted); }}
td:first-child {{ color: var(--text); }}
tr:last-child td {{ border-bottom: none; }}
.sql-toggle {{ font-size: 11px; color: var(--muted); cursor: pointer; margin-top: .75rem; display: inline-flex; align-items: center; gap: 4px; }}
.sql-toggle:hover {{ color: var(--accent); }}
.sql-block {{ background: var(--bg3); border-radius: 6px; padding: .625rem .875rem; font-size: 11px; font-family: monospace; color: var(--muted); margin-top: .5rem; line-height: 1.6; white-space: pre-wrap; display: none; }}
.sql-block.visible {{ display: block; }}
.error-card {{ background: rgba(248,81,73,.08); border: 1px solid rgba(248,81,73,.3); border-radius: 8px; padding: 1rem 1.25rem; color: var(--red); font-size: 13px; }}
.loading {{ color: var(--muted); font-size: 13px; padding: 1rem 0; }}
.badge {{ display: inline-block; font-size: 11px; font-weight: 600; padding: 1px 7px; border-radius: 4px; }}
.badge-green {{ background: rgba(63,185,80,.15); color: var(--green); }}
.badge-red {{ background: rgba(248,81,73,.15); color: var(--red); }}
.badge-blue {{ background: rgba(56,139,253,.15); color: var(--blue); }}
.badge-gold {{ background: rgba(210,153,34,.15); color: var(--gold); }}
.no-results {{ color: var(--muted); font-size: 13px; font-style: italic; padding: .5rem 0; }}
</style>
</head>
<body>
<nav class="nav">
  <span class="nav-logo">⚾ Statalizers</span>
  <a href="/">Dashboard</a>
  <a href="/performance-html">Performance</a>
  <a href="/analytics" class="active">Analytics</a>
  <a href="/admin/model-config">Model Config</a>
  <a href="/status">Status</a>
</nav>
<div class="container">
  <h1>Analytics</h1>
  <p class="sub">Ask any question about your picks, props, or player stats.</p>

  <div class="search-wrap">
    <div class="search-row">
      <input id="q" class="search-input" type="text"
             placeholder="e.g. How did LOCK picks perform when Kalshi confirmed the model?"
             onkeydown="if(event.key==='Enter') runQuery()">
      <button id="btn" class="search-btn" onclick="runQuery()">Ask</button>
    </div>
    <div class="chips" id="chips"></div>
  </div>

  <div class="result-area" id="results"></div>
</div>

<script>
const QUESTIONS = {questions_js};
const chips = document.getElementById("chips");
QUESTIONS.forEach(q => {{
  const b = document.createElement("button");
  b.className = "chip";
  b.textContent = q;
  b.onclick = () => {{ document.getElementById("q").value = q; runQuery(); }};
  chips.appendChild(b);
}});

let lastSql = "";

async function runQuery() {{
  const q = document.getElementById("q").value.trim();
  if (!q) return;
  const btn = document.getElementById("btn");
  btn.disabled = true;
  btn.textContent = "Thinking…";
  const ra = document.getElementById("results");
  ra.innerHTML = '<p class="loading">Generating query…</p>';

  try {{
    const resp = await fetch("/analytics/query", {{
      method: "POST",
      headers: {{"Content-Type": "application/json"}},
      body: JSON.stringify({{question: q}})
    }});
    const data = await resp.json();
    if (data.error) {{
      ra.innerHTML = `<div class="error-card">${{data.error}}</div>`;
      return;
    }}
    lastSql = data.sql || "";
    renderResult(q, data);
  }} catch(e) {{
    ra.innerHTML = `<div class="error-card">Request failed: ${{e.message}}</div>`;
  }} finally {{
    btn.disabled = false;
    btn.textContent = "Ask";
  }}
}}

function renderResult(question, data) {{
  const ra = document.getElementById("results");
  const rows = data.rows || [];
  const cols = data.columns || [];
  const sqlId = "sql" + Date.now();

  let tableHtml = "";
  if (rows.length === 0) {{
    tableHtml = '<p class="no-results">No results found.</p>';
  }} else {{
    tableHtml = "<table><thead><tr>" + cols.map(c => `<th>${{c}}</th>`).join("") + "</tr></thead><tbody>";
    rows.forEach(row => {{
      tableHtml += "<tr>" + row.map((v, i) => {{
        let val = v === null ? "—" : v;
        if (typeof val === "number") val = Number.isInteger(val) ? val : parseFloat(val.toFixed(2));
        return `<td>${{val}}</td>`;
      }}).join("") + "</tr>";
    }});
    tableHtml += "</tbody></table>";
  }}

  ra.innerHTML = `
    <div class="result-card">
      <div class="result-q">💬 "${{question}}"</div>
      <div class="result-answer"><span class="hl">${{rows.length}} row${{rows.length !== 1 ? "s" : ""}}</span> returned</div>
      ${{tableHtml}}
      <span class="sql-toggle" onclick="toggleSql('${{sqlId}}')">⌨ View SQL</span>
      <div class="sql-block" id="${{sqlId}}">${{(data.sql || "").replace(/</g,"&lt;")}}</div>
    </div>`;
}}

function toggleSql(id) {{
  const el = document.getElementById(id);
  el.classList.toggle("visible");
}}
</script>
</body>
</html>"""
    return Response(html, mimetype="text/html")


@analytics_bp.route("/analytics/query", methods=["POST"])
def analytics_query():
    """
    POST {question: str} → {columns, rows, sql} or {error: str}
    Translates natural language to SQL via Claude API, executes against Postgres.
    """
    body = request.get_json(silent=True) or {}
    question = (body.get("question") or "").strip()
    if not question:
        return jsonify({"error": "No question provided"}), 400

    # Generate SQL
    try:
        sql = _call_claude(question)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        log.warning(f"Claude API error: {e}")
        return jsonify({"error": f"Failed to generate query: {e}"}), 500

    # Strip any accidental markdown fences
    if sql.startswith("```"):
        sql = sql.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    # Execute
    try:
        cols, rows = _run_query(sql)
    except ValueError as e:
        return jsonify({"error": str(e), "sql": sql}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e), "sql": sql}), 503
    except Exception as e:
        log.warning(f"Query execution error: {e}  SQL: {sql}")
        return jsonify({"error": f"Query failed: {e}", "sql": sql}), 500

    return jsonify({
        "question": question,
        "sql":      sql,
        "columns":  cols,
        "rows":     [list(r) for r in rows],
        "count":    len(rows),
    })


@analytics_bp.route("/analytics/schema")
def analytics_schema():
    """Return DB schema summary as JSON (for debugging)."""
    return jsonify({"schema": _SCHEMA_CONTEXT, "suggested": SUGGESTED_QUESTIONS})
