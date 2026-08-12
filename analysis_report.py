"""
analysis_report.py
Nightly / on-demand performance analysis. Pulls graded data from Postgres,
computes the day's results plus multi-week TRENDS the eye would miss, then asks
Claude to write a narrative: how the day went, what underperformed, concrete
model-tweak ideas, and standout patterns.

Entry point: build_report(date=None) -> {"date","data","narrative","generated_at"}
All queries are read-only. Column names verified against db/schema.py.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

BREAK_EVEN = 52.38   # -110 juice


def _yesterday_et() -> str:
    return (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")


def _rows(cur, sql, params=()):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def build_data_pack(date: str) -> dict:
    """Gather the day's results + multi-week trends into a structured dict."""
    from db.connection import db_conn
    pack = {"date": date, "error": None}

    with db_conn() as conn:
        if conn is None:
            pack["error"] = "No DB connection"
            return pack
        cur = conn.cursor()

        # ── The day: overall, by tier, by type ──────────────────────────────
        pack["day_by_tier"] = _rows(cur, """
            SELECT tier,
                   COUNT(*) FILTER (WHERE actual_result='WIN')  AS w,
                   COUNT(*) FILTER (WHERE actual_result='LOSS') AS l,
                   COUNT(*) FILTER (WHERE actual_result='PUSH') AS p,
                   ROUND(AVG(conf)::numeric,3) AS avg_conf
            FROM picks
            WHERE pick_date=%s AND actual_result IN ('WIN','LOSS','PUSH')
            GROUP BY tier ORDER BY tier
        """, (date,))
        pack["day_by_type"] = _rows(cur, """
            SELECT pick_type,
                   COUNT(*) FILTER (WHERE actual_result='WIN')  AS w,
                   COUNT(*) FILTER (WHERE actual_result='LOSS') AS l,
                   COUNT(*) FILTER (WHERE actual_result='PUSH') AS p
            FROM picks
            WHERE pick_date=%s AND actual_result IN ('WIN','LOSS','PUSH')
            GROUP BY pick_type ORDER BY pick_type
        """, (date,))
        # the day's actual pick list (for context)
        pack["day_picks"] = _rows(cur, """
            SELECT pick_type, tier, label, team, conf, actual_result
            FROM picks
            WHERE pick_date=%s AND actual_result IN ('WIN','LOSS','PUSH')
            ORDER BY conf DESC
        """, (date,))

        # ── The day: props (side-aware) ─────────────────────────────────────
        pack["day_props"] = _rows(cur, """
            SELECT prop_type,
                   COUNT(*) FILTER (WHERE result = COALESCE(pick_side,'OVER')) AS hits,
                   COUNT(*) FILTER (WHERE result IN ('OVER','UNDER','PUSH'))    AS total
            FROM player_prop_history
            WHERE game_date=%s AND result IS NOT NULL
            GROUP BY prop_type ORDER BY prop_type
        """, (date,))

        # ── Trends: last 21 days, but NEVER before the post-fix boundary ─────
        # Before 2026-07-25 the model was a different animal: dead umpire/bullpen/
        # platoon/Kalshi signals, the old always-favorite run line, HR/SB not yet
        # suppressed, and a mixed 0-1 vs 0-100 confidence scale. Pooling that with
        # the current model produces conclusions that are wrong about both, so we
        # floor every trend query at the boundary. (Widen POSTFIX_CUTOFF's lookback
        # naturally as more clean days accumulate.)
        POSTFIX_CUTOFF = "2026-07-25"
        _raw_cutoff = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=21)).strftime("%Y-%m-%d")
        cutoff = max(_raw_cutoff, POSTFIX_CUTOFF)
        pack["trend_cutoff"] = cutoff

        pack["trend_by_conf_band"] = _rows(cur, """
            SELECT width_bucket(conf, 0.50, 0.85, 7) AS band,
                   MIN(conf) AS lo, MAX(conf) AS hi,
                   COUNT(*) FILTER (WHERE actual_result='WIN')  AS w,
                   COUNT(*) FILTER (WHERE actual_result='LOSS') AS l
            FROM picks
            WHERE pick_date >= %s AND actual_result IN ('WIN','LOSS')
            GROUP BY band ORDER BY band
        """, (cutoff,))

        pack["trend_by_tier"] = _rows(cur, """
            SELECT tier, pick_type,
                   COUNT(*) FILTER (WHERE actual_result='WIN')  AS w,
                   COUNT(*) FILTER (WHERE actual_result='LOSS') AS l
            FROM picks
            WHERE pick_date >= %s AND actual_result IN ('WIN','LOSS')
            GROUP BY tier, pick_type ORDER BY tier, pick_type
        """, (cutoff,))

        pack["trend_market_signal"] = _rows(cur, """
            SELECT COALESCE(market_signal,'NONE') AS market_signal,
                   COUNT(*) FILTER (WHERE actual_result='WIN')  AS w,
                   COUNT(*) FILTER (WHERE actual_result='LOSS') AS l
            FROM picks
            WHERE pick_date >= %s AND actual_result IN ('WIN','LOSS')
            GROUP BY COALESCE(market_signal,'NONE') ORDER BY market_signal
        """, (cutoff,))

        # props trend by type + side
        pack["trend_props"] = _rows(cur, """
            SELECT prop_type, COALESCE(pick_side,'OVER') AS side,
                   COUNT(*) FILTER (WHERE result = COALESCE(pick_side,'OVER')) AS hits,
                   COUNT(*) FILTER (WHERE result IN ('OVER','UNDER','PUSH'))    AS total
            FROM player_prop_history
            WHERE game_date >= %s AND result IS NOT NULL
            GROUP BY prop_type, COALESCE(pick_side,'OVER')
            HAVING COUNT(*) FILTER (WHERE result IN ('OVER','UNDER','PUSH')) >= 5
            ORDER BY prop_type, side
        """, (cutoff,))

    return pack


def _fmt_pack(pack: dict) -> str:
    """Compact human/LLM-readable rendering of the data pack."""
    def wl(w, l):
        n = (w or 0) + (l or 0)
        return f"{w}-{l} ({(w/n*100):.1f}%)" if n else "0-0 (—)"
    out = [f"DATE: {pack['date']}", ""]

    out.append("=== THE DAY ===")
    out.append("By tier:")
    for r in pack.get("day_by_tier", []):
        out.append(f"  {r['tier']}: {wl(r['w'], r['l'])}  push {r['p']}  avg_conf {r['avg_conf']}")
    out.append("By type:")
    for r in pack.get("day_by_type", []):
        out.append(f"  {r['pick_type']}: {wl(r['w'], r['l'])}  push {r['p']}")
    out.append("Props (side-aware):")
    for r in pack.get("day_props", []):
        out.append(f"  {r['prop_type']}: {r['hits']}/{r['total']}")

    out.append("")
    out.append(f"=== TRENDS (POST-FIX ONLY, since {pack.get('trend_cutoff','2026-07-25')}) ===")
    out.append("ML/RL/TOTAL by confidence band:")
    for r in pack.get("trend_by_conf_band", []):
        out.append(f"  {float(r['lo'])*100:.0f}-{float(r['hi'])*100:.0f}%: {wl(r['w'], r['l'])}")
    out.append("By tier x type:")
    for r in pack.get("trend_by_tier", []):
        out.append(f"  {r['tier']} {r['pick_type']}: {wl(r['w'], r['l'])}")
    out.append("By market signal (Kalshi/Poly):")
    for r in pack.get("trend_market_signal", []):
        out.append(f"  {r['market_signal']}: {wl(r['w'], r['l'])}")
    out.append("Props by type+side:")
    for r in pack.get("trend_props", []):
        out.append(f"  {r['prop_type']} {r['side']}: {r['hits']}/{r['total']}")
    return "\n".join(out)


_SYSTEM = """You are the analyst for a data-driven MLB betting model (Statalizers).
You are writing a candid evening performance review for the model's owner, Justin.
Break-even against -110 juice is 52.38%. Be direct and quantitative. Do NOT invent
numbers — use only the data provided. Structure your answer with these sections:

1. HOW THE DAY WENT — a short, honest paragraph on the day's record.
2. WHAT UNDERPERFORMED — call out the specific tiers/types/props that lost, with numbers.
3. TRENDS WORTH KNOWING — the highest-signal patterns in the POST-FIX data (only
   picks since the model was repaired; older picks are deliberately excluded).
   Only cite a pattern if it has a usable sample (n>=20 game picks, n>=10 props)
   and is clearly above or below break-even. This window is SMALL and recent, so
   samples will be thin — say so explicitly and do NOT over-conclude from n<20.
4. IDEAS TO IMPROVE — concrete next steps tied to a number in the data. Do NOT
   recommend anything already done: the run line was rebuilt, HR/SB/HITS/RBI/R/TB
   props are suppressed unless they have a real book line, per-type calibration is
   live, real Pinnacle prop lines are wired in, and prices are now stored per pick.

ESTABLISHED FINDINGS (2026-08-11, 661 graded picks since the 2026-07-21 boundary).
Treat these as settled and do not re-derive or contradict them without new data:
  - RUN LINE is the only walk-forward validated edge: 51-21, 70.8%, p=0.0011, in
    the 57-65% confidence band. It collapses above 65% (15-14 at the cover cap).
  - MONEYLINE did not hold up out of sample: 48-42, 53.3%, p=0.47. It also loses
    at the favourite prices it actually gets (-130 to -160 needs 56.5-61.5%).
  - TOTALS cannot clear breakeven structurally: conf is capped at 0.68, which
    calibrates to 48.8% against a 52.38% breakeven. Do not propose raising the cap;
    that relabels the same non-information. The run projection needs rebuilding.
  - Per-type Platt: ML A=0.621123 B=-0.368625, TOTAL A=0.178915 B=-0.183981. RL is
    deliberately uncalibrated because fitting it made out-of-sample Brier worse.

Keep it tight. No hedging filler. If a section has no signal, say so."""


def build_report(date: str = None) -> dict:
    """Build the data pack and ask Claude for the narrative analysis."""
    date = date or _yesterday_et()
    pack = build_data_pack(date)
    generated = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")

    if pack.get("error"):
        return {"date": date, "data": pack, "narrative":
                f"Could not build report: {pack['error']}", "generated_at": generated}

    data_text = _fmt_pack(pack)
    narrative = _call_claude_report(data_text)
    return {"date": date, "data": pack, "data_text": data_text,
            "narrative": narrative, "generated_at": generated}


def render_html(rep: dict) -> str:
    """Render a report dict into a standalone HTML email/page body."""
    import html as _h
    import re as _re
    date = rep.get("date", "")
    gen  = rep.get("generated_at", "")
    body = _h.escape(rep.get("narrative", ""))
    # numbered section headers -> styled h3
    body = _re.sub(r'(?m)^(\d+\.\s+[A-Z][^\n]*)$',
                   r'</p><h3 style="color:#58a6ff;margin:20px 0 6px">\1</h3><p>', body)
    body = body.replace("\n\n", "</p><p>").replace("\n", "<br>")
    data = _h.escape(rep.get("data_text", ""))
    return f"""<div style="background:#0d1117;color:#c9d1d9;font-family:system-ui,Segoe UI,Arial;
padding:24px;max-width:820px;margin:0 auto;line-height:1.55">
<h2 style="color:#58a6ff;margin:0 0 2px">Statalizers Analysis — {date}</h2>
<div style="color:#6e7681;font-size:.82rem;margin-bottom:16px">Generated {gen}</div>
<p>{body}</p>
<details style="margin-top:24px"><summary style="cursor:pointer;color:#8b949e">Raw data</summary>
<pre style="background:#161b22;padding:14px;border-radius:8px;overflow:auto;font-size:12px;color:#8b949e">{data}</pre></details>
<p style="margin-top:20px;color:#6e7681;font-size:.8rem">— Statalizers · statalizers.com/admin/analysis</p>
</div>"""


def email_report(date: str = None) -> bool:
    """Build the report for `date` (default yesterday) and email it.
    Returns True if the email was sent."""
    rep = build_report(date)
    html = render_html(rep)
    text = f"Statalizers Analysis — {rep.get('date')}\n\n{rep.get('narrative','')}"
    try:
        from alerts import send_html_email
        return send_html_email(f"Analysis — {rep.get('date')}", html, text)
    except Exception as e:
        log.warning(f"email_report failed: {e}")
        return False


def _call_claude_report(data_text: str) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ("[ANTHROPIC_API_KEY not set — showing raw numbers only]\n\n" + data_text)
    import urllib.request
    payload = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1400,
        "system": _SYSTEM,
        "messages": [{"role": "user",
                      "content": "Here is the data. Write the review.\n\n" + data_text}],
    }
    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(payload).encode(),
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
        return body["content"][0]["text"].strip()
    except Exception as e:
        log.warning(f"analysis report Claude call failed: {e}")
        return f"[Claude call failed: {e}]\n\n{data_text}"


# ── Claude-vs-Gemini debate over the same data ───────────────────────────────
_GEMINI_SYSTEM = """You are a skeptical quant reviewing another analyst's take on a
data-driven MLB betting model (Statalizers). Break-even vs -110 juice is 52.38%. You
get the raw performance data AND Claude's written analysis of it. RED-TEAM Claude — do
not just agree. Use ONLY the numbers provided; never invent data.

Hunt specifically for the mistakes a Claude-style model tends to make: over-smoothing a
thin calibration curve into a tidy story, treating a small-sample streak (n<20) as real
signal, ignoring vigorish when comparing model prob to market price, confusing
confidence with value, and drawing verdicts from the post-fix window that is too new to
support them.

Structure: (1) WHERE CLAUDE IS RIGHT — briefly. (2) WHERE CLAUDE IS OVERREACHING — the
specific claims the sample does not support, with the numbers. (3) WHAT CLAUDE MISSED —
a pattern or risk in the data it skipped. Be direct and quantitative. If the data
genuinely backs Claude, say so rather than manufacturing disagreement."""

_DEBATE_SYSTEM = """You are the Statalizers analyst who wrote the first analysis. A
skeptical second reviewer has challenged it. You get the raw data, your original
analysis, and their rebuttal. Respond honestly: CONCEDE the points where they are right,
DEFEND the ones where the data backs you and explain why, and end with a BOTTOM LINE on
what to actually believe from this window. Use only the data provided. Tight, no filler."""


def _call_claude(system: str, user: str, max_tokens: int = 1200) -> str:
    """Generic Claude call (the debate response uses this; the report uses its own)."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "[ANTHROPIC_API_KEY not set]"
    import urllib.request
    payload = {"model": "claude-haiku-4-5-20251001", "max_tokens": max_tokens,
               "system": system, "messages": [{"role": "user", "content": user}]}
    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(payload).encode(),
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
        return body["content"][0]["text"].strip()
    except Exception as e:
        log.warning(f"claude debate call failed: {e}")
        return f"[Claude call failed: {e}]"


def build_debate(data_text: str, claude_narrative: str) -> dict:
    """One-round debate on the SAME data: Gemini challenges Claude's analysis, then
    Claude responds. Returns {'enabled', 'gemini_challenge', 'claude_response'}.
    Degrades gracefully when GEMINI_API_KEY is missing."""
    from gemini_client import gemini_available, call_gemini
    if not gemini_available():
        return {"enabled": False,
                "gemini_challenge": ("[GEMINI_API_KEY not set — add it in Railway env "
                                     "vars (free key from Google AI Studio) to turn on "
                                     "the Claude-vs-Gemini debate.]"),
                "claude_response": ""}
    challenge = call_gemini(
        _GEMINI_SYSTEM,
        f"RAW DATA:\n{data_text}\n\nCLAUDE'S ANALYSIS:\n{claude_narrative}\n\n"
        "Red-team Claude's analysis now.", max_tokens=1200)
    response = _call_claude(
        _DEBATE_SYSTEM,
        f"RAW DATA:\n{data_text}\n\nYOUR ORIGINAL ANALYSIS:\n{claude_narrative}\n\n"
        f"THE REVIEWER'S REBUTTAL:\n{challenge}\n\nRespond.")
    return {"enabled": True, "gemini_challenge": challenge, "claude_response": response}
