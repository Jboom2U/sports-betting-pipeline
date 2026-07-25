"""
ask_model.py — Ask the Statalizers model about any game, team, or prop.

Gathers today's full board (every game's model read: pitching, win%, market line,
value/EV, the picks, and the real-line K props) plus the model's calibration
truths, then asks Claude to answer the user's question grounded ONLY in that data.
For giving a real, honest read on demand — e.g. when a friend asks about a game.

Entry point: answer_question(question, date=None) -> {"answer", "used_pack"}
The board pack is cached ~10 min so repeated questions don't re-score.
"""
import os
import json
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

_pack_cache = {"date": None, "built_at": 0.0, "text": ""}
_PACK_TTL = 600  # seconds


def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def _fmt_ev(v: dict) -> str:
    ev = (v or {}).get("ev")
    return f"{ev*100:+.1f}%" if isinstance(ev, (int, float)) else "n/a"


def build_board_pack(force: bool = False) -> str:
    """Score today's board into a compact, LLM-readable text pack (cached)."""
    today = _today_et()
    now = time.time()
    if (not force and _pack_cache["date"] == today
            and now - _pack_cache["built_at"] < _PACK_TTL and _pack_cache["text"]):
        return _pack_cache["text"]

    out = [f"TODAY: {today}", ""]

    # ── Today's games + picks ────────────────────────────────────────────────
    try:
        from collections import defaultdict
        from model.mlb_model import MLBModel
        from model.mlb_picks import generate_picks

        m = MLBModel()
        m.load()
        scored, _actual = m.score_today(target_date=today)
        picks = generate_picks(scored)
        by_game = defaultdict(list)
        for p in picks:
            by_game[p.get("game", "")].append(p)

        out.append("=== GAMES (model reads) ===")
        for g in scored:
            gl = f"{g.get('away_team','')} @ {g.get('home_team','')}"
            out.append(f"\n{gl}")
            out.append(f"  SP: {g.get('away_sp','TBD')} (ERA {g.get('away_sp_era_adj','?')}) "
                       f"vs {g.get('home_sp','TBD')} (ERA {g.get('home_sp_era_adj','?')})")
            out.append(f"  Model win%: away {g.get('away_wp')} / home {g.get('home_wp')} | "
                       f"exp total {g.get('exp_total')} vs line {g.get('total_line')}")
            out.append(f"  Market ML: away {g.get('ml_away_odds')} / home {g.get('ml_home_odds')} | "
                       f"park {g.get('park_runs')} | weather {g.get('weather_flag')} | "
                       f"sharp {g.get('sharp_side','')} {g.get('ml_signal','')}")
            for p in by_game.get(gl, []):
                v = p.get("value", {}) or {}
                out.append(f"  PICK {p.get('type')}: {p.get('label')} | conf "
                           f"{p.get('conf',0):.0%} | tier {p.get('tier')} | "
                           f"value {v.get('tag','')} | EV {_fmt_ev(v)} | "
                           f"model {p.get('conf',0):.0%} vs market "
                           f"{'' if v.get('market_prob') is None else round(v['market_prob']*100)}%")
    except Exception as e:
        out.append(f"[board scoring failed: {e}]")

    # ── K props (real Pinnacle lines) ────────────────────────────────────────
    try:
        from model.mlb_props_model import score_all_props
        from scrapers.mlb_pinnacle_scraper import save_strikeout_lines
        try:
            save_strikeout_lines(today)
        except Exception:
            pass
        props = [p for p in score_all_props(today) if p.get("prop_type") == "K"]
        if props:
            out.append("\n=== PITCHER K PROPS (real Pinnacle lines) ===")
            for p in sorted(props, key=lambda x: x.get("confidence", 0), reverse=True)[:20]:
                out.append(f"  {p.get('player_name')}: {p.get('label','')} | "
                           f"proj {p.get('proj')} | conf {p.get('confidence',0):.0%} | "
                           f"EV {p.get('ev','?')}u")
    except Exception as e:
        out.append(f"[props unavailable: {e}]")

    # ── Recent performance / trends (honest calibration context) ─────────────
    try:
        from analysis_report import build_data_pack, _fmt_pack
        pack = build_data_pack((datetime.now(ET)).strftime("%Y-%m-%d"))
        if not pack.get("error"):
            out.append("\n=== RECENT PERFORMANCE (21-day trends) ===")
            out.append(_fmt_pack(pack).split("=== TRENDS", 1)[-1][:1800])
    except Exception:
        pass

    text = "\n".join(out)
    _pack_cache.update(date=today, built_at=now, text=text)
    return text


_SYSTEM = """You are Statalizer Bot, the analyst for the Statalizers MLB betting
model. Answer the user's question about a specific game, team, matchup, or prop
using ONLY the board data provided below. You are talking to the model's owner, who may be relaying your
read to a friend, so be clear and confident where the data supports it and honest
where it doesn't.

Calibration truths you MUST apply:
- The model is well-calibrated only at 85%+ confidence. It is OVERCONFIDENT in the
  75-84% range (predicts ~80%, actually hits ~64%). Discount high-confidence chalk.
- Confidence is NOT value. A heavy favorite (e.g. -240) can be a bad bet when the
  price is worse than the model's real edge. Prefer positive-EV picks at fair prices
  and call out chalk with no value.
- The run line is newly rebuilt and UNPROVEN — describe its read but do not give it
  a confident betting verdict yet.
- If the provided data does not cover the question, say so plainly. NEVER invent
  numbers, lines, or games that aren't in the data.

Give a direct read: what the model sees, whether there's real value at the price,
and how much to trust it. Keep it tight — a few sentences, not an essay."""


def answer_question(question: str, date: str = None) -> dict:
    question = (question or "").strip()
    if not question:
        return {"answer": "Ask me about a game, matchup, team, or prop on today's board.",
                "used_pack": ""}
    pack = build_board_pack()
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"answer": "[ANTHROPIC_API_KEY not set — showing the raw board data instead]\n\n" + pack,
                "used_pack": pack}
    import urllib.request
    payload = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 900,
        "system": _SYSTEM,
        "messages": [{"role": "user",
                      "content": f"BOARD DATA:\n{pack}\n\nQUESTION: {question}"}],
    }
    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(payload).encode(),
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=40) as resp:
            body = json.loads(resp.read())
        return {"answer": body["content"][0]["text"].strip(), "used_pack": pack}
    except Exception as e:
        log.warning(f"ask_model Claude call failed: {e}")
        return {"answer": f"[model call failed: {e}]", "used_pack": pack}
