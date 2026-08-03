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

# Cache the loaded model so we don't re-read every CSV on each question (the main
# source of the /ask timeout). Reloaded every 30 min.
_model_cache = {"model": None, "loaded_at": 0.0}
_MODEL_TTL = 1800


def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def _get_model(force: bool = False):
    now = time.time()
    if force or _model_cache["model"] is None or now - _model_cache["loaded_at"] > _MODEL_TTL:
        from model.mlb_model import MLBModel
        m = MLBModel()
        m.load()
        _model_cache["model"] = m
        _model_cache["loaded_at"] = now
    return _model_cache["model"]


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

        m = _get_model()

        def _slate(model):
            # Group the schedule by date and pick the ACTIVE slate: today if it has
            # games, otherwise the next upcoming date (in the evening, that's
            # tomorrow's card — same auto-switch the dashboard does).
            dates = {}
            for g in model.schedule:
                d = g.get("game_date")
                if d:
                    dates.setdefault(d, []).append(g)
            if not dates:
                return None, []
            future = sorted(d for d in dates if d >= today)
            sd = today if today in dates else (future[0] if future else max(dates))
            seen, games = set(), []
            for g in dates.get(sd, []):
                gid = g.get("game_id")
                if gid in seen:
                    continue
                seen.add(gid)
                games.append(g)
            return sd, games

        slate_date, sched = _slate(m)
        if not sched:
            # Stale/empty cached model — force ONE fresh load and retry before
            # ever telling the user there's nothing here.
            m = _get_model(force=True)
            slate_date, sched = _slate(m)

        scored = []
        for g in sched:
            try:
                scored.append(m.score_game(g))
            except Exception:
                pass
        has_games = bool(scored)
        picks = generate_picks(scored)
        by_game = defaultdict(list)
        for p in picks:
            by_game[p.get("game", "")].append(p)

        if slate_date and slate_date != today:
            out.append(f"(Today's games are finished — showing the next slate: {slate_date}.)")
        if not has_games:
            out.append("=== GAMES ===")
            out.append("(The board has no games loaded this second — it is still refreshing. "
                       "Tell the user the board is loading and to ask again in a few seconds. "
                       "Do NOT say they didn't provide games or ask them to paste a slate — this "
                       "is an MLB board and the games are always supplied here once loaded.)")
        else:
            out.append(f"=== GAMES on {slate_date} (full raw data for YOUR analysis + the model's read) ===")
            for g in scored:
                gl = f"{g.get('away_team','')} @ {g.get('home_team','')}"
                out.append(f"\n{gl}  ({g.get('venue','')})")
                out.append(f"  Starters: {g.get('away_sp','TBD')} (ERA {g.get('away_sp_era', g.get('away_sp_era_adj','?'))}) "
                           f"vs {g.get('home_sp','TBD')} (ERA {g.get('home_sp_era', g.get('home_sp_era_adj','?'))})")
                out.append(f"  Bullpen ERA: away {g.get('away_bp_era','?')} / home {g.get('home_bp_era','?')}")
                out.append(f"  Offense: away {g.get('away_rpg','?')} RPG (OPS {g.get('away_ops','?')}) / "
                           f"home {g.get('home_rpg','?')} RPG (OPS {g.get('home_ops','?')})")
                out.append(f"  Park runs factor {g.get('park_runs','?')} | "
                           f"weather {g.get('weather_flag','')} {g.get('temp_f','?')}F {g.get('wind_label','')}")
                out.append(f"  MODEL: win% away {g.get('away_wp')} / home {g.get('home_wp')} | "
                           f"exp total {g.get('exp_total')} vs line {g.get('total_line')} | "
                           f"sharp {g.get('sharp_side','')} {g.get('ml_signal','')}")
                out.append(f"  MARKET: ML away {g.get('ml_away_odds')} / home {g.get('ml_home_odds')}")
                ph = by_game.get(gl, [])
                if ph:
                    for p in ph:
                        v = p.get("value", {}) or {}
                        out.append(f"  MODEL PICK {p.get('type')}: {p.get('label')} | conf "
                                   f"{p.get('conf',0):.0%} | tier {p.get('tier')} | value {v.get('tag','')} "
                                   f"| EV {_fmt_ev(v)} | model {p.get('conf',0):.0%} vs market "
                                   f"{'' if v.get('market_prob') is None else round(v['market_prob']*100)}%")
                else:
                    out.append("  MODEL PICK: none — the model passed here (no qualifying edge or TBD "
                               "starters). Give YOUR OWN read from the data above.")
    except Exception as e:
        out.append(f"[board scoring failed: {e}]")

    # ── K props (real Pinnacle lines) ────────────────────────────────────────
    try:
        from model.mlb_props_model import score_all_props
        # Use the K-line file already pulled by the pipeline — do NOT do a live
        # Pinnacle fetch here (it was a big chunk of the /ask request latency).
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


_SYSTEM = """You are Statalizer Bot, an MLB betting analyst working alongside the
Statalizers model. For any game, team, matchup, or prop the user asks about, give
BOTH of these, clearly separated:

1. YOUR OWN read. Reason from the raw data provided - starting pitching and ERA,
   bullpen ERA, team offense (RPG/OPS), park factor, weather, market price. Form
   your own opinion on who has the edge and whether there is value at the price.
   This is your independent take, not a summary of the model.

2. THE MODEL'S read. What its win%, pick, tier, and value/EV say. Then explicitly
   state whether you AGREE or DISAGREE with the model, and why. It is genuinely
   useful when you see it differently - say so and make the case.

If the model has NO pick on a game, do NOT go silent - give your own read from the
data anyway; that is the most interesting case. Use ONLY the data provided; NEVER
invent numbers, lines, or games.

Calibration truths to apply to the model's numbers: it is well-calibrated only at
85%+ confidence and OVERCONFIDENT at 75-84% (predicts ~80%, hits ~64%); confidence
is NOT value (chalk can be a bad price); the run line is newly rebuilt and unproven.
Be direct and tight - a few short paragraphs, opinion clearly distinct from the model."""


# Gemini's role on the bot: its OWN independent read from the raw data, then a debate
# with Statalizer Bot's (Claude's) answer — a different model family, uncorrelated errors.
_GEMINI_BOT_SYSTEM = """You are a second, independent MLB analyst debating Statalizer Bot
(a Claude-based analyst) about today's board. You get the raw board data, the user's
question, and Statalizer Bot's answer. Do TWO things: (1) give YOUR OWN read from the raw
data — starters/ERA, bullpen, offense RPG/OPS, park, weather, market price, your own take
on edge and value; (2) DEBATE the bot — state clearly where you AGREE and where you
DISAGREE and why, and push back on any overconfident or unsupported claim. Use ONLY the
data provided; never invent numbers, lines, or games. Apply the calibration truths: the
model is well-calibrated only at 85%+ and overconfident at 75-84%; confidence is not
value; the run line is unproven. Be direct and tight."""


def _call_claude_bot(pack: str, question: str) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "[ANTHROPIC_API_KEY not set — showing the raw board data instead]\n\n" + pack
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
        return body["content"][0]["text"].strip()
    except Exception as e:
        log.warning(f"ask_model Claude call failed: {e}")
        return f"[model call failed: {e}]"


def answer_question(question: str, date: str = None) -> dict:
    question = (question or "").strip()
    if not question:
        return {"answer": "Ask me about a game, matchup, team, or prop on today's board.",
                "gemini": "", "used_pack": ""}
    pack = build_board_pack()
    # 1) Statalizer Bot (Claude) answers first.
    answer = _call_claude_bot(pack, question)
    # 2) Gemini gives its own read and debates the bot (different model family).
    gemini = ""
    try:
        from gemini_client import gemini_available, call_gemini
        if gemini_available():
            gemini = call_gemini(
                _GEMINI_BOT_SYSTEM,
                f"BOARD DATA:\n{pack}\n\nQUESTION: {question}\n\n"
                f"STATALIZER BOT'S ANSWER:\n{answer}\n\n"
                "Give your own read from the data, then debate the bot.",
                max_tokens=1400)
    except Exception as e:
        log.warning(f"ask_model Gemini call failed: {e}")
        gemini = ""
    return {"answer": answer, "gemini": gemini, "used_pack": pack}
