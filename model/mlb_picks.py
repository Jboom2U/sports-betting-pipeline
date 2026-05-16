"""
mlb_picks.py
Converts scored games into ranked individual picks and parlay recommendations.

Confidence tiers:
  LOCK   75%+  (strongest model signal)
  STRONG 68-75%
  LEAN   60-68%
  TOSSUP 48-60% (shown for coverage, no Kelly recommendation)
  PASS   <48%  (not included in output)

Parlay rules:
  - Minimum 57% per leg
  - No two picks from the same game
  - Ranked by combined probability
"""

import csv
import os
import logging
from itertools import combinations
from datetime import datetime

log = logging.getLogger(__name__)

LOCK_THRESH   = 0.75   # raised from 0.68 -- only genuine high-confidence picks
STRONG_THRESH = 0.68   # raised from 0.62
LEAN_THRESH   = 0.60   # raised from 0.52 -- sub-60% not worth showing
TOSSUP_THRESH = 0.48
PARLAY_MIN    = 0.57   # minimum per-leg confidence for parlay inclusion

# Run line requires beating a -1.5 spread — needs meaningfully more edge than ML.
# Only publish RL picks at LEAN or better (60%+) to filter thin-edge noise.
RL_MIN_THRESH = LEAN_THRESH  # 0.60

# TBD starter penalty: when a probable pitcher is literally "TBD" (no assignment),
# the model fills with league-average ERA which inflates fake confidence.
# Rules (only on literal "TBD" string — not on missing stats, which is too broad
# and caused the previous hotfix revert):
#   - TOTAL picks: suppress entirely when EITHER SP is TBD (run total unreliable)
#   - ML picks: downgrade one tier when BOTH SPs are TBD
#   - RL picks: suppress entirely when EITHER SP is TBD (spread needs real edge)
TBD_SUPPRESS_TOTAL_EITHER = True
TBD_SUPPRESS_RL_EITHER    = True
TBD_DOWNGRADE_ML_BOTH     = True

# Approximate payout at -110 per leg (American odds)
PARLAY_PAYOUTS = {2: "+260", 3: "+595", 4: "+1228", 5: "+2435"}


# ─────────────────────────────────────────────────────────────────────────────
# TIER HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def tier(conf: float) -> str:
    if conf >= LOCK_THRESH:   return "LOCK"
    if conf >= STRONG_THRESH: return "STRONG"
    if conf >= LEAN_THRESH:   return "LEAN"
    if conf >= TOSSUP_THRESH: return "TOSSUP"
    return "PASS"


def stars(conf: float) -> str:
    if conf >= LOCK_THRESH:   return "★★★"
    if conf >= STRONG_THRESH: return "★★ "
    if conf >= LEAN_THRESH:   return "★  "
    if conf >= TOSSUP_THRESH: return "≈  "
    return "   "


def tier_emoji(t: str) -> str:
    return {"LOCK": "🔒", "STRONG": "⭐⭐", "LEAN": "⭐", "TOSSUP": "≈", "PASS": "—"}.get(t, "")


# ─────────────────────────────────────────────────────────────────────────────
# PICK GENERATION
# ─────────────────────────────────────────────────────────────────────────────
def generate_picks(scored_games: list) -> list:
    """
    Convert scored games into a flat sorted list of individual picks.
    Includes picks down to TOSSUP threshold (48%). TOSSUP picks are shown
    for coverage only — no Kelly recommendation, no parlay inclusion.
    """
    picks = []

    for g in scored_games:
        game_label = f"{g['away_team']} @ {g['home_team']}"

        # TBD starter flags — only suppress on literal "TBD", not on missing stats
        away_sp_tbd = (g.get("away_sp", "") or "").strip().upper() == "TBD"
        home_sp_tbd = (g.get("home_sp", "") or "").strip().upper() == "TBD"
        either_tbd  = away_sp_tbd or home_sp_tbd
        both_tbd    = away_sp_tbd and home_sp_tbd

        # ── Moneyline ──────────────────────────────────────────────────────
        ml_conf = g["ml_conf"]
        # Downgrade one tier when BOTH starters are TBD (model has no pitcher data at all)
        if both_tbd and TBD_DOWNGRADE_ML_BOTH:
            if   ml_conf >= LOCK_THRESH:   ml_conf = STRONG_THRESH
            elif ml_conf >= STRONG_THRESH: ml_conf = LEAN_THRESH
            elif ml_conf >= LEAN_THRESH:   ml_conf = TOSSUP_THRESH
            log.debug(f"ML pick downgraded (both SP TBD): {game_label}")
        if ml_conf >= TOSSUP_THRESH:
            ml_team  = g["ml_team"]
            opp_team = g["home_team"] if ml_team == g["away_team"] else g["away_team"]
            picks.append({
                "type":       "ML",
                "label":      f"{ml_team} ML",
                "team":       ml_team,
                "opp_team":   opp_team,
                "side":       g["ml_side"],
                "conf":       ml_conf,
                "tier":       tier(ml_conf),
                "stars":      stars(ml_conf),
                "game":       game_label,
                "game_id":    g["game_id"],
                "venue":      g["venue"],
                "exp_total":  g["exp_total"],
                "reasoning":  _ml_reasoning(g),
                "narrative":  _build_narrative(g, "ML", ml_team),
                "game_data":  g,
            })

        # ── Totals ─────────────────────────────────────────────────────────
        # Suppress entirely when either SP is TBD — run totals are unreliable
        # without real pitcher data (model falls back to league-average ERA).
        tot_conf = g["total_conf"]
        if either_tbd and TBD_SUPPRESS_TOTAL_EITHER:
            log.debug(f"TOTAL pick suppressed (SP TBD): {game_label}")
        elif tot_conf >= TOSSUP_THRESH:
            total_pick = g["total_pick"]
            opp_side   = "UNDER" if total_pick == "OVER" else "OVER"
            picks.append({
                "type":       "TOTAL",
                "label":      f"{total_pick} {g['total_line']}",
                "team":       total_pick,
                "opp_team":   opp_side,
                "side":       total_pick.lower(),
                "conf":       tot_conf,
                "tier":       tier(tot_conf),
                "stars":      stars(tot_conf),
                "game":       game_label,
                "game_id":    g["game_id"],
                "venue":      g["venue"],
                "exp_total":  g["exp_total"],
                "reasoning":  _total_reasoning(g),
                "narrative":  _build_narrative(g, "TOTAL", total_pick),
                "game_data":  g,
            })

        # ── Run Line ───────────────────────────────────────────────────────
        # Two gates: either SP TBD → suppress; confidence below RL_MIN_THRESH → suppress.
        # RL requires beating -1.5 so needs real pitcher data and a genuine edge.
        rl_conf = g["rl_conf"]
        if either_tbd and TBD_SUPPRESS_RL_EITHER:
            log.debug(f"RL pick suppressed (SP TBD): {game_label}")
        elif rl_conf >= RL_MIN_THRESH and g["rl_team"]:
            rl_team  = g["rl_team"]
            opp_team = g["home_team"] if rl_team == g["away_team"] else g["away_team"]
            picks.append({
                "type":       "RL",
                "label":      g["rl_pick"],
                "team":       rl_team,
                "opp_team":   opp_team,
                "side":       "rl",
                "conf":       rl_conf,
                "tier":       tier(rl_conf),
                "stars":      stars(rl_conf),
                "game":       game_label,
                "game_id":    g["game_id"],
                "venue":      g["venue"],
                "exp_total":  g["exp_total"],
                "reasoning":  _rl_reasoning(g),
                "narrative":  _build_narrative(g, "RL", rl_team),
                "game_data":  g,
            })

    picks.sort(key=lambda x: x["conf"], reverse=True)
    return picks


# ─────────────────────────────────────────────────────────────────────────────
# PARLAY BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def build_parlays(picks: list, legs: int = 2, max_parlays: int = 3) -> list:
    """
    Build the best N-leg parlays from qualified picks.
    No two legs from the same game. Ranked by combined probability.
    """
    qualified = [p for p in picks if p["conf"] >= PARLAY_MIN]
    parlays   = []

    for combo in combinations(qualified, legs):
        game_ids = [p["game_id"] for p in combo]
        if len(set(game_ids)) < legs:
            continue   # two picks from same game

        combined = 1.0
        for p in combo:
            combined *= p["conf"]

        parlays.append({
            "legs":        list(combo),
            "n_legs":      legs,
            "combined":    round(combined, 4),
            "payout":      PARLAY_PAYOUTS.get(legs, f"+{legs*200}"),
            "summary":     " + ".join(p["label"] for p in combo),
            "min_leg":     min(p["conf"] for p in combo),
        })

    parlays.sort(key=lambda x: x["combined"], reverse=True)
    return parlays[:max_parlays]


# ─────────────────────────────────────────────────────────────────────────────
# REASONING STRINGS
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_era(val) -> str:
    return f"{float(val):.2f}" if val is not None else "N/A"


def _ml_reasoning(g: dict) -> str:
    parts = []

    # Pitcher matchup + recent form trend
    away_era = g.get("away_sp_era_adj")
    home_era = g.get("home_sp_era_adj")
    away_sp  = g.get("away_sp", "TBD")
    home_sp  = g.get("home_sp", "TBD")
    away_trend = g.get("away_sp_trend", "")
    home_trend = g.get("home_sp_trend", "")

    away_label = f"{away_sp} ERA {_fmt_era(away_era)}"
    if away_trend and away_trend != "N/A":
        away_label += f" [{away_trend}]"
    home_label = f"{home_sp} ERA {_fmt_era(home_era)}"
    if home_trend and home_trend != "N/A":
        home_label += f" [{home_trend}]"

    if not g.get("away_sp_missing") and not g.get("home_sp_missing"):
        parts.append(f"{away_label} vs {home_label}")
    elif not g.get("away_sp_missing"):
        parts.append(f"{away_label} (home SP TBD)")
    elif not g.get("home_sp_missing"):
        parts.append(f"Away SP TBD vs {home_label}")

    # Recent team form
    ar = g.get("away_form_rpg", 0)
    hr = g.get("home_form_rpg", 0)
    aw = g.get("away_form_wpct", 0)
    hw = g.get("home_form_wpct", 0)
    parts.append(
        f"Recent: {g['away_team']} {ar:.1f} RPG ({aw*100:.0f}% W) | "
        f"{g['home_team']} {hr:.1f} RPG ({hw*100:.0f}% W)"
    )

    # Line movement signal
    ml_sig   = g.get("ml_signal", "")
    sharp    = g.get("sharp_side", "")
    ml_adj   = g.get("ml_adj", 0)
    if ml_sig in ("STEAM", "DRIFT") and sharp:
        direction = "WITH model" if ml_adj > 0 else "AGAINST model"
        parts.append(f"💰 {ml_sig}: Sharp $ on {sharp} ({direction})")

    # Current odds
    ml_away_odds = g.get("ml_away_odds")
    ml_home_odds = g.get("ml_home_odds")
    if ml_away_odds and ml_home_odds:
        def fmt_ml(v):
            v = int(v)
            return f"+{v}" if v > 0 else str(v)
        parts.append(f"Line: {g['away_team']} {fmt_ml(ml_away_odds)} | {g['home_team']} {fmt_ml(ml_home_odds)}")

    # Park note
    pf = g.get("park_runs", 100)
    if pf >= 108:
        parts.append(f"Hitter park ({pf})")
    elif pf <= 96:
        parts.append(f"Pitcher park ({pf})")

    # Prediction market signal
    poly_sig = g.get("poly_market_signal", "")
    if poly_sig in ("CONFIRM", "DIVERGE"):
        parts.append(f"📊 Markets: {poly_sig}")
    return " | ".join(parts)


def _total_reasoning(g: dict) -> str:
    exp   = g.get("exp_total", 0)
    line  = g.get("total_line", 8.5)
    diff  = abs(exp - line)

    # Use market line if available
    market_line = g.get("total_odds_line")
    display_line = market_line if market_line else line
    parts = [f"Model projects {exp:.1f} runs vs {display_line} line ({diff:.1f} run edge)"]

    # Line movement on totals
    tot_sig    = g.get("total_signal", "")
    total_move = g.get("total_move")
    total_adj  = g.get("total_adj", 0)
    if tot_sig in ("STEAM", "DRIFT") and total_move is not None:
        direction  = "WITH model" if total_adj > 0 else "AGAINST model"
        move_dir   = "UP" if total_move > 0 else "DOWN"
        parts.append(f"💰 {tot_sig}: Total moved {move_dir} {abs(total_move):.1f} pts ({direction})")

    pf = g.get("park_runs", 100)
    if pf >= 112:
        parts.append(f"Coors-level hitter park (factor {pf})")
    elif pf >= 106:
        parts.append(f"Hitter-friendly park (factor {pf})")
    elif pf <= 96:
        parts.append(f"Pitcher-friendly park (factor {pf})")

    away_era = g.get("away_sp_era_adj")
    home_era = g.get("home_sp_era_adj")
    if away_era and home_era:
        avg = (away_era + home_era) / 2
        if avg <= 3.50:
            parts.append(f"Elite pitching matchup (avg ERA {avg:.2f})")
        elif avg >= 4.80:
            parts.append(f"Weak pitching matchup (avg ERA {avg:.2f})")

    # Prediction market signal
    poly_sig_t = g.get("poly_market_signal", "")
    if poly_sig_t in ("CONFIRM", "DIVERGE"):
        parts.append(f"📊 Markets: {poly_sig_t}")
    return " | ".join(parts)


def _rl_reasoning(g: dict) -> str:
    margin = abs(g.get("exp_home", 0) - g.get("exp_away", 0))
    ml_pct = g.get("ml_conf", 0) * 100
    parts  = [
        f"ML conf {ml_pct:.0f}%",
        f"Projected margin {margin:.1f} runs",
    ]
    if g.get("away_form_wpct", 0) > 0.60 or g.get("home_form_wpct", 0) > 0.60:
        rl_team = g.get("rl_team", "")
        side_form = g.get("home_form_wpct") if g.get("home_team") == rl_team else g.get("away_form_wpct")
        if side_form:
            parts.append(f"{rl_team} {side_form*100:.0f}% W last 10")
    return " | ".join(parts)



def _build_narrative(g: dict, pick_type: str, pick_team: str) -> str:
    """
    2-3 plain-English sentences summarising the primary edge, a supporting
    signal, and any notable concern.  Displayed on each pick card.

    pick_type : "ML" | "TOTAL" | "RL"
    pick_team : team name we are backing, or "OVER" / "UNDER" for totals
    """
    away = g.get("away_team", "")
    home = g.get("home_team", "")

    away_sp  = (g.get("away_sp") or "TBD").strip()
    home_sp  = (g.get("home_sp") or "TBD").strip()
    away_era = g.get("away_sp_era_adj")
    home_era = g.get("home_sp_era_adj")

    ml_signal  = g.get("ml_signal", "")
    sharp_side = g.get("sharp_side", "")
    poly_sig   = g.get("poly_market_signal", "")
    park_runs  = g.get("park_runs", 100)

    away_fatigue = (g.get("away_fatigue_tier") or "NORMAL").upper()
    home_fatigue = (g.get("home_fatigue_tier") or "NORMAL").upper()

    ml_home_odds = g.get("ml_home_odds")
    ml_away_odds = g.get("ml_away_odds")

    # --- helpers ---
    is_home      = (pick_team == home)
    opp_team     = away if is_home else home
    pick_sp      = home_sp if is_home else away_sp
    opp_sp       = away_sp if is_home else home_sp
    pick_era     = home_era if is_home else away_era
    opp_era      = away_era if is_home else home_era
    pick_fatigue = home_fatigue if is_home else away_fatigue
    opp_fatigue  = away_fatigue if is_home else home_fatigue
    pick_odds    = ml_home_odds if is_home else ml_away_odds
    pick_wpct    = g.get("home_form_wpct") if is_home else g.get("away_form_wpct")
    opp_wpct     = g.get("away_form_wpct") if is_home else g.get("home_form_wpct")

    sentences = []

    # ── PRIMARY EDGE ──────────────────────────────────────────────────────────
    primary = None

    if pick_type in ("ML", "RL"):
        # 1. Steam money ON our side
        if ml_signal == "STEAM" and sharp_side and pick_team in sharp_side:
            primary = (
                f"Sharp money is steaming toward {pick_team} — informed bettors"
                f" are moving this line and the model agrees."
            )

        # 2. Meaningful pitcher mismatch
        if primary is None and pick_era and opp_era:
            era_gap = opp_era - pick_era   # positive → we have the better arm
            if era_gap >= 1.5 and pick_sp.upper() != "TBD":
                primary = (
                    f"{pick_sp} has a significant pitching edge ({pick_era:.2f} ERA"
                    f" vs {opp_era:.2f} for {opp_sp}) — a {era_gap:.1f}-run gap"
                    f" in adjusted ERA is the primary driver here."
                )
            elif era_gap >= 0.75 and pick_sp.upper() != "TBD":
                primary = (
                    f"{pick_sp} carries a real mound advantage ({pick_era:.2f} ERA"
                    f" vs {opp_era:.2f} for {opp_sp}), giving {pick_team}"
                    f" the edge in this matchup."
                )

        # 3. Home dog value
        if primary is None and is_home and pick_odds:
            try:
                if int(pick_odds) > 0:
                    primary = (
                        f"{pick_team} is a home underdog at +{int(pick_odds)}"
                        f" but the model sees more value here than the market is pricing."
                    )
            except (TypeError, ValueError):
                pass

        # 4. Hot recent form gap
        if primary is None and pick_wpct is not None and opp_wpct is not None:
            form_gap = pick_wpct - opp_wpct
            if form_gap >= 0.15:
                primary = (
                    f"{pick_team} has been the hotter club lately"
                    f" ({int(pick_wpct*100)}% W last 10 vs"
                    f" {int(opp_wpct*100)}% for {opp_team})"
                    f" and the model weights recent momentum."
                )

        # Default
        if primary is None:
            ml_conf_pct = round(g.get("ml_conf", 0) * 100)
            primary = (
                f"The model projects {pick_team} as the stronger side at"
                f" {ml_conf_pct}% confidence after accounting for pitcher"
                f" quality, team offense, and recent form."
            )

    else:  # TOTAL
        exp  = g.get("exp_total", 0)
        line = g.get("total_odds_line") or g.get("total_line", 8.5)
        edge = abs(exp - line)
        direction = pick_team   # "OVER" or "UNDER"

        if direction == "OVER" and park_runs >= 108:
            primary = (
                f"This is a hitter-friendly venue (run factor {park_runs})"
                f" and the model projects {exp:.1f} total runs vs a {line}"
                f" line — a {edge:.1f}-run edge for the OVER."
            )
        elif direction == "UNDER" and park_runs <= 96:
            primary = (
                f"This pitcher-friendly park (run factor {park_runs})"
                f" suppresses scoring, and the model only projects {exp:.1f}"
                f" runs vs the {line} line — {edge:.1f} runs of UNDER edge."
            )
        elif away_era and home_era:
            avg_era = (away_era + home_era) / 2
            if avg_era <= 3.50 and direction == "UNDER":
                primary = (
                    f"Elite pitching on both sides (avg ERA {avg_era:.2f})"
                    f" backs the UNDER — model projects just {exp:.1f} runs"
                    f" vs a {line} line."
                )
            elif avg_era >= 4.80 and direction == "OVER":
                primary = (
                    f"Weak starters from both sides (avg ERA {avg_era:.2f})"
                    f" fuel the OVER — model projects {exp:.1f} runs"
                    f" vs a {line} line."
                )
        if primary is None:
            primary = (
                f"The model projects {exp:.1f} total runs, putting the {direction}"
                f" {edge:.1f} runs ahead of the {line} line based on pitcher"
                f" quality and park context."
            )

    sentences.append(primary)

    # ── SUPPORTING SIGNAL ─────────────────────────────────────────────────────
    support = None

    if pick_type in ("ML", "RL"):
        # Bullpen fatigue edge
        if pick_fatigue == "FRESH" and opp_fatigue in ("TIRED", "SPENT"):
            support = (
                f"{pick_team}'s bullpen is fresh while {opp_team}'s pen is"
                f" {opp_fatigue.lower()} — late-game leverage strongly favors"
                f" {pick_team}."
            )
        elif opp_fatigue == "SPENT":
            support = (
                f"The opposing bullpen is spent from heavy recent usage —"
                f" {pick_team} should see vulnerable relievers late."
            )
        # Markets confirm
        if support is None and poly_sig == "CONFIRM":
            support = (
                "Both Kalshi and Polymarket back this side as well,"
                " adding cross-market confirmation on top of the model signal."
            )
        # Park note
        if support is None and park_runs >= 108:
            support = (
                f"The hitter-friendly park (run factor {park_runs})"
                f" can extend margins — a blowout here is a real possibility."
            )
        elif support is None and park_runs <= 93:
            support = (
                f"The pitcher-friendly venue (run factor {park_runs})"
                f" keeps games tight — ML value beats the run-line risk here."
            )
        # Hot form backup
        if support is None and pick_wpct is not None and pick_wpct >= 0.65:
            support = (
                f"{pick_team} has been rolling recently"
                f" ({int(pick_wpct*100)}% W last 10 games),"
                f" adding momentum on top of the model edge."
            )

    else:  # TOTAL
        if poly_sig == "CONFIRM":
            support = (
                "Both prediction markets align with this call,"
                " which adds confidence beyond the model alone."
            )
        elif pick_team == "OVER" and (
            away_fatigue in ("TIRED", "SPENT") or home_fatigue in ("TIRED", "SPENT")
        ):
            tired_team = away if away_fatigue in ("TIRED", "SPENT") else home
            support = (
                f"{tired_team}'s bullpen fatigue could leave the door open"
                f" for extra runs late — favors the OVER."
            )
        elif pick_team == "UNDER" and away_fatigue == "FRESH" and home_fatigue == "FRESH":
            support = (
                "Both bullpens are fresh, which typically means tighter"
                " late-inning control — favorable for the UNDER."
            )

    if support:
        sentences.append(support)

    # ── CONCERN / FLAG ────────────────────────────────────────────────────────
    concern = None

    if pick_type in ("ML", "RL"):
        if poly_sig == "DIVERGE":
            concern = (
                "Note: prediction markets are leaning the other way —"
                " worth monitoring line movement closer to first pitch."
            )
        elif pick_odds:
            try:
                if int(pick_odds) < -220:
                    concern = (
                        f"The heavy juice ({pick_odds}) means you need to hit"
                        f" this at a high clip to be profitable — Kelly sizing"
                        f" is conservative here."
                    )
            except (TypeError, ValueError):
                pass
        if concern is None and ml_signal == "STEAM" and sharp_side and pick_team not in sharp_side:
            concern = (
                f"Caution: steam action is on {sharp_side}, which diverges"
                f" from this pick — monitor before game time."
            )
    else:  # TOTAL
        if poly_sig == "DIVERGE":
            concern = (
                "Note: prediction markets lean the opposite direction —"
                " worth checking for late weather or lineup changes."
            )

    if concern:
        sentences.append(concern)

    return " ".join(sentences)


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTER
# ─────────────────────────────────────────────────────────────────────────────
def format_output(picks: list, parlays_2: list, parlays_3: list,
                  scored_games: list, date: str) -> str:

    W = 68
    lines = []

    def bar(char="="):  lines.append(char * W)
    def blank():        lines.append("")
    def hdr(txt):
        bar()
        lines.append(f"  {txt}")
        bar()

    hdr(f"MLB BETTING PICKS  |  {date}")
    blank()

    if not scored_games:
        lines.append("  No games found for this date.")
        return "\n".join(lines)

    # ── Game-by-game breakdown ────────────────────────────────────────────────
    lines.append("  GAME BREAKDOWN")
    bar("-")

    for g in scored_games:
        away = g["away_team"]
        home = g["home_team"]
        blank()
        lines.append(f"  {away}  @  {home}")

        # Time (convert UTC to rough local reference)
        t = g.get("game_time_utc", "")
        if t:
            lines.append(f"  Time (UTC): {t[11:16]}")

        # Park
        pf   = g["park_runs"]
        note = ""
        if   pf >= 112: note = "  ← extreme hitter park"
        elif pf >= 106: note = "  ← hitter-friendly"
        elif pf <= 96:  note = "  ← pitcher-friendly"
        lines.append(f"  Park: {g['venue']} (run factor {pf}){note}")

        # Pitchers
        a_era = _fmt_era(g.get("away_sp_era_adj"))
        h_era = _fmt_era(g.get("home_sp_era_adj"))
        a_fip = _fmt_era(g.get("away_sp_fip"))
        h_fip = _fmt_era(g.get("home_sp_fip"))
        lines.append(f"  SP: {g['away_sp']:28} ERA {a_era}  FIP {a_fip}  (away)")
        lines.append(f"      {g['home_sp']:28} ERA {h_era}  FIP {h_fip}  (home)")

        # Offense
        lines.append(
            f"  Off: {away} {g['away_rpg']:.2f} RPG "
            f"({g['away_form_rpg']:.1f} recent, OPS {g.get('away_ops') or 'N/A'})  |  "
            f"{home} {g['home_rpg']:.2f} RPG "
            f"({g['home_form_rpg']:.1f} recent, OPS {g.get('home_ops') or 'N/A'})"
        )

        # Projections
        lines.append(
            f"  Proj: {away} {g['exp_away']:.1f}  |  {home} {g['exp_home']:.1f}  "
            f"|  Total {g['exp_total']:.1f}"
        )

        # Win probabilities
        aw = g["away_wp"] * 100
        hw = g["home_wp"] * 100
        lines.append(f"  ML:   {away} {aw:.0f}%  |  {home} {hw:.0f}%")

        # Totals line
        tc = g["total_conf"] * 100
        lines.append(
            f"  Tot:  {g['total_pick']} {g['total_line']}  "
            f"(model {g['exp_total']:.1f} runs, conf {tc:.0f}%)"
        )

        # Run line
        if g["rl_team"]:
            rc = g["rl_conf"] * 100
            lines.append(f"  RL:   {g['rl_pick']} (conf {rc:.0f}%)")

    blank()

    # ── Top individual picks ──────────────────────────────────────────────────
    hdr("TOP INDIVIDUAL PICKS  (ranked by confidence)")
    blank()

    visible = [p for p in picks if p["tier"] != "PASS"]
    if not visible:
        lines.append("  No picks meet the confidence threshold today.")
    else:
        for i, p in enumerate(visible[:12], 1):
            pct    = p["conf"] * 100
            t_icon = tier_emoji(p["tier"])
            lines.append(
                f"  {i:>2}. {p['stars']} [{p['type']:5}] "
                f"{p['label']:<38} {pct:4.1f}%  {t_icon}"
            )
            lines.append(f"       {p['game']}")
            lines.append(f"       {p['reasoning']}")
            blank()

    # ── 2-leg parlays ─────────────────────────────────────────────────────────
    hdr(f"BEST 2-LEG PARLAYS  (est. {PARLAY_PAYOUTS.get(2, '+260')} payout)")
    blank()

    if not parlays_2:
        lines.append("  Not enough qualified legs for 2-leg parlays today.")
    else:
        for i, par in enumerate(parlays_2, 1):
            cpct = par["combined"] * 100
            lines.append(
                f"  Parlay {i} | Combined confidence: {cpct:.1f}% "
                f"| Est. payout: {par['payout']}"
            )
            for leg in par["legs"]:
                lc = leg["conf"] * 100
                lines.append(f"    {leg['stars']}  {leg['label']:<40} {lc:.1f}%")
                lines.append(f"          {leg['game']}")
            blank()

    # ── 3-leg parlays ─────────────────────────────────────────────────────────
    hdr(f"BEST 3-LEG PARLAYS  (est. {PARLAY_PAYOUTS.get(3, '+595')} payout)")
    blank()

    if not parlays_3:
        lines.append("  Not enough qualified legs for 3-leg parlays today.")
    else:
        for i, par in enumerate(parlays_3, 1):
            cpct = par["combined"] * 100
            lines.append(
                f"  Parlay {i} | Combined confidence: {cpct:.1f}% "
                f"| Est. payout: {par['payout']}"
            )
            for leg in par["legs"]:
                lc = leg["conf"] * 100
                lines.append(f"    {leg['stars']}  {leg['label']:<40} {lc:.1f}%")
                lines.append(f"          {leg['game']}")
            blank()

    # ── Footer ────────────────────────────────────────────────────────────────
    bar()
    lines.append("  🔒 LOCK 68%+  |  ⭐⭐ STRONG 62-68%  |  ⭐ LEAN 55-62%")
    lines.append("  Model: Pitcher ERA/FIP + Home/Away Splits + Team RPG + Park Factors + Form")
    bar()

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# CSV EXPORT
# ─────────────────────────────────────────────────────────────────────────────
def save_picks_csv(picks: list, date: str, out_dir: str):
    """Save picks to a dated CSV for tracking and backtesting."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"mlb_picks_{date}.csv")

    fieldnames = ["date", "game", "type", "label", "conf", "tier", "reasoning"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for p in picks:
            w.writerow({
                "date":      date,
                "game":      p["game"],
                "type":      p["type"],
                "label":     p["label"],
                "conf":      round(p["conf"], 4),
                "tier":      p["tier"],
                "reasoning": p["reasoning"],
            })

    log.info(f"Picks saved: {path}")
    return path
