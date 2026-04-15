"""
mlb_picks.py
Converts scored games into ranked individual picks and parlay recommendations.

Confidence tiers:
  LOCK   68%+  (strongest model signal)
  STRONG 62-68%
  LEAN   55-62%
  PASS   <55%  (not included in output)

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

LOCK_THRESH   = 0.68
STRONG_THRESH = 0.62
LEAN_THRESH   = 0.55
PARLAY_MIN    = 0.57   # minimum per-leg confidence for parlay inclusion

# Approximate payout at -110 per leg (American odds)
PARLAY_PAYOUTS = {2: "+260", 3: "+595", 4: "+1228", 5: "+2435"}


# ─────────────────────────────────────────────────────────────────────────────
# TIER HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def tier(conf: float) -> str:
    if conf >= LOCK_THRESH:   return "LOCK"
    if conf >= STRONG_THRESH: return "STRONG"
    if conf >= LEAN_THRESH:   return "LEAN"
    return "PASS"


def stars(conf: float) -> str:
    if conf >= LOCK_THRESH:   return "★★★"
    if conf >= STRONG_THRESH: return "★★ "
    if conf >= LEAN_THRESH:   return "★  "
    return "   "


def tier_emoji(t: str) -> str:
    return {"LOCK": "🔒", "STRONG": "⭐⭐", "LEAN": "⭐", "PASS": "—"}.get(t, "")


# ─────────────────────────────────────────────────────────────────────────────
# PICK GENERATION
# ─────────────────────────────────────────────────────────────────────────────
def generate_picks(scored_games: list) -> list:
    """
    Convert scored games into a flat sorted list of individual picks.
    Only includes picks at or above LEAN threshold.
    """
    picks = []

    for g in scored_games:
        game_label = f"{g['away_team']} @ {g['home_team']}"

        # ── Moneyline ──────────────────────────────────────────────────────
        ml_conf = g["ml_conf"]
        if ml_conf >= LEAN_THRESH:
            picks.append({
                "type":       "ML",
                "label":      f"{g['ml_team']} ML",
                "team":       g["ml_team"],
                "side":       g["ml_side"],
                "conf":       ml_conf,
                "tier":       tier(ml_conf),
                "stars":      stars(ml_conf),
                "game":       game_label,
                "game_id":    g["game_id"],
                "venue":      g["venue"],
                "exp_total":  g["exp_total"],
                "reasoning":  _ml_reasoning(g),
                "game_data":  g,
            })

        # ── Totals ─────────────────────────────────────────────────────────
        tot_conf = g["total_conf"]
        if tot_conf >= LEAN_THRESH:
            picks.append({
                "type":       "TOTAL",
                "label":      f"{g['total_pick']} {g['total_line']}",
                "team":       g["total_pick"],
                "side":       g["total_pick"].lower(),
                "conf":       tot_conf,
                "tier":       tier(tot_conf),
                "stars":      stars(tot_conf),
                "game":       game_label,
                "game_id":    g["game_id"],
                "venue":      g["venue"],
                "exp_total":  g["exp_total"],
                "reasoning":  _total_reasoning(g),
                "game_data":  g,
            })

        # ── Run Line ───────────────────────────────────────────────────────
        rl_conf = g["rl_conf"]
        if rl_conf >= LEAN_THRESH and g["rl_team"]:
            picks.append({
                "type":       "RL",
                "label":      g["rl_pick"],
                "team":       g["rl_team"],
                "side":       "rl",
                "conf":       rl_conf,
                "tier":       tier(rl_conf),
                "stars":      stars(rl_conf),
                "game":       game_label,
                "game_id":    g["game_id"],
                "venue":      g["venue"],
                "exp_total":  g["exp_total"],
                "reasoning":  _rl_reasoning(g),
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

    # Pitcher matchup
    away_era = g.get("away_sp_era_adj")
    home_era = g.get("home_sp_era_adj")
    away_sp  = g.get("away_sp", "TBD")
    home_sp  = g.get("home_sp", "TBD")

    if not g.get("away_sp_missing") and not g.get("home_sp_missing"):
        parts.append(f"{away_sp} ERA {_fmt_era(away_era)} vs {home_sp} ERA {_fmt_era(home_era)}")
    elif not g.get("away_sp_missing"):
        parts.append(f"{away_sp} ERA {_fmt_era(away_era)} (home SP TBD)")
    elif not g.get("home_sp_missing"):
        parts.append(f"Away SP TBD vs {home_sp} ERA {_fmt_era(home_era)}")

    # Recent form
    ar = g.get("away_form_rpg", 0)
    hr = g.get("home_form_rpg", 0)
    aw = g.get("away_form_wpct", 0)
    hw = g.get("home_form_wpct", 0)
    parts.append(
        f"Recent: {g['away_team']} {ar:.1f} RPG ({aw*100:.0f}% W) | "
        f"{g['home_team']} {hr:.1f} RPG ({hw*100:.0f}% W)"
    )

    # Park note
    pf = g.get("park_runs", 100)
    if pf >= 108:
        parts.append(f"Hitter park (factor {pf})")
    elif pf <= 96:
        parts.append(f"Pitcher park (factor {pf})")

    return " | ".join(parts)


def _total_reasoning(g: dict) -> str:
    exp   = g.get("exp_total", 0)
    line  = g.get("total_line", 8.5)
    pick  = g.get("total_pick", "")
    diff  = abs(exp - line)
    parts = [f"Model projects {exp:.1f} runs vs {line} line ({diff:.1f} run edge)"]

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
