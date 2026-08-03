"""
mlb_picks.py
Converts scored games into ranked individual picks and parlay recommendations.

Confidence tiers:
  LOCK   68%+  (strongest model signal)
  STRONG 62-68%
  LEAN   52-62%
  TOSSUP 48-52% (shown for coverage, no Kelly recommendation)
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

LOCK_THRESH   = 0.68
STRONG_THRESH = 0.62
LEAN_THRESH   = 0.52
TOSSUP_THRESH = 0.48
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
def generate_picks(scored_games: list, cfg: dict = None) -> list:
    """
    Convert scored games into a flat sorted list of individual picks.
    Includes picks down to TOSSUP threshold (48%). TOSSUP picks are shown
    for coverage only — no Kelly recommendation, no parlay inclusion.
    """
    # Use cfg thresholds if provided (from model control panel)
    _lock   = float((cfg or {}).get("tier_lock",   LOCK_THRESH))
    _strong = float((cfg or {}).get("tier_strong", STRONG_THRESH))
    _lean   = float((cfg or {}).get("tier_lean",   LEAN_THRESH))

    def _tier(c):
        if c >= _lock:   return "LOCK"
        if c >= _strong: return "STRONG"
        if c >= _lean:   return "LEAN"
        if c >= TOSSUP_THRESH: return "TOSSUP"
        return "PASS"

    picks = []

    for g in scored_games:
        game_label = f"{g['away_team']} @ {g['home_team']}"

        # ── TBD starter suppression ─────────────────────────────────────────
        # Run projections (totals, run line) lean heavily on the starting
        # pitcher. When a starter is unannounced ("TBD"), those numbers are
        # guesses off a league-average filler, so we do NOT publish TOTAL or RL
        # for that game. Moneyline is more robust (team quality carries it), so
        # it still publishes but is knocked down one tier — and capped below
        # LOCK — when BOTH starters are TBD, since that is the least certain case.
        _away_tbd = (g.get("away_sp", "TBD") or "TBD").strip().upper() == "TBD"
        _home_tbd = (g.get("home_sp", "TBD") or "TBD").strip().upper() == "TBD"
        _either_tbd = _away_tbd or _home_tbd
        _both_tbd   = _away_tbd and _home_tbd

        # ── Moneyline ──────────────────────────────────────────────────────
        ml_conf = g["ml_conf"]
        if _both_tbd:
            # Cap below LOCK and drop a tier — never a LOCK on two unknown SPs.
            ml_conf = min(ml_conf, LOCK_THRESH - 0.001)
        if ml_conf >= TOSSUP_THRESH:
            # opp_team: whichever team is NOT the ml_team
            ml_team = g["ml_team"]
            opp_team = g["home_team"] if ml_team == g["away_team"] else g["away_team"]
            picks.append({
                "type":       "ML",
                "label":      f"{ml_team} ML",
                "team":       ml_team,
                "opp_team":   opp_team,
                "side":       g["ml_side"],
                "conf":       ml_conf,
                "tier":       _tier(ml_conf),
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
        if tot_conf >= TOSSUP_THRESH and not _either_tbd:
            total_pick = g["total_pick"]
            opp_side = "UNDER" if total_pick == "OVER" else "OVER"
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
                "game_data":  g,
            })

        # ── Run Line ───────────────────────────────────────────────────────
        rl_conf = g["rl_conf"]
        if rl_conf >= TOSSUP_THRESH and g["rl_team"] and not _either_tbd:
            rl_team = g["rl_team"]
            opp_team = g["home_team"] if rl_team == g["away_team"] else g["away_team"]
            picks.append({
                "type":       "RL",
                "label":      g["rl_pick"],
                "team":       rl_team,
                "opp_team":   opp_team,
                "side":       "rl",
                "conf":       rl_conf,
                "tier":       _tier(rl_conf),
                "stars":      stars(rl_conf),
                "game":       game_label,
                "game_id":    g["game_id"],
                "venue":      g["venue"],
                "exp_total":  g["exp_total"],
                "reasoning":  _rl_reasoning(g),
                "game_data":  g,
            })

    # ── Attach market VALUE / EV to every pick (advisory; not a filter) ─────────
    # Confidence says how likely; value says whether the price is wrong. A
    # high-confidence heavy favorite can still be a NO VALUE / CHALK play.
    try:
        from model.value import value_for_pick
        for p in picks:
            p["value"] = value_for_pick(p)
    except Exception:
        for p in picks:
            p.setdefault("value", {"market_prob": None, "edge": None,
                                   "ev": None, "tag": "", "chalk": False})

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

        # Real combined book odds from each leg's actual price (ML legs carry
        # ml_away/ml_home on game_data). If any leg has no retrievable price
        # (totals/RL prices aren't stored), leave book_odds None and fall back
        # to the generic payout in the display.
        dec = 1.0
        have_all = True
        for p in combo:
            gd = p.get("game_data", {}) or {}
            am = None
            if p.get("type") == "ML":
                away = gd.get("away_team", "")
                pk   = (p.get("team") or "").lower()
                am = gd.get("ml_away_odds") if (away and (away.lower() in pk or pk in away.lower())) \
                     else gd.get("ml_home_odds")
            if am in (None, "", 0):
                have_all = False
                break
            am = float(am)
            dec *= (1 + am/100.0) if am > 0 else (1 + 100.0/abs(am))
        book_odds = None
        if have_all and dec > 1.0:
            net = dec - 1.0
            book_odds = f"+{round(net*100)}" if net >= 1 else f"-{round(100/net)}"

        parlays.append({
            "legs":        list(combo),
            "n_legs":      legs,
            "combined":    round(combined, 4),
            "payout":      PARLAY_PAYOUTS.get(legs, f"+{legs*200}"),
            "book_odds":   book_odds,   # real combined American odds (ML), else None
            "summary":     " + ".join(p["label"] for p in combo),
            "min_leg":     min(p["conf"] for p in combo),
        })

    parlays.sort(key=lambda x: x["combined"], reverse=True)
    return parlays[:max_parlays]


# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# THEMATIC PARLAY BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def build_thematic_parlays(picks: list, max_parlays: int = 3) -> list:
    """
    Build thematic 2-leg parlays grouped by pick type/direction.
    E.g. all-favorites, all-overs, all-unders, etc.
    Returns a list of parlay dicts (same shape as build_parlays output).
    """
    from itertools import combinations as _comb

    qualified = [p for p in picks if p.get("conf", 0) >= PARLAY_MIN]

    def _theme(p):
        label = (p.get("label") or "").lower()
        t = p.get("pick_type", "")
        if t == "TOTAL":
            return "over" if "over" in label else "under"
        if t == "RL":
            return "rl"
        return "ml"

    results = []
    for combo in _comb(qualified, 2):
        game_ids = [p["game_id"] for p in combo]
        if len(set(game_ids)) < 2:
            continue
        if _theme(combo[0]) != _theme(combo[1]):
            continue
        combined = combo[0]["conf"] * combo[1]["conf"]
        theme_label = {
            "ml": "Moneyline plays",
            "over": "Over plays",
            "under": "Under plays",
            "rl": "Run line plays",
        }.get(_theme(combo[0]), "Thematic plays")
        results.append({
            "legs":     list(combo),
            "n_legs":   2,
            "combined": round(combined, 4),
            "payout":   PARLAY_PAYOUTS.get(2, "+260"),
            "summary":  " + ".join(p["label"] for p in combo),
            "min_leg":  min(p["conf"] for p in combo),
            "theme":    _theme(combo[0]),
            "thesis":      theme_label,
            "thesis_desc": theme_label + ": " + " + ".join(p["label"] for p in combo),
        })

    results.sort(key=lambda x: x["combined"], reverse=True)
    return results[:max_parlays]

# REASONING STRINGS
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_era(val) -> str:
    return f"{float(val):.2f}" if val is not None else "N/A"


def _ml_reasoning(g: dict) -> str:
    parts = []

    # Pitcher matchup + recent form trend. DISPLAY the raw SEASON ERA (what the user
    # verifies online), not the model's split-adjusted era_adj — showing the adjusted
    # number made cards look "wrong" vs Baseball-Reference. The model still USES era_adj
    # internally for scoring; this only changes what's shown.
    away_era = g.get("away_sp_era", g.get("away_sp_era_adj"))
    home_era = g.get("home_sp_era", g.get("home_sp_era_adj"))
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
