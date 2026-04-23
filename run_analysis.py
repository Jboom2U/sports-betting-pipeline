"""
run_analysis.py
Grades yesterday's picks against actual MLB results and generates a
structured performance report with model improvement recommendations.

Usage:
    python run_analysis.py                   # grade yesterday
    python run_analysis.py --date 2026-04-16 # grade specific date
    python run_analysis.py --days 7          # grade last 7 days (summary)

Output:
    picks/mlb_analysis_YYYY-MM-DD.json   — structured data for HTML dashboard
    Printed report to stdout
"""

import sys
import os
import csv
import json
import logging
import argparse
import re
from datetime import datetime, timedelta

import requests

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(__file__)
PICKS_DIR  = os.path.join(BASE_DIR, "picks")
CLEAN_DIR  = os.path.join(BASE_DIR, "data", "clean")
MLB_API    = "https://statsapi.mlb.com/api/v1"


# ─────────────────────────────────────────────────────────────────────────────
# FETCH RESULTS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_results(date: str) -> dict:
    """
    Pull final scores from MLB Stats API for the given date.
    Returns dict keyed by (away_team, home_team) -> {away_score, home_score, total}.
    """
    url = f"{MLB_API}/schedule"
    params = {
        "sportId":  1,
        "date":     date,
        "hydrate":  "linescore,decisions",
        "gameType": "R",
    }
    try:
        resp = requests.get(url, params=params,
                            headers={"User-Agent": "mlb-betting-pipeline/1.0"},
                            timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Failed to fetch results for {date}: {e}")
        return {}

    results = {}
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            status = game.get("status", {}).get("abstractGameState", "")
            if status != "Final":
                continue
            away_team  = game["teams"]["away"]["team"]["name"]
            home_team  = game["teams"]["home"]["team"]["name"]
            away_score = int(game["teams"]["away"].get("score", 0) or 0)
            home_score = int(game["teams"]["home"].get("score", 0) or 0)
            results[(away_team, home_team)] = {
                "away_team":  away_team,
                "home_team":  home_team,
                "away_score": away_score,
                "home_score": home_score,
                "total":      away_score + home_score,
            }
    log.info(f"Results fetched: {len(results)} final games on {date}")
    return results


def load_results_from_csv(date: str) -> dict:
    """Fallback: load results from local scores master CSV."""
    path = os.path.join(CLEAN_DIR, "mlb_scores_master.csv")
    if not os.path.exists(path):
        return {}
    results = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("game_date", "") != date:
                continue
            away = row.get("away_team", "").strip()
            home = row.get("home_team", "").strip()
            try:
                away_score = int(float(row.get("away_score", 0) or 0))
                home_score = int(float(row.get("home_score", 0) or 0))
            except (ValueError, TypeError):
                continue
            if away and home:
                results[(away, home)] = {
                    "away_team":  away,
                    "home_team":  home,
                    "away_score": away_score,
                    "home_score": home_score,
                    "total":      away_score + home_score,
                }
    return results


# ─────────────────────────────────────────────────────────────────────────────
# LOAD PICKS
# ─────────────────────────────────────────────────────────────────────────────

def load_picks(date: str) -> list:
    """Load picks CSV for the given date."""
    path = os.path.join(PICKS_DIR, f"mlb_picks_{date}.csv")
    if not os.path.exists(path):
        log.warning(f"Picks file not found: {path}")
        return []
    picks = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            picks.append({
                "date":      row.get("date", date),
                "game":      row.get("game", "").strip(),
                "type":      row.get("type", "").strip().upper(),
                "label":     row.get("label", "").strip(),
                "conf":      float(row.get("conf", 0) or 0),
                "tier":      row.get("tier", "").strip().upper(),
                "reasoning": row.get("reasoning", "").strip(),
            })
    log.info(f"Picks loaded: {len(picks)} for {date}")
    return picks


# ─────────────────────────────────────────────────────────────────────────────
# GAME MATCHING
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_team(name: str) -> str:
    """Normalize team name for fuzzy matching."""
    return name.lower().strip()


def find_result(game_str: str, results: dict):
    """
    Match 'Away Team @ Home Team' string to a results entry.
    Returns result dict or None.
    """
    if " @ " not in game_str:
        return None
    away_raw, home_raw = game_str.split(" @ ", 1)
    away_raw = away_raw.strip()
    home_raw = home_raw.strip()

    # Exact match first
    key = (away_raw, home_raw)
    if key in results:
        return results[key]

    # Fuzzy: check if picks team name is contained in results team name or vice versa
    aw = _normalize_team(away_raw)
    ho = _normalize_team(home_raw)
    for (ra, rh), res in results.items():
        ra_n = _normalize_team(ra)
        rh_n = _normalize_team(rh)
        if (aw in ra_n or ra_n in aw) and (ho in rh_n or rh_n in ho):
            return res
    return None


# ─────────────────────────────────────────────────────────────────────────────
# GRADING
# ─────────────────────────────────────────────────────────────────────────────

def _extract_total_line(label: str):
    """Extract direction and line from 'OVER 8.5' or 'UNDER 8.5'."""
    m = re.search(r'(OVER|UNDER)\s+([\d.]+)', label.upper())
    if m:
        return m.group(1), float(m.group(2))
    return None, None


def _extract_rl_info(label: str):
    """
    Extract team and spread from run line label.
    e.g. 'New York Yankees -1.5' -> ('New York Yankees', -1.5)
    """
    m = re.search(r'^(.*?)\s+([+-][\d.]+)\s*$', label.strip())
    if m:
        team   = m.group(1).strip()
        spread = float(m.group(2))
        return team, spread
    return None, None


def _extract_edge_from_reasoning(reasoning: str) -> float:
    """Pull the run edge claim from TOTAL reasoning string."""
    m = re.search(r'\(([\d.]+)\s*run\s*edge\)', reasoning, re.IGNORECASE)
    return float(m.group(1)) if m else 0.0


def grade_pick(pick: dict, result: dict) -> str:
    """
    Grade a single pick against a game result.
    Returns 'WIN', 'LOSS', or 'PUSH'.
    """
    if result is None:
        return "NO_RESULT"

    ptype  = pick["type"]
    label  = pick["label"]
    away_s = result["away_score"]
    home_s = result["home_score"]
    total  = result["total"]

    if ptype == "ML":
        # label: "Team Name ML"
        team = label.replace(" ML", "").strip()
        team_n = _normalize_team(team)
        away_n = _normalize_team(result["away_team"])
        home_n = _normalize_team(result["home_team"])

        if team_n in away_n or away_n in team_n:
            if away_s > home_s:   return "WIN"
            if away_s == home_s:  return "PUSH"
            return "LOSS"
        elif team_n in home_n or home_n in team_n:
            if home_s > away_s:   return "WIN"
            if home_s == away_s:  return "PUSH"
            return "LOSS"
        return "NO_RESULT"

    elif ptype == "TOTAL":
        direction, line = _extract_total_line(label)
        if line is None:
            return "NO_RESULT"
        if total == line:  return "PUSH"
        if direction == "OVER":
            return "WIN" if total > line else "LOSS"
        else:  # UNDER
            return "WIN" if total < line else "LOSS"

    elif ptype == "RL":
        team, spread = _extract_rl_info(label)
        if team is None:
            return "NO_RESULT"
        team_n = _normalize_team(team)
        away_n = _normalize_team(result["away_team"])
        home_n = _normalize_team(result["home_team"])

        if team_n in away_n or away_n in team_n:
            margin = away_s - home_s + spread
        elif team_n in home_n or home_n in team_n:
            margin = home_s - away_s + spread
        else:
            return "NO_RESULT"

        if margin > 0:   return "WIN"
        if margin == 0:  return "PUSH"
        return "LOSS"

    return "NO_RESULT"


def calc_profit(pick: dict, result_str: str) -> float:
    """
    Flat-unit profit/loss.
    ML uses -110 standard unless odds are embedded in reasoning.
    Props assumed -150 (not graded here — game picks only).
    """
    if result_str == "PUSH":   return 0.0
    if result_str == "LOSS":   return -1.0
    if result_str != "WIN":    return 0.0

    # Try to extract ML odds from reasoning
    reasoning = pick.get("reasoning", "")
    ptype = pick["type"]
    if ptype == "ML":
        m = re.search(r'(?:' + re.escape(pick["label"].replace(" ML","").strip()) +
                      r'.*?)([+-]\d{3,4})', reasoning, re.IGNORECASE)
        if m:
            odds = int(m.group(1))
            if odds > 0:
                return odds / 100.0
            else:
                return 100.0 / abs(odds)
    # Default -110
    return 100.0 / 110.0


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATE METRICS
# ─────────────────────────────────────────────────────────────────────────────

def _empty_bucket():
    return {"wins": 0, "losses": 0, "pushes": 0, "total": 0,
            "profit": 0.0, "staked": 0.0}


def _add_result(bucket, result_str, profit):
    if result_str in ("WIN", "LOSS", "PUSH"):
        bucket["total"]  += 1
        bucket["staked"] += 1.0
        bucket["profit"] += profit
        if result_str == "WIN":    bucket["wins"]   += 1
        elif result_str == "LOSS": bucket["losses"] += 1
        else:                      bucket["pushes"]  += 1


def _finalize(bucket):
    denom = bucket["wins"] + bucket["losses"]
    bucket["win_rate"] = round(bucket["wins"] / denom, 3) if denom > 0 else None
    bucket["roi"]      = round(bucket["profit"] / bucket["staked"], 3) \
                         if bucket["staked"] > 0 else None
    bucket["profit"]   = round(bucket["profit"], 3)
    return bucket


def compute_metrics(graded_picks: list) -> dict:
    overall   = _empty_bucket()
    by_tier   = {t: _empty_bucket() for t in ("LOCK", "STRONG", "LEAN")}
    by_type   = {t: _empty_bucket() for t in ("ML", "TOTAL", "RL")}
    over_stats  = _empty_bucket()
    under_stats = _empty_bucket()

    for p in graded_picks:
        r   = p["result"]
        pft = p["profit"]
        _add_result(overall, r, pft)

        tier = p["tier"]
        if tier in by_tier:
            _add_result(by_tier[tier], r, pft)

        ptype = p["type"]
        if ptype in by_type:
            _add_result(by_type[ptype], r, pft)

        # OVER/UNDER split
        if ptype == "TOTAL":
            direction, _ = _extract_total_line(p["label"])
            if direction == "OVER":
                _add_result(over_stats, r, pft)
            elif direction == "UNDER":
                _add_result(under_stats, r, pft)

    _finalize(overall)
    for b in by_tier.values(): _finalize(b)
    for b in by_type.values(): _finalize(b)
    _finalize(over_stats)
    _finalize(under_stats)

    return {
        "overall":    overall,
        "by_tier":    by_tier,
        "by_type":    by_type,
        "over_bias":  over_stats,
        "under_bias": under_stats,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RECOMMENDATIONS ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def generate_findings(metrics: dict, graded_picks: list) -> list:
    findings = []
    overall  = metrics["overall"]
    by_tier  = metrics["by_tier"]
    by_type  = metrics["by_type"]
    overs    = metrics["over_bias"]
    unders   = metrics["under_bias"]

    total_graded = overall["wins"] + overall["losses"]
    if total_graded == 0:
        return ["No graded picks found for this date — results may not be final yet."]

    # Overall record
    wr = overall["win_rate"]
    roi = overall["roi"]
    findings.append(
        f"Overall: {overall['wins']}-{overall['losses']} ({wr*100:.1f}% WR) | "
        f"ROI: {roi*100:+.1f}% on {overall['staked']:.0f} units staked"
    )

    # LOCK tier
    lock = by_tier.get("LOCK", {})
    if lock.get("total", 0) > 0:
        lwr = lock.get("win_rate")
        findings.append(
            f"LOCK tier: {lock['wins']}-{lock['losses']} ({lwr*100:.1f}%) — "
            f"{'✓ meeting expectations' if lwr and lwr >= 0.65 else '⚠ underperforming for highest-confidence tier'}"
        )

    # STRONG tier
    strong = by_tier.get("STRONG", {})
    if strong.get("total", 0) > 0:
        swr = strong.get("win_rate")
        findings.append(
            f"STRONG tier: {strong['wins']}-{strong['losses']} ({swr*100:.1f}%) — "
            f"{'✓ on track' if swr and swr >= 0.58 else '⚠ below 62% expected threshold'}"
        )

    # LEAN tier
    lean = by_tier.get("LEAN", {})
    if lean.get("total", 0) > 0:
        lwr2 = lean.get("win_rate")
        findings.append(
            f"LEAN tier: {lean['wins']}-{lean['losses']} ({lwr2*100:.1f}%) — "
            f"{'✓ solid for LEAN range' if lwr2 and lwr2 >= 0.52 else '⚠ consider filtering lowest-conf LEANs'}"
        )

    # OVER bias
    if overs.get("total", 0) >= 2:
        owr = overs.get("win_rate", 0) or 0
        findings.append(
            f"OVERs: {overs['wins']}-{overs['losses']} ({owr*100:.1f}%) — "
            f"{'⚠ OVER bias confirmed — model systematically projects too many runs' if owr < 0.45 else 'within range'}"
        )

    if unders.get("total", 0) >= 2:
        uwr = unders.get("win_rate", 0) or 0
        findings.append(f"UNDERs: {unders['wins']}-{unders['losses']} ({uwr*100:.1f}%)")

    # Totals overall
    totals = by_type.get("TOTAL", {})
    if totals.get("total", 0) >= 3:
        twr = totals.get("win_rate", 0) or 0
        if twr < 0.40:
            findings.append(
                "⚠ Totals model is significantly below breakeven — retune run-expectancy weights"
            )

    # ML performance
    ml = by_type.get("ML", {})
    if ml.get("total", 0) >= 2:
        mlwr = ml.get("win_rate", 0) or 0
        findings.append(
            f"Moneyline: {ml['wins']}-{ml['losses']} ({mlwr*100:.1f}%) | "
            f"Profit: {ml['profit']:+.2f}u — "
            f"{'strongest segment today' if ml['roi'] and ml['roi'] > 0 else 'unprofitable — check heavy-fav exposure'}"
        )

    # Heavy favorite losses
    heavy_losses = [
        p for p in graded_picks
        if p["type"] == "ML"
        and p["result"] == "LOSS"
        and p["conf"] >= 0.72
    ]
    if heavy_losses:
        findings.append(
            f"⚠ {len(heavy_losses)} heavy-favorite ML(s) lost (conf ≥72%) — "
            f"review juice vs edge before publishing high-confidence ML favorites"
        )

    # TBD SP losses in totals
    tbd_losses = [
        p for p in graded_picks
        if p["type"] == "TOTAL"
        and p["result"] == "LOSS"
        and "TBD" in p.get("reasoning", "")
    ]
    if tbd_losses:
        findings.append(
            f"⚠ {len(tbd_losses)} TOTAL loss(es) on games with TBD starting pitchers — "
            "suppress confidence or skip totals when SP is unknown"
        )

    # Thin-edge total losses
    thin_losses = [
        p for p in graded_picks
        if p["type"] == "TOTAL"
        and p["result"] == "LOSS"
        and _extract_edge_from_reasoning(p.get("reasoning", "")) < 0.8
    ]
    if thin_losses:
        findings.append(
            f"⚠ {len(thin_losses)} thin-edge TOTAL loss(es) (<0.8 run edge) — "
            "consider raising minimum edge threshold for publishing TOTAL picks"
        )

    return findings


def generate_recommendations(metrics: dict, graded_picks: list) -> list:
    recs = []
    by_type  = metrics["by_type"]
    by_tier  = metrics["by_tier"]
    overs    = metrics["over_bias"]

    totals = by_type.get("TOTAL", {})
    if totals.get("total", 0) >= 3 and (totals.get("win_rate") or 1) < 0.45:
        recs.append({
            "priority": "HIGH",
            "area":     "Totals model",
            "action":   "OVERs are systematically too high. Add bullpen recency weight "
                        "and cap TOTAL picks where run edge < 1.0 or SP is TBD."
        })

    owr = overs.get("win_rate")
    if overs.get("total", 0) >= 2 and owr is not None and owr < 0.45:
        recs.append({
            "priority": "HIGH",
            "area":     "OVER bias",
            "action":   f"OVERs went {overs['wins']}-{overs['losses']} ({owr*100:.0f}%). "
                        "Retune run-expectancy baseline downward or apply a 5-10% shrinkage "
                        "to projected run totals before comparing to the line."
        })

    lock = by_tier.get("LOCK", {})
    if lock.get("total", 0) >= 2 and (lock.get("win_rate") or 1) < 0.60:
        recs.append({
            "priority": "HIGH",
            "area":     "LOCK tier calibration",
            "action":   "LOCK picks underperformed. Raise LOCK threshold to 72%+ "
                        "or remove heavy favorites (worse than -200) from LOCK tier."
        })

    strong = by_tier.get("STRONG", {})
    if strong.get("total", 0) >= 3 and (strong.get("win_rate") or 1) < 0.50:
        recs.append({
            "priority": "MED",
            "area":     "STRONG tier calibration",
            "action":   "STRONG tier hit below 50%. Consider raising threshold to 65%+ "
                        "or reviewing feature weights for 62-68% confidence range."
        })

    lean = by_tier.get("LEAN", {})
    if lean.get("total", 0) >= 4 and (lean.get("win_rate") or 1) < 0.48:
        recs.append({
            "priority": "MED",
            "area":     "LEAN tier filtering",
            "action":   "LEAN picks are below breakeven. Consider treating LEANs as "
                        "informational only rather than published recommendations, "
                        "or raise the LEAN threshold from 55% to 58%."
        })

    tbd_in_picks = [
        p for p in graded_picks
        if "TBD" in p.get("reasoning", "")
    ]
    if tbd_in_picks:
        recs.append({
            "priority": "MED",
            "area":     "TBD starter suppression",
            "action":   f"{len(tbd_in_picks)} picks included games with TBD starters. "
                        "Auto-downgrade picks to LEAN (or suppress TOTAL picks entirely) "
                        "when either SP is TBD at publish time."
        })

    if not recs:
        recs.append({
            "priority": "INFO",
            "area":     "General",
            "action":   "No critical issues identified today. Continue monitoring "
                        "OVER bias and heavy-favorite ML exposure."
        })

    return recs


# ─────────────────────────────────────────────────────────────────────────────
# PRINT REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_report(date: str, graded_picks: list, metrics: dict,
                 findings: list, recs: list):
    sep = "=" * 65
    print(f"\n{sep}")
    print(f"  MLB PICKS PERFORMANCE REPORT — {date}")
    print(sep)

    # Graded picks table
    print(f"\n{'#':<4} {'Tier':<8} {'Type':<7} {'Label':<32} {'Conf':>6}  Result")
    print("-" * 65)
    for i, p in enumerate(graded_picks, 1):
        result_icon = {"WIN": "✓", "LOSS": "✗", "PUSH": "—",
                       "NO_RESULT": "?"}.get(p["result"], "?")
        print(f"{i:<4} {p['tier']:<8} {p['type']:<7} {p['label']:<32} "
              f"{p['conf']*100:5.1f}%  {result_icon} {p['result']}")

    # Summary
    print(f"\n{sep}")
    print("  SUMMARY")
    print(sep)
    for f in findings:
        print(f"  {f}")

    # By tier
    print(f"\n  Tier Breakdown:")
    for t, b in metrics["by_tier"].items():
        if b["total"] > 0:
            wr = f"{b['win_rate']*100:.1f}%" if b["win_rate"] is not None else "n/a"
            print(f"    {t:<8}  {b['wins']}-{b['losses']}  ({wr})  "
                  f"ROI: {b['roi']*100:+.1f}%" if b['roi'] is not None
                  else f"    {t:<8}  {b['wins']}-{b['losses']}  ({wr})")

    # By type
    print(f"\n  Type Breakdown:")
    for t, b in metrics["by_type"].items():
        if b["total"] > 0:
            wr = f"{b['win_rate']*100:.1f}%" if b["win_rate"] is not None else "n/a"
            print(f"    {t:<8}  {b['wins']}-{b['losses']}  ({wr})")

    # Recommendations
    print(f"\n{sep}")
    print("  RECOMMENDATIONS")
    print(sep)
    for r in recs:
        print(f"  [{r['priority']}] {r['area']}")
        print(f"         {r['action']}\n")


# ─────────────────────────────────────────────────────────────────────────────
# SAVE JSON
# ─────────────────────────────────────────────────────────────────────────────

def save_analysis(date: str, graded_picks: list, metrics: dict,
                  findings: list, recs: list):
    """Save structured analysis JSON for use by the HTML dashboard."""
    os.makedirs(PICKS_DIR, exist_ok=True)
    path = os.path.join(PICKS_DIR, f"mlb_analysis_{date}.json")

    # Serialize graded picks (drop heavy game_data refs)
    serializable_picks = []
    for p in graded_picks:
        serializable_picks.append({
            "label":     p["label"],
            "game":      p["game"],
            "type":      p["type"],
            "tier":      p["tier"],
            "conf":      round(p["conf"] * 100, 1),
            "result":    p["result"],
            "profit":    round(p["profit"], 3),
            "reasoning": p.get("reasoning", ""),
        })

    payload = {
        "date":           date,
        "graded_picks":   serializable_picks,
        "metrics":        metrics,
        "findings":       findings,
        "recommendations": recs,
        "generated_at":   datetime.now().isoformat(),
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    log.info(f"Analysis saved: {path}")
    return path


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run(date: str):
    """Grade picks for a single date. Returns the analysis dict."""
    log.info(f"Grading picks for {date}")

    picks = load_picks(date)
    if not picks:
        log.warning(f"No picks found for {date}")
        return None

    # Try API first, fall back to CSV
    results = fetch_results(date)
    if not results:
        log.info("API returned no results — trying local CSV")
        results = load_results_from_csv(date)
    if not results:
        log.warning(f"No results found for {date} — game may not be final yet")
        return None

    # Grade each pick
    graded = []
    for p in picks:
        result_obj = find_result(p["game"], results)
        result_str = grade_pick(p, result_obj)
        profit     = calc_profit(p, result_str)
        graded.append({**p, "result": result_str, "profit": profit,
                        "result_game": result_obj})

    # Compute metrics
    metrics  = compute_metrics(graded)
    findings = generate_findings(metrics, graded)
    recs     = generate_recommendations(metrics, graded)

    # Output
    print_report(date, graded, metrics, findings, recs)
    save_analysis(date, graded, metrics, findings, recs)

    return {
        "date":     date,
        "graded":   graded,
        "metrics":  metrics,
        "findings": findings,
        "recs":     recs,
    }


def main():
    parser = argparse.ArgumentParser(description="MLB Picks Performance Analyzer")
    parser.add_argument("--date", default=None,
                        help="Date to grade (YYYY-MM-DD). Defaults to yesterday.")
    parser.add_argument("--days", type=int, default=None,
                        help="Grade the last N days and print a rolling summary.")
    args = parser.parse_args()

    if args.days:
        # Rolling window
        summaries = []
        for d in range(args.days, 0, -1):
            target = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
            result = run(target)
            if result:
                summaries.append(result)

        if summaries:
            print(f"\n{'='*65}")
            print(f"  {args.days}-DAY ROLLING SUMMARY")
            print(f"{'='*65}")
            total_w = sum(r["metrics"]["overall"]["wins"]   for r in summaries)
            total_l = sum(r["metrics"]["overall"]["losses"] for r in summaries)
            total_p = sum(r["metrics"]["overall"]["profit"] for r in summaries)
            total_s = sum(r["metrics"]["overall"]["staked"] for r in summaries)
            wr = total_w / (total_w + total_l) if (total_w + total_l) > 0 else 0
            roi = total_p / total_s if total_s > 0 else 0
            print(f"  Record:  {total_w}-{total_l} ({wr*100:.1f}%)")
            print(f"  Profit:  {total_p:+.2f}u  ROI: {roi*100:+.1f}%")
    else:
        target = args.date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        run(target)


if __name__ == "__main__":
    main()
