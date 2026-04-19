"""
scrapers/mlb_hitter_scraper.py
Fetches season hitting stats for players in today's confirmed lineups,
plus home/away and platoon (vs LHP / vs RHP) splits.

Also pulls pitcher opponent stats needed for props:
  - HR/9 allowed, H/9 allowed, WHIP, opponent BA
  - All from the MLB Stats API.

Output: data/raw/mlb_hitter_stats_YYYY-MM-DD.json
"""

import os, json, logging, time, requests
from datetime import datetime

log = logging.getLogger(__name__)

BASE     = "https://statsapi.mlb.com/api/v1"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

SEASON = datetime.now().year


def _get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params,
                             headers={"User-Agent": "mlb-betting-pipeline/1.0"},
                             timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def fetch_hitter_full(player_id: int) -> dict:
    """
    Returns season + home/away + platoon splits for one hitter.
    Gracefully returns partial data on failures.
    """
    result = {"player_id": player_id}

    # ── Season totals ─────────────────────────────────────────────────────────
    url    = f"{BASE}/people/{player_id}/stats"
    season_params = {
        "stats":    "season",
        "group":    "hitting",
        "gameType": "R",
        "season":   SEASON,
    }
    try:
        data   = _get(url, season_params)
        splits = data.get("stats", [{}])[0].get("splits", [])
        if splits:
            s = splits[0].get("stat", {})
            pa  = int(s.get("plateAppearances", 0) or 0)
            hr  = int(s.get("homeRuns",          0) or 0)
            h   = int(s.get("hits",              0) or 0)
            bb  = int(s.get("baseOnBalls",        0) or 0)
            so  = int(s.get("strikeOuts",         0) or 0)
            tb  = int(s.get("totalBases",   0) or 0)
            rbi = int(s.get("rbi",          0) or 0)
            runs= int(s.get("runs",         0) or 0)
            sb  = int(s.get("stolenBases",  0) or 0)
            result.update({
                "pa":    pa,  "hr": hr, "hits": h, "bb": bb,
                "so":    so,  "tb": tb, "rbi":  rbi,
                "runs":  runs, "sb": sb,
                "avg":   float(s.get("avg", "0") or 0),
                "ops":   float(s.get("ops", "0") or 0),
                "slg":   float(s.get("slg", "0") or 0),
                "obp":   float(s.get("obp", "0") or 0),
                "hr_per_pa":  round(hr   / pa, 4) if pa >= 20 else 0.035,
                "h_per_pa":   round(h    / pa, 4) if pa >= 20 else 0.270,
                "tb_per_pa":  round(tb   / pa, 4) if pa >= 20 else 0.400,
                "rbi_per_pa": round(rbi  / pa, 4) if pa >= 20 else 0.110,
                "r_per_pa":   round(runs / pa, 4) if pa >= 20 else 0.130,
                "sb_per_pa":  round(sb   / pa, 4) if pa >= 20 else 0.020,
                "k_rate":     round(so   / pa, 4) if pa >= 20 else 0.220,
            })
    except Exception as e:
        log.debug(f"Season stats failed for {player_id}: {e}")

    time.sleep(0.15)

    # ── Home / Away splits ────────────────────────────────────────────────────
    ha_params = {
        "stats":    "homeAndAway",
        "group":    "hitting",
        "gameType": "R",
        "season":   SEASON,
    }
    try:
        data   = _get(url, ha_params)
        splits = data.get("stats", [{}])[0].get("splits", [])
        for split in splits:
            label = split.get("isHome")
            s     = split.get("stat", {})
            pa    = int(s.get("plateAppearances", 0) or 0)
            hr    = int(s.get("homeRuns",          0) or 0)
            h     = int(s.get("hits",              0) or 0)
            if label is True:
                result["home_hr_per_pa"] = round(hr / pa, 4) if pa >= 10 else result.get("hr_per_pa", 0.035)
                result["home_h_per_pa"]  = round(h  / pa, 4) if pa >= 10 else result.get("h_per_pa",  0.270)
                result["home_avg"]       = float(s.get("avg", "0") or 0)
                result["home_ops"]       = float(s.get("ops", "0") or 0)
            elif label is False:
                result["away_hr_per_pa"] = round(hr / pa, 4) if pa >= 10 else result.get("hr_per_pa", 0.035)
                result["away_h_per_pa"]  = round(h  / pa, 4) if pa >= 10 else result.get("h_per_pa",  0.270)
                result["away_avg"]       = float(s.get("avg", "0") or 0)
                result["away_ops"]       = float(s.get("ops", "0") or 0)
    except Exception as e:
        log.debug(f"Home/away splits failed for {player_id}: {e}")

    time.sleep(0.15)

    # ── Platoon splits (vs LHP / vs RHP) ─────────────────────────────────────
    plat_params = {
        "stats":    "statSplits",
        "group":    "hitting",
        "gameType": "R",
        "season":   SEASON,
        "sitCodes": "vl,vr",
    }
    try:
        data   = _get(url, plat_params)
        splits = data.get("stats", [{}])[0].get("splits", [])
        for split in splits:
            sc = split.get("split", {}).get("code", "")
            s  = split.get("stat", {})
            pa = int(s.get("plateAppearances", 0) or 0)
            hr = int(s.get("homeRuns",          0) or 0)
            h  = int(s.get("hits",              0) or 0)
            if sc == "vl":   # vs LHP
                result["vs_lhp_avg"]      = float(s.get("avg", "0") or 0)
                result["vs_lhp_ops"]      = float(s.get("ops", "0") or 0)
                result["vs_lhp_hr_per_pa"]= round(hr / pa, 4) if pa >= 10 else result.get("hr_per_pa", 0.035)
                result["vs_lhp_h_per_pa"] = round(h  / pa, 4) if pa >= 10 else result.get("h_per_pa",  0.270)
            elif sc == "vr":  # vs RHP
                result["vs_rhp_avg"]      = float(s.get("avg", "0") or 0)
                result["vs_rhp_ops"]      = float(s.get("ops", "0") or 0)
                result["vs_rhp_hr_per_pa"]= round(hr / pa, 4) if pa >= 10 else result.get("hr_per_pa", 0.035)
                result["vs_rhp_h_per_pa"] = round(h  / pa, 4) if pa >= 10 else result.get("h_per_pa",  0.270)
    except Exception as e:
        log.debug(f"Platoon splits failed for {player_id}: {e}")

    return result


def fetch_pitcher_opponent_stats(pitcher_id: int) -> dict:
    """
    Pull what opposing batters hit against this pitcher this season.
    Returns: opp_avg, opp_slg, hr_per_9, h_per_9, k_per_9, bb_per_9
    """
    url    = f"{BASE}/people/{pitcher_id}/stats"
    params = {
        "stats":    "season",
        "group":    "pitching",
        "gameType": "R",
        "season":   SEASON,
    }
    result = {"pitcher_id": pitcher_id}
    try:
        data   = _get(url, params)
        splits = data.get("stats", [{}])[0].get("splits", [])
        if splits:
            s   = splits[0].get("stat", {})
            ip  = float(s.get("inningsPitched", "0") or 0)
            hr  = int(s.get("homeRuns",    0) or 0)
            h   = int(s.get("hits",        0) or 0)
            k   = int(s.get("strikeOuts",  0) or 0)
            bb  = int(s.get("baseOnBalls", 0) or 0)
            result.update({
                "opp_avg":    float(s.get("avg",  "0") or 0),
                "opp_obp":    float(s.get("obp",  "0") or 0),
                "opp_slg":    float(s.get("slg",  "0") or 0),
                "era":        float(s.get("era",  "0").replace("-.--","0") or 0),
                "whip":       float(s.get("whip", "0").replace("-.--","0") or 0),
                "hr_per_9":   round((hr / ip) * 9, 3) if ip > 0 else 1.0,
                "h_per_9":    round((h  / ip) * 9, 3) if ip > 0 else 8.5,
                "k_per_9":    float(s.get("strikeoutsPer9Inn", "0") or 0),
                "bb_per_9":   float(s.get("walksPer9Inn",      "0") or 0),
                "pitcher_ip": ip,
            })
    except Exception as e:
        log.debug(f"Pitcher opp stats failed for {pitcher_id}: {e}")
    return result


def run(target_date: str = None) -> dict:
    """
    Load today's lineups, fetch full hitter stats for confirmed players,
    and pitcher opponent stats for today's starters.
    Returns {"hitters": [...], "pitcher_opp": [...]}
    """
    today    = target_date or datetime.now().strftime("%Y-%m-%d")
    raw_path = os.path.join(DATA_DIR, f"mlb_lineups_{today}.json")

    if not os.path.exists(raw_path):
        log.warning(f"No lineup file found for {today} — run mlb_lineup_scraper first")
        return {"hitters": [], "pitcher_opp": []}

    with open(raw_path, encoding="utf-8") as f:
        lineups = json.load(f)

    # Collect all unique player IDs from confirmed lineups
    hitter_ids = set()
    for game in lineups:
        if game.get("lineup_confirmed"):
            for p in game["away_lineup"] + game["home_lineup"]:
                pid = p.get("player_id")
                if pid:
                    hitter_ids.add(pid)

    log.info(f"Fetching full hitter stats for {len(hitter_ids)} players")
    hitter_stats = {}
    for pid in hitter_ids:
        hitter_stats[pid] = fetch_hitter_full(pid)
        time.sleep(0.1)

    # Merge stats back into lineup structure
    enriched = []
    for game in lineups:
        g = dict(game)
        for side in ("away_lineup", "home_lineup"):
            enriched_lineup = []
            for p in g.get(side, []):
                pid   = p.get("player_id")
                stats = hitter_stats.get(pid, {}) if pid else {}
                enriched_lineup.append({**p, **stats})
            g[side] = enriched_lineup
        enriched.append(g)

    # Get pitcher IDs from schedule master for today
    import csv as _csv
    sched_path = os.path.join(os.path.dirname(__file__), "..", "data", "clean",
                              "mlb_schedule_master.csv")
    pitcher_ids = set()
    if os.path.exists(sched_path):
        with open(sched_path, encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                if row.get("game_date") == today:
                    for col in ("away_probable_pitcher_id", "home_probable_pitcher_id"):
                        pid = row.get(col, "").strip()
                        if pid and pid.isdigit():
                            pitcher_ids.add(int(pid))

    log.info(f"Fetching pitcher opponent stats for {len(pitcher_ids)} pitchers")
    pitcher_opp = []
    for pid in pitcher_ids:
        stats = fetch_pitcher_opponent_stats(pid)
        pitcher_opp.append(stats)
        time.sleep(0.15)

    output = {"hitters": enriched, "pitcher_opp": pitcher_opp}
    out_path = os.path.join(DATA_DIR, f"mlb_hitter_stats_{today}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    log.info(f"Hitter stats saved → {out_path}")
    return output


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = run()
    print(f"Hitters: {len(result['hitters'])} games")
    print(f"Pitcher opp stats: {len(result['pitcher_opp'])} pitchers")
