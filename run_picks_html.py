"""
run_picks_html.py
Generates a self-contained HTML dashboard for today's MLB betting picks.

Usage:
    python run_picks_html.py                  # Today's picks
    python run_picks_html.py --date 2026-04-15
    python run_picks_html.py --no-open        # Don't auto-open browser
"""

import sys, os, json, logging, argparse, webbrowser, requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

PICKS_DIR = os.path.join(os.path.dirname(__file__), "picks")


# ─────────────────────────────────────────────────────────────────────────────
# DATA PREP
# ─────────────────────────────────────────────────────────────────────────────
def fetch_live_scores(date: str) -> list:
    """
    Pull today's completed and in-progress scores directly from the MLB Stats API.
    Returns fresh data regardless of when the pipeline last ran.
    """
    url = "https://statsapi.mlb.com/api/v1/schedule"
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
        log.warning(f"Live scores fetch failed: {e}")
        return []

    results = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            status     = game.get("status", {})
            abstract   = status.get("abstractGameState", "")
            detailed   = status.get("detailedState", "")

            # Only include Final or In Progress
            if abstract not in ("Final", "Live"):
                continue

            away       = game["teams"]["away"]
            home       = game["teams"]["home"]
            linescore  = game.get("linescore", {})
            inning     = linescore.get("currentInning", 9)
            inning_half= linescore.get("inningHalf", "")

            away_score = away.get("score", 0) or 0
            home_score = home.get("score", 0) or 0

            results.append({
                "away_team":   away["team"]["name"],
                "home_team":   home["team"]["name"],
                "away_score":  away_score,
                "home_score":  home_score,
                "status":      abstract,
                "detailed":    detailed,
                "inning":      inning,
                "inning_half": inning_half,
            })

    log.info(f"Live scores fetched: {len(results)} games")
    return results


def prep_scores_ticker(scores: list) -> list:
    """Prepare live/completed game scores for the ticker."""
    def city(name):
        parts = name.split()
        return parts[0] if parts else name

    out = []
    for s in scores:
        away   = s.get("away_team", "")
        home   = s.get("home_team", "")
        ascore = s.get("away_score", 0)
        hscore = s.get("home_score", 0)
        if not away or not home:
            continue

        is_live   = s.get("status") == "Live"
        inning    = s.get("inning", 9)
        inh       = s.get("inning_half", "")
        label     = f"{inh[:3]} {inning}" if is_live else "Final"
        if inning and int(inning) > 9 and not is_live:
            label = f"F/{inning}"

        out.append({
            "away":       away,
            "home":       home,
            "away_city":  city(away),
            "home_city":  city(home),
            "away_score": str(ascore),
            "home_score": str(hscore),
            "is_live":    is_live,
            "label":      label,
        })
    return out


def prep_picks(picks, kalshi_data: dict = None):
    """
    Serialize picks for HTML embedding.
    Adds warning flags and Kalshi consensus signals when available.

    Warning flags:
      tbd_sp        — either starting pitcher is TBD
      thin_edge     — TOTAL pick with <0.8 run edge
      heavy_fav     — ML pick where model conf implies >67% (~-200 or worse)
      unconfirmed   — lineup not confirmed at time of HTML generation

    Kalshi fields:
      kalshi_prob   — Kalshi implied win probability for picked team (0-100)
      kalshi_signal — AGREE / DISAGREE / NEUTRAL
    """
    import re as _re
    out = []
    for p in picks:
        gd      = p.get("game_data", {})
        ptype   = p["type"]
        conf    = round(p["conf"] * 100, 1)
        reasoning = p["reasoning"]

        # ── Warning flags ─────────────────────────────────────────────────
        away_sp = gd.get("away_sp", "TBD") or "TBD"
        home_sp = gd.get("home_sp", "TBD") or "TBD"
        tbd_sp  = (away_sp.strip().upper() == "TBD" or
                   home_sp.strip().upper() == "TBD")

        # Thin edge: TOTAL picks where run edge < 0.8
        thin_edge = False
        if ptype == "TOTAL":
            m = _re.search(r'\(([\d.]+)\s*run\s*edge\)', reasoning, _re.IGNORECASE)
            if m:
                edge = float(m.group(1))
                thin_edge = edge < 0.8

        # Heavy favorite: ML conf implies worse than -200 odds (>67% implied)
        heavy_fav = (ptype == "ML" and p["conf"] >= 0.70)

        # Favorite tier — applies to every pick
        #   HEAVY  : conf >= 68%  (equiv ~-210 or worse ML line)
        #   MEDIUM : conf 58-68%  (equiv ~-138 to -210)
        #   NEUTRAL: conf < 58%   (near pick'em territory)
        if p["conf"] >= 0.68:
            fav_tier = "HEAVY"
        elif p["conf"] >= 0.58:
            fav_tier = "MEDIUM"
        else:
            fav_tier = "NEUTRAL"

        # Unconfirmed lineup
        unconfirmed = not bool(gd.get("lineup_confirmed", False))

        # ── Kalshi signal ─────────────────────────────────────────────────
        kalshi_prob   = None
        kalshi_signal = None
        if kalshi_data:
            away_team = gd.get("away_team", "")
            home_team = gd.get("home_team", "")
            key = tuple(sorted([away_team, home_team]))
            kd  = kalshi_data.get(key)
            if kd:
                # Determine Kalshi prob for the PICKED team
                if ptype in ("ML", "RL"):
                    team_picked = p["team"].lower()
                    if team_picked in kd["away_team"].lower() or \
                       kd["away_team"].lower() in team_picked:
                        kp = kd["kalshi_away_prob"]
                    else:
                        kp = kd["kalshi_home_prob"]
                elif ptype == "TOTAL":
                    # For totals: if model says OVER and Kalshi total_prob > 0.5 → AGREE
                    # Use home team win prob as a proxy (imperfect but informative)
                    kp = kd["kalshi_home_prob"]  # placeholder
                else:
                    kp = None

                if kp is not None:
                    kalshi_prob = round(kp * 100, 1)
                    # Signal
                    model_wp  = p["conf"]
                    diff      = abs(model_wp - kp)
                    same_side = (model_wp > 0.50) == (kp > 0.50)
                    if diff <= 0.04:
                        kalshi_signal = "NEUTRAL"
                    elif same_side:
                        kalshi_signal = "AGREE"
                    else:
                        kalshi_signal = "DISAGREE"

        out.append({
            "type":           ptype,
            "label":          p["label"],
            "team":           p["team"],
            "conf":           conf,
            "tier":           p["tier"],
            "game":           p["game"],
            "game_id":        p["game_id"],
            "venue":          p["venue"],
            "reasoning":      reasoning,
            "exp_total":      p["exp_total"],
            "away":           gd.get("away_team", ""),
            "home":           gd.get("home_team", ""),
            # Warning flags
            "tbd_sp":         tbd_sp,
            "thin_edge":      thin_edge,
            "heavy_fav":      heavy_fav,
            "unconfirmed":    unconfirmed,
            "fav_tier":       fav_tier,
            # Kalshi
            "kalshi_prob":    kalshi_prob,
            "kalshi_signal":  kalshi_signal,
        })
    return out


def prep_games(scored):
    from datetime import timezone as _tz, timedelta as _td
    _EDT = _tz(_td(hours=-4))   # April–Oct = EDT (UTC-4)
    out = []
    for g in scored:
        t = g.get("game_time_utc", "")
        time_str = ""
        if t:
            try:
                from datetime import datetime as _dt
                utc_dt = _dt.fromisoformat(t.replace("Z", "+00:00"))
                et_dt  = utc_dt.astimezone(_EDT)
                hour   = et_dt.hour % 12 or 12
                ampm   = "AM" if et_dt.hour < 12 else "PM"
                time_str = f"{hour}:{et_dt.minute:02d} {ampm} ET"
            except Exception:
                time_str = t[11:16] + " ET"
        pf = g["park_runs"]
        if pf >= 112:   park_tag = "Extreme Hitter Park"
        elif pf >= 106: park_tag = "Hitter-Friendly"
        elif pf <= 96:  park_tag = "Pitcher-Friendly"
        else:           park_tag = "Neutral"

        def era_str(v): return f"{float(v):.2f}" if v else "N/A"

        out.append({
            "game_id":   g.get("game_id", ""),
            "away":      g["away_team"],
            "home":      g["home_team"],
            "venue":     g["venue"],
            "time":      time_str,
            "park_runs": pf,
            "park_tag":  park_tag,
            "away_sp":   g.get("away_sp", "TBD"),
            "home_sp":   g.get("home_sp", "TBD"),
            "away_era":  era_str(g.get("away_sp_era_adj")),
            "home_era":  era_str(g.get("home_sp_era_adj")),
            "away_fip":  era_str(g.get("away_sp_fip")),
            "home_fip":  era_str(g.get("home_sp_fip")),
            "away_rpg":  round(g.get("away_rpg", 4.5), 2),
            "home_rpg":  round(g.get("home_rpg", 4.5), 2),
            "away_ops":  g.get("away_ops") or "N/A",
            "home_ops":  g.get("home_ops") or "N/A",
            "away_form": round(g.get("away_form_rpg", 4.5), 1),
            "home_form": round(g.get("home_form_rpg", 4.5), 1),
            "exp_away":  g["exp_away"],
            "exp_home":  g["exp_home"],
            "exp_total": g["exp_total"],
            "away_wp":   round(g["away_wp"] * 100, 1),
            "home_wp":   round(g["home_wp"] * 100, 1),
            "total_pick":g["total_pick"],
            "total_line":g["total_line"],
            "total_conf":round(g["total_conf"] * 100, 1),
            "rl_pick":   g.get("rl_pick", ""),
            "rl_team":   g.get("rl_team", ""),
            "rl_conf":   round(g.get("rl_conf", 0) * 100, 1),
            # Weather
            "weather_flag":   g.get("weather_flag", "NORMAL"),
            "wind_label":     g.get("wind_label", ""),
            "wind_speed":     round(g.get("wind_speed", 0) or 0, 1),
            "wind_component": round(g.get("wind_component", 0) or 0, 1),
            "temp_f":         round(g.get("temp_f", 70) or 70, 0),
            "precip_prob":    round(g.get("precip_prob", 0) or 0, 0),
            "has_roof":       bool(g.get("has_roof")),
            # Bullpen
            "away_bp_era":    round(g.get("away_bp_era", 4.20) or 4.20, 2),
            "home_bp_era":    round(g.get("home_bp_era", 4.20) or 4.20, 2),
            "lineup_confirmed": bool(g.get("lineup_confirmed")),
            "away_lineup_ops":  g.get("away_lineup_ops"),
            "home_lineup_ops":  g.get("home_lineup_ops"),
        })
    return out


def load_standings() -> dict:
    """Load latest W-L record for every team from the standings master."""
    path = os.path.join(os.path.dirname(__file__), "data", "clean",
                        "mlb_standings_master.csv")
    records: dict[str, dict] = {}
    if not os.path.exists(path):
        return records
    import csv as _csv
    with open(path, encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            team = row.get("team", "").strip()
            if team:
                existing = records.get(team)
                if not existing or row.get("game_date", "") >= existing.get("game_date", ""):
                    records[team] = row
    return records


def prep_schedule_view(all_games: list, live_scores: list, standings: dict) -> list:
    """
    Build a full today's-games list combining schedule data,
    live API status, and team records.

    all_games  — all scored game dicts for the date (including already-started)
    live_scores — from fetch_live_scores() with status/score/inning
    standings  — team_name -> standings row
    """
    # Index live scores by (away, home) for quick lookup
    live_idx: dict[tuple, dict] = {}
    for s in live_scores:
        key = (s.get("away_team", ""), s.get("home_team", ""))
        live_idx[key] = s

    def record_str(team: str) -> str:
        row = standings.get(team, {})
        if not row:
            # Partial name match
            tl = team.lower()
            for k, v in standings.items():
                if k.lower() in tl or tl in k.lower():
                    row = v
                    break
        if row:
            w = row.get("wins", "?")
            l = row.get("losses", "?")
            streak = row.get("streak", "")
            last10 = row.get("last_10", "")
            return {"record": f"{w}-{l}", "streak": streak, "last10": last10}
        return {"record": "—", "streak": "", "last10": ""}

    out = []
    for g in all_games:
        away = g.get("away_team", "")
        home = g.get("home_team", "")
        key  = (away, home)
        live = live_idx.get(key, {})

        # Status
        if live.get("status") == "Final":
            status = "Final"
        elif live.get("status") == "Live":
            inh   = live.get("inning_half", "")[:3]
            inn   = live.get("inning", "")
            status = f"Live — {inh} {inn}"
        else:
            status = "Upcoming"

        away_rec = record_str(away)
        home_rec = record_str(home)

        out.append({
            "game_id":      g.get("game_id", ""),
            "away_team":    away,
            "home_team":    home,
            "away_record":  away_rec["record"],
            "away_streak":  away_rec["streak"],
            "away_last10":  away_rec["last10"],
            "home_record":  home_rec["record"],
            "home_streak":  home_rec["streak"],
            "home_last10":  home_rec["last10"],
            "venue":        g.get("venue", ""),
            "game_time_utc": g.get("game_time_utc", ""),
            "status":       status,
            "away_score":   live.get("away_score", ""),
            "home_score":   live.get("home_score", ""),
            "away_sp":      g.get("away_sp", "TBD"),
            "home_sp":      g.get("home_sp", "TBD"),
        })

    # Sort by game time
    out.sort(key=lambda x: x.get("game_time_utc", ""))
    return out


def prep_team_schedule(today: str) -> dict:
    """
    Build a team → next_game lookup covering today + next 7 days.
    Used by the Teams tab to always show each team's upcoming game
    even when they don't play today.
    Returns dict keyed by team name with game details.
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    import csv as _csv

    _EDT = _tz(_td(hours=-4))
    today_dt  = _dt.fromisoformat(today)
    cutoff_dt = today_dt + _td(days=8)

    sched_path = os.path.join(os.path.dirname(__file__), "data", "clean",
                              "mlb_schedule_master.csv")
    if not os.path.exists(sched_path):
        return {}

    # Collect all upcoming games in the window, sorted by date
    games = []
    with open(sched_path, encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            gdate = row.get("game_date", "")
            if not gdate:
                continue
            try:
                gdt = _dt.fromisoformat(gdate)
            except ValueError:
                continue
            if today_dt <= gdt < cutoff_dt:
                # Convert UTC time to ET for display
                utc_str = row.get("game_time_utc", "")
                time_str = ""
                day_label = ""
                try:
                    utc_dt = _dt.fromisoformat(utc_str.replace("Z", "+00:00"))
                    et_dt  = utc_dt.astimezone(_EDT)
                    hr     = et_dt.hour % 12 or 12
                    ampm   = "AM" if et_dt.hour < 12 else "PM"
                    time_str = f"{hr}:{et_dt.minute:02d} {ampm} ET"
                    delta = (gdt.date() - today_dt.date()).days
                    if delta == 0:
                        day_label = "Today"
                    elif delta == 1:
                        day_label = "Tomorrow"
                    else:
                        day_label = et_dt.strftime("%A, %b %-d")
                except Exception:
                    delta = (gdt.date() - today_dt.date()).days
                    day_label = "Today" if delta == 0 else ("Tomorrow" if delta == 1 else gdate)

                games.append({
                    "game_date":  gdate,
                    "game_id":    row.get("game_id", ""),
                    "away_team":  row.get("away_team", ""),
                    "home_team":  row.get("home_team", ""),
                    "away_sp":    row.get("away_probable_pitcher", "TBD") or "TBD",
                    "home_sp":    row.get("home_probable_pitcher", "TBD") or "TBD",
                    "venue":      row.get("venue", ""),
                    "time_str":   time_str,
                    "day_label":  day_label,
                    "is_today":   gdate == today,
                })

    # Sort by date then time
    games.sort(key=lambda x: x["game_date"])

    # Build team → first upcoming game lookup
    team_next: dict[str, dict] = {}
    for g in games:
        for team in (g["away_team"], g["home_team"]):
            if team and team not in team_next:
                team_next[team] = g

    return team_next


def prep_props(props: list) -> list:
    """Serialize player props for HTML embedding."""
    out = []
    for p in props:
        conf_raw = p["confidence"]
        ptype    = p["prop_type"]

        # Lineup/starter flags (carried from score_all_props if present)
        unconfirmed = p.get("lineup_unconfirmed", False)
        tbd_sp      = p.get("tbd_sp", False)

        # Edge strength — shown as fav-tier equivalent
        # Each prop type has calibrated thresholds matching the model
        if ptype == "HR":
            if conf_raw >= 0.20:   fav_tier = "HEAVY"
            elif conf_raw >= 0.15: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        elif ptype == "SB":
            if conf_raw >= 0.28:   fav_tier = "HEAVY"
            elif conf_raw >= 0.20: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        elif ptype in ("RBI", "R"):
            if conf_raw >= 0.50:   fav_tier = "HEAVY"
            elif conf_raw >= 0.42: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        elif ptype == "TB":
            if conf_raw >= 0.62:   fav_tier = "HEAVY"
            elif conf_raw >= 0.55: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        else:   # HITS, K — standard scale
            if conf_raw >= 0.68:   fav_tier = "HEAVY"
            elif conf_raw >= 0.58: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"

        out.append({
            "prop_type":    ptype,
            "player_name":  p["player_name"],
            "line":         p["line"],
            "proj":         p["proj"],
            "conf":         round(conf_raw * 100, 1),
            "tier":         p["tier"],
            "game":         p.get("game", ""),
            "game_id":      p.get("game_id", ""),
            "away_team":    p.get("away_team", ""),
            "home_team":    p.get("home_team", ""),
            "side":         p.get("side", ""),
            "batting_order":p.get("batting_order", ""),
            "reasoning":    p.get("reasoning", ""),
            "fav_tier":     fav_tier,
            "unconfirmed":  unconfirmed,
            "tbd_sp":       tbd_sp,
            "projected":    p.get("projected", False),
        })
    return out


def prep_parlays(parlays):
    out = []
    for p in parlays:
        out.append({
            "n_legs":   p["n_legs"],
            "combined": round(p["combined"] * 100, 1),
            "payout":   p["payout"],
            "legs": [
                {"label": l["label"], "conf": round(l["conf"] * 100, 1),
                 "tier": l["tier"], "game": l["game"]}
                for l in p["legs"]
            ],
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# KALSHI + ANALYSIS LOADERS
# ─────────────────────────────────────────────────────────────────────────────

def load_kalshi(date: str) -> dict:
    """
    Load Kalshi market data for date. Returns dict keyed by sorted team tuple.
    Returns empty dict if Kalshi scraper hasn't been configured/run yet.
    """
    try:
        from scrapers.mlb_kalshi_scraper import load_kalshi_for_date
        data = load_kalshi_for_date(date)
        if data:
            log.info(f"Kalshi data loaded: {len(data)} markets")
        return data
    except Exception:
        return {}


def load_line_movement(date: str) -> list:
    """
    Load line movement for date and merge in Kalshi movement.
    Returns list of movement dicts — includes games where EITHER sportsbooks
    OR Kalshi show meaningful movement (STEAM / DRIFT).
    """
    import csv as _csv
    clean_dir = os.path.join(os.path.dirname(__file__), "data", "clean")

    def _n(v):
        try: return float(v)
        except: return None

    # ── Sportsbook movement ──────────────────────────────────────────────────
    sb_map = {}
    sb_path = os.path.join(clean_dir, f"mlb_line_movement_{date}.csv")
    if os.path.exists(sb_path):
        try:
            with open(sb_path, encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
            latest = {}
            for r in rows:
                k = (r.get("away_team",""), r.get("home_team",""))
                if k not in latest or r.get("snap2_time","") > latest[k].get("snap2_time",""):
                    latest[k] = r
            for k, r in latest.items():
                sb_map[k] = {
                    "away":          r.get("away_team",""),
                    "home":          r.get("home_team",""),
                    "ml_away_open":  _n(r.get("ml_away_open")),
                    "ml_away_now":   _n(r.get("ml_away_now")),
                    "ml_away_move":  _n(r.get("ml_away_move")),
                    "ml_home_open":  _n(r.get("ml_home_open")),
                    "ml_home_now":   _n(r.get("ml_home_now")),
                    "ml_home_move":  _n(r.get("ml_home_move")),
                    "total_open":    _n(r.get("total_open")),
                    "total_now":     _n(r.get("total_now")),
                    "total_move":    _n(r.get("total_move")),
                    "ml_signal":     r.get("ml_signal", "NO_DATA"),
                    "total_signal":  r.get("total_signal", "NO_DATA"),
                    "sharp_side":    r.get("sharp_side",""),
                    "snap1_time":    r.get("snap1_time",""),
                    "snap2_time":    r.get("snap2_time",""),
                    # Kalshi fields default to None until merged
                    "kalshi_away_open":  None,
                    "kalshi_away_now":   None,
                    "kalshi_away_move":  None,
                    "kalshi_home_open":  None,
                    "kalshi_home_now":   None,
                    "kalshi_home_move":  None,
                    "kalshi_signal":     "NO_DATA",
                    "kalshi_sharp_side": "",
                }
        except Exception as e:
            log.warning(f"Sportsbook movement load failed: {e}")

    # ── Kalshi movement ──────────────────────────────────────────────────────
    try:
        from scrapers.mlb_kalshi_scraper import load_kalshi_movement as _load_km
        km_rows = _load_km(date)
        for r in km_rows:
            k = (r.get("away_team",""), r.get("home_team",""))
            kalshi_fields = {
                "kalshi_away_open":  _n(r.get("kalshi_away_open")),
                "kalshi_away_now":   _n(r.get("kalshi_away_now")),
                "kalshi_away_move":  _n(r.get("kalshi_away_move")),
                "kalshi_home_open":  _n(r.get("kalshi_home_open")),
                "kalshi_home_now":   _n(r.get("kalshi_home_now")),
                "kalshi_home_move":  _n(r.get("kalshi_home_move")),
                "kalshi_signal":     r.get("kalshi_signal", "STABLE"),
                "kalshi_sharp_side": r.get("kalshi_sharp_side", ""),
            }
            if k in sb_map:
                sb_map[k].update(kalshi_fields)
            else:
                # Kalshi moved but sportsbooks didn't — still worth surfacing
                sb_map[k] = {
                    "away":  r.get("away_team",""),
                    "home":  r.get("home_team",""),
                    "ml_away_open": None, "ml_away_now": None, "ml_away_move": None,
                    "ml_home_open": None, "ml_home_now": None, "ml_home_move": None,
                    "total_open": None,   "total_now":   None, "total_move":   None,
                    "ml_signal":    "NO_DATA",
                    "total_signal": "NO_DATA",
                    "sharp_side":   "",
                    "snap1_time":   r.get("snap1_time",""),
                    "snap2_time":   r.get("snap2_time",""),
                    **kalshi_fields,
                }
    except Exception as e:
        log.warning(f"Kalshi movement merge failed: {e}")

    # ── Filter to games with at least one meaningful signal ──────────────────
    out = []
    for r in sb_map.values():
        sb_notable = (r.get("ml_signal") in ("STEAM","DRIFT") or
                      r.get("total_signal") in ("STEAM","DRIFT"))
        k_notable  = r.get("kalshi_signal") in ("STEAM","DRIFT")
        if sb_notable or k_notable:
            out.append(r)

    log.info(f"Line movement loaded: {len(out)} games with notable moves (sportsbook + Kalshi)")
    return out


def load_yesterday_analysis(date: str) -> dict:
    """
    Load the most recent analysis JSON (for yesterday's picks) to show in
    the 'Yesterday' panel.  Returns empty dict if not yet generated.
    """
    import glob as _glob
    from datetime import datetime as _dt, timedelta as _td

    # Try yesterday first, then walk back up to 3 days
    for days_back in range(1, 4):
        check = (_dt.strptime(date, "%Y-%m-%d") - _td(days=days_back)).strftime("%Y-%m-%d")
        path = os.path.join(PICKS_DIR, f"mlb_analysis_{check}.json")
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                log.info(f"Yesterday's analysis loaded: {path}")
                return data
            except Exception:
                pass
    return {}


PROJ_LINEUPS_CACHE = os.path.join(os.path.dirname(__file__), "data", "clean", "mlb_projected_lineups.json")


def save_projected_lineups_cache(lineups: dict) -> None:
    """
    Persist projected lineups to data/clean/ so they survive git pushes to Railway.
    Called by run_pipeline.py after building lineups from raw hitter stats files.
    """
    try:
        os.makedirs(os.path.dirname(PROJ_LINEUPS_CACHE), exist_ok=True)
        with open(PROJ_LINEUPS_CACHE, "w", encoding="utf-8") as f:
            json.dump(lineups, f)
        log.info(f"Projected lineups cache saved: {len(lineups)} teams → {PROJ_LINEUPS_CACHE}")
    except Exception as e:
        log.warning(f"Could not save projected lineups cache: {e}")


def load_projected_lineups(today: str) -> dict:
    """
    Return the most recent confirmed batting order per team from previous days.
    Used to pre-populate the Teams tab before today's lineups are officially posted.

    Returns dict keyed by team full name:
      { "Boston Red Sox": {"players": [{"name": "...", "order": 1, "pos": "CF"}, ...], "date": "2026-04-22"}, ... }
    """
    import glob as _glob

    raw_dir = os.path.join(os.path.dirname(__file__), "data", "raw")
    files = sorted(
        _glob.glob(os.path.join(raw_dir, "mlb_hitter_stats_*.json")),
        reverse=True  # most recent first
    )

    team_lineups: dict = {}
    for filepath in files:
        date_str = os.path.basename(filepath).replace("mlb_hitter_stats_", "").replace(".json", "")
        if date_str >= today:
            continue  # skip today or future files
        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for game in data.get("hitters", []):
                if not game.get("lineup_confirmed"):
                    continue
                for team, key in [
                    (game.get("away_team", ""), "away_lineup"),
                    (game.get("home_team", ""), "home_lineup"),
                ]:
                    if not team or team in team_lineups:
                        continue
                    players = sorted(game.get(key, []), key=lambda p: p.get("batting_order", 99))
                    if players:
                        team_lineups[team] = {
                            # Full player dicts so the props model can score them
                            "players": [
                                {
                                    **p,                               # all stats
                                    "name": p.get("player_name", ""), # JS display alias
                                    "pos":  p.get("position", ""),    # JS display alias
                                }
                                for p in players
                            ],
                            "date": date_str,
                        }
        except Exception:
            continue
        if len(team_lineups) >= 30:
            break

    if team_lineups:
        log.info(f"Projected lineups loaded: {len(team_lineups)} teams from raw hitter stats")
        # Auto-save to data/clean/ so Railway gets them on next git push
        save_projected_lineups_cache(team_lineups)
    else:
        # No raw files found — try the pre-saved cache (present on Railway)
        if os.path.exists(PROJ_LINEUPS_CACHE):
            try:
                with open(PROJ_LINEUPS_CACHE, encoding="utf-8") as f:
                    team_lineups = json.load(f)
                log.info(f"Projected lineups loaded from cache: {len(team_lineups)} teams")
            except Exception as e:
                log.warning(f"Projected lineups cache load failed: {e}")
        else:
            log.info("No projected lineups available (no raw files or cache)")
    return team_lineups


# ─────────────────────────────────────────────────────────────────────────────
# HTML TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<meta http-equiv="cache-control" content="no-cache, no-store, must-revalidate"/>
<meta http-equiv="pragma" content="no-cache"/>
<meta http-equiv="expires" content="0"/>
<title>Sports Betting Parlay Genius</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
:root {
  --bg:        #07090f;
  --bg2:       #0d1117;
  --card:      #111827;
  --card2:     #161f2e;
  --border:    #1e2d44;
  --green:     #00e676;
  --gold:      #ffc107;
  --blue:      #42a5f5;
  --purple:    #ab47bc;
  --red:       #ef5350;
  --text:      #e2e8f0;
  --sub:       #7a8899;
  --lock-c:    #ffc107;
  --strong-c:  #42a5f5;
  --lean-c:    #00e676;
  --radius:    12px;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;padding-bottom:60px}

/* ── HEADER ── */
.header{
  background:linear-gradient(135deg,#0d1b2e 0%,#0a1628 50%,#0d1b2e 100%);
  border-bottom:1px solid var(--border);
  padding:28px 24px 20px;
  text-align:center;
  position:relative;
  overflow:hidden;
}
.header::before{
  content:'';position:absolute;inset:0;
  background:radial-gradient(ellipse at 50% 0%,rgba(0,230,118,.08) 0%,transparent 70%);
  pointer-events:none;
}
.header h1{
  font-size:clamp(1.6rem,4vw,2.6rem);font-weight:800;letter-spacing:-0.5px;
  background:linear-gradient(90deg,#00e676,#42a5f5,#ffc107);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
}
.header-sub{color:var(--sub);margin-top:6px;font-size:.9rem;font-weight:500}
.header-stats{
  display:flex;justify-content:center;gap:28px;margin-top:16px;flex-wrap:wrap;align-items:center;
}
.stat-pill{
  background:rgba(255,255,255,.05);border:1px solid var(--border);
  border-radius:20px;padding:5px 14px;font-size:.82rem;font-weight:600;color:var(--text);
}
.stat-pill span{color:var(--green)}
/* ── REFRESH BUTTON ── */
#refreshBtn{
  display:none;align-items:center;gap:8px;
  background:linear-gradient(135deg,#00e676,#00b248);
  border:none;border-radius:24px;padding:9px 22px;
  font-size:.88rem;font-weight:700;color:#000;cursor:pointer;
  font-family:'Inter',sans-serif;transition:opacity .2s,transform .15s;
  box-shadow:0 0 20px rgba(0,230,118,.3);
}
#refreshBtn:hover{opacity:.88;transform:scale(1.03)}
#refreshBtn:disabled{opacity:.5;cursor:not-allowed;transform:none}
#refreshBtn svg{width:15px;height:15px;flex-shrink:0}
.spin{animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
#refreshStatus{
  font-size:.75rem;color:var(--sub);margin-top:6px;
  min-height:16px;text-align:center;
}

/* ── FILTERS ── */
.filters{
  position:sticky;top:0;z-index:100;
  background:rgba(7,9,15,.92);backdrop-filter:blur(12px);
  border-bottom:1px solid var(--border);
  padding:12px 24px;display:flex;gap:16px;flex-wrap:wrap;align-items:center;
}
.filter-group{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.filter-label{color:var(--sub);font-size:.78rem;font-weight:600;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}
.filter-btn{
  background:transparent;border:1px solid var(--border);color:var(--sub);
  padding:5px 13px;border-radius:20px;font-size:.8rem;font-weight:600;cursor:pointer;
  transition:all .2s;font-family:inherit;
}
.filter-btn:hover{border-color:var(--green);color:var(--text)}
.filter-btn.active{background:var(--green);border-color:var(--green);color:#000}
.filter-btn.active-gold{background:var(--gold);border-color:var(--gold);color:#000}
.filter-btn.active-blue{background:var(--blue);border-color:var(--blue);color:#000}
.search-wrap{margin-left:auto}
.search-input{
  background:var(--card);border:1px solid var(--border);color:var(--text);
  padding:6px 14px;border-radius:20px;font-size:.82rem;font-family:inherit;
  outline:none;width:180px;transition:border-color .2s;
}
.search-input:focus{border-color:var(--green)}
.search-input::placeholder{color:var(--sub)}

/* ── MAIN LAYOUT ── */
.main{max-width:1400px;margin:0 auto;padding:28px 20px}
.section-title{
  font-size:1rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;
  color:var(--sub);margin-bottom:16px;display:flex;align-items:center;gap:8px;
}
.section-title::after{content:'';flex:1;height:1px;background:var(--border)}

/* ── WARNING FLAGS ── */
.warn-row{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:8px}
.warn-badge{
  font-size:.62rem;font-weight:700;padding:2px 7px;border-radius:4px;
  letter-spacing:.4px;text-transform:uppercase;transition:opacity .15s;
}
/* Active (condition triggered) */
.warn-tbd.active    {background:rgba(255,152,0,.18);color:#ffb74d;border:1px solid rgba(255,152,0,.45)}
.warn-thin.active   {background:rgba(239,83,80,.15);color:#ef9a9a;border:1px solid rgba(239,83,80,.4)}
.warn-heavy.active  {background:rgba(171,71,188,.18);color:#ce93d8;border:1px solid rgba(171,71,188,.45)}
.warn-lineup.active {background:rgba(66,165,245,.12);color:#90caf9;border:1px solid rgba(66,165,245,.3)}
/* Inactive (condition not triggered — always shown, just dim) */
.warn-badge.inactive{background:rgba(255,255,255,.03);color:rgba(255,255,255,.2);border:1px solid rgba(255,255,255,.08);opacity:.55}

/* ── FAVORITE TIER ── */
.fav-row{display:flex;gap:6px;align-items:center;margin-bottom:8px}
.fav-label{font-size:.6rem;font-weight:600;color:rgba(255,255,255,.3);text-transform:uppercase;letter-spacing:.5px;margin-right:2px}
.fav-badge{
  font-size:.62rem;font-weight:700;padding:2px 8px;border-radius:4px;
  letter-spacing:.4px;text-transform:uppercase;
}
.fav-HEAVY  {background:rgba(239,83,80,.15);color:#ef9a9a;border:1px solid rgba(239,83,80,.4)}
.fav-MEDIUM {background:rgba(255,152,0,.13);color:#ffb74d;border:1px solid rgba(255,152,0,.35)}
.fav-NEUTRAL{background:rgba(0,230,118,.1);color:#69f0ae;border:1px solid rgba(0,230,118,.3)}

/* ── LINE MOVEMENT ── */
.move-row{
  display:flex;align-items:center;gap:8px;flex-wrap:wrap;
  margin-top:8px;padding:6px 10px;border-radius:6px;
  background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);
}
.move-icon{font-size:.9rem}
.move-label{font-size:.72rem;font-weight:700;color:var(--sub);text-transform:uppercase;letter-spacing:.4px}
.move-detail{font-size:.75rem;color:var(--text);flex:1}
.move-badge-steam{font-size:.65rem;font-weight:800;padding:2px 7px;border-radius:4px;
  background:rgba(239,83,80,.2);color:#ef5350;border:1px solid rgba(239,83,80,.4);text-transform:uppercase}
.move-badge-drift{font-size:.65rem;font-weight:800;padding:2px 7px;border-radius:4px;
  background:rgba(255,152,0,.2);color:#ffa726;border:1px solid rgba(255,152,0,.4);text-transform:uppercase}
.move-confirm{font-size:.68rem;font-weight:700;color:#69f0ae}
.move-reverse{font-size:.68rem;font-weight:700;color:#ef5350}

/* ── SHARP MONEY PANEL ── */
.sharp-panel{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:20px 24px;margin-bottom:28px}
.sharp-game{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-start;
  padding:14px 0;border-bottom:1px solid rgba(255,255,255,.06)}
.sharp-game:last-child{border-bottom:none;padding-bottom:0}
.sharp-matchup{font-size:.9rem;font-weight:700;color:var(--text);min-width:180px}
.sharp-desc{font-size:.82rem;color:var(--sub);flex:1;line-height:1.5}
.sharp-agree{font-size:.75rem;font-weight:700;color:#69f0ae;white-space:nowrap}
.sharp-reverse{font-size:.75rem;font-weight:700;color:#ef5350;white-space:nowrap}
.sharp-neutral{font-size:.75rem;font-weight:700;color:var(--sub);white-space:nowrap}
.sharp-badge{font-size:.65rem;font-weight:800;padding:3px 9px;border-radius:4px;
  text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}
.sharp-badge-steam{background:rgba(239,83,80,.2);color:#ef5350;border:1px solid rgba(239,83,80,.4)}
.sharp-badge-drift{background:rgba(255,152,0,.2);color:#ffa726;border:1px solid rgba(255,152,0,.4)}

/* ── KALSHI SIGNAL ── */
.kalshi-row{
  display:flex;align-items:center;gap:8px;
  margin-top:8px;padding:5px 8px;border-radius:6px;
  background:rgba(255,255,255,.03);border:1px solid var(--border);
  font-size:.72rem;
}
.kalshi-label{color:var(--sub);font-weight:600}
.kalshi-prob {font-weight:700;color:var(--text)}
.signal-AGREE    {color:#00e676;font-weight:700}
.signal-DISAGREE {color:#ef5350;font-weight:700}
.signal-NEUTRAL  {color:var(--sub);font-weight:600}

/* ── YESTERDAY PANEL ── */
.yesterday-banner{
  background:linear-gradient(135deg,rgba(13,23,46,.9),rgba(13,27,46,.9));
  border:1px solid var(--border);border-radius:var(--radius);
  padding:18px 22px;margin-bottom:28px;
}
.yesterday-title{
  font-size:.8rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;
  color:var(--sub);margin-bottom:12px;display:flex;align-items:center;gap:8px;
}
.yesterday-stats{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:12px}
.yday-stat{text-align:center}
.yday-num{font-size:1.5rem;font-weight:800;color:var(--text)}
.yday-lbl{font-size:.7rem;color:var(--sub);font-weight:600;text-transform:uppercase;letter-spacing:.5px}
.yday-wins   .yday-num{color:var(--green)}
.yday-losses .yday-num{color:var(--red)}
.yday-roi    .yday-num{color:var(--gold)}
.yday-findings{display:flex;flex-direction:column;gap:4px;border-top:1px solid var(--border);padding-top:10px;margin-top:4px}
.yday-finding{font-size:.74rem;color:var(--sub);line-height:1.5}
.yday-finding.warn{color:#ffb74d}

/* ── PICKS GRID ── */
.picks-grid{
  display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
  gap:14px;margin-bottom:36px;
}
.pick-card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;cursor:pointer;transition:transform .15s,border-color .15s;
  position:relative;overflow:hidden;
}
.pick-card:hover{transform:translateY(-2px)}
.pick-card.expanded{border-color:var(--green)}
.pick-card-props-toggle{
  display:flex;align-items:center;gap:5px;font-size:.7rem;font-weight:600;
  color:var(--sub);margin-top:8px;padding-top:8px;
  border-top:1px solid rgba(255,255,255,.05);cursor:pointer;
  transition:color .15s;user-select:none;
}
.pick-card-props-toggle:hover{color:var(--green)}
.pick-card-props-panel{
  display:none;margin-top:10px;padding-top:10px;
  border-top:1px solid rgba(0,230,118,.2);
}
.pick-card-props-panel.open{display:block}
.inline-prop{
  display:flex;justify-content:space-between;align-items:center;
  padding:5px 0;border-bottom:1px solid rgba(255,255,255,.04);
  font-size:.75rem;
}
.inline-prop:last-child{border-bottom:none}
.inline-prop-player{color:var(--text);font-weight:600}
.inline-prop-line{color:var(--sub)}
.inline-prop-conf{font-weight:700;white-space:nowrap;margin-left:8px}
.inline-prop-tier-LOCK  {color:var(--gold)}
.inline-prop-tier-STRONG{color:var(--blue)}
.inline-prop-tier-LEAN  {color:var(--green)}
.inline-props-empty{font-size:.74rem;color:var(--sub);padding:6px 0;font-style:italic}
.pick-card.tier-LOCK  {border-top:3px solid var(--gold)}
.pick-card.tier-STRONG{border-top:3px solid var(--blue)}
.pick-card.tier-LEAN  {border-top:3px solid var(--green)}
.pick-card.hidden{display:none}

.pick-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
.pick-type-badge{
  font-size:.68rem;font-weight:700;letter-spacing:.8px;padding:3px 9px;border-radius:4px;
  text-transform:uppercase;
}
.badge-ML   {background:rgba(66,165,245,.15);color:#42a5f5;border:1px solid rgba(66,165,245,.3)}
.badge-RL   {background:rgba(171,71,188,.15);color:#ce93d8;border:1px solid rgba(171,71,188,.3)}
.badge-TOTAL{background:rgba(0,230,118,.15);color:#00e676;border:1px solid rgba(0,230,118,.3)}
.tier-badge{
  font-size:.7rem;font-weight:700;padding:3px 9px;border-radius:4px;
}
.tb-LOCK  {background:rgba(255,193,7,.15);color:var(--gold);border:1px solid rgba(255,193,7,.3)}
.tb-STRONG{background:rgba(66,165,245,.15);color:var(--blue);border:1px solid rgba(66,165,245,.3)}
.tb-LEAN  {background:rgba(0,230,118,.15);color:var(--green);border:1px solid rgba(0,230,118,.3)}

.pick-label{font-size:1.08rem;font-weight:700;color:var(--text);margin-bottom:4px}
.pick-game{font-size:.78rem;color:var(--sub);margin-bottom:12px}

.conf-row{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.conf-bar-wrap{flex:1;height:7px;background:rgba(255,255,255,.07);border-radius:4px;overflow:hidden}
.conf-bar{height:100%;border-radius:4px;transition:width .4s ease}
.bar-LOCK  {background:linear-gradient(90deg,#e65100,var(--gold))}
.bar-STRONG{background:linear-gradient(90deg,#0d47a1,var(--blue))}
.bar-LEAN  {background:linear-gradient(90deg,#1b5e20,var(--green))}
.conf-pct{font-size:.95rem;font-weight:700;white-space:nowrap}
.pct-LOCK  {color:var(--gold)}
.pct-STRONG{color:var(--blue)}
.pct-LEAN  {color:var(--green)}

.pick-reasoning{
  font-size:.74rem;color:var(--sub);line-height:1.5;
  border-top:1px solid var(--border);padding-top:8px;margin-top:4px;
}

/* ── PARLAY CARDS ── */
.parlay-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px;margin-bottom:36px}
.parlay-card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;position:relative;overflow:hidden;
}
.parlay-card::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--green),var(--blue),var(--gold));
}
.parlay-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.parlay-tag{font-size:.7rem;font-weight:700;letter-spacing:.6px;color:var(--sub);text-transform:uppercase}
.parlay-payout{
  font-size:.95rem;font-weight:800;color:var(--green);
  background:rgba(0,230,118,.1);border:1px solid rgba(0,230,118,.25);
  padding:3px 10px;border-radius:6px;
}
.parlay-conf{font-size:1.5rem;font-weight:800;color:var(--text);margin-bottom:12px}
.parlay-conf span{font-size:.85rem;font-weight:500;color:var(--sub);margin-left:4px}
.parlay-legs{display:flex;flex-direction:column;gap:8px}
.parlay-leg{
  background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:9px 12px;
  display:flex;align-items:center;gap:10px;
}
.leg-conf{
  font-size:.78rem;font-weight:700;white-space:nowrap;padding:2px 8px;border-radius:4px;
}
.leg-LOCK  {background:rgba(255,193,7,.15);color:var(--gold)}
.leg-STRONG{background:rgba(66,165,245,.15);color:var(--blue)}
.leg-LEAN  {background:rgba(0,230,118,.15);color:var(--green)}
.leg-info{flex:1}
.leg-label{font-size:.85rem;font-weight:600;color:var(--text)}
.leg-game {font-size:.72rem;color:var(--sub);margin-top:1px}

/* ── GAME BREAKDOWN ── */
.games-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(380px,1fr));gap:14px}
.game-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.game-header{
  background:var(--card2);padding:12px 16px;display:flex;justify-content:space-between;align-items:center;
  border-bottom:1px solid var(--border);
}
.matchup{font-size:.95rem;font-weight:700}
.game-time{font-size:.75rem;color:var(--sub)}
.game-body{padding:14px 16px;display:flex;flex-direction:column;gap:8px}
.game-row{display:flex;justify-content:space-between;align-items:center;font-size:.8rem}
.row-label{color:var(--sub);font-weight:600;min-width:48px}
.row-val{color:var(--text);font-weight:500;text-align:right}
.park-badge{
  display:inline-block;font-size:.68rem;font-weight:700;padding:2px 7px;border-radius:4px;margin-left:6px;
}
.park-hitter {background:rgba(239,83,80,.15);color:#ef9a9a}
.park-pitcher{background:rgba(66,165,245,.15);color:#90caf9}
.park-neutral{background:rgba(255,255,255,.06);color:var(--sub)}
.wp-row{display:flex;gap:6px;margin-top:4px}
.wp-bar-outer{flex:1;background:rgba(255,255,255,.06);border-radius:4px;overflow:hidden;height:6px;margin-top:2px}
.wp-bar-inner{height:100%;background:var(--green);border-radius:4px}
.wp-team{font-size:.78rem;font-weight:600}
.wp-pct {font-size:.85rem;font-weight:700;color:var(--green)}

/* ── EMPTY STATE ── */
.empty{text-align:center;color:var(--sub);padding:48px;font-size:.95rem}

/* ── TABS ── */
.section-tabs{display:flex;gap:0;margin-bottom:16px;border:1px solid var(--border);border-radius:8px;overflow:hidden;width:fit-content}
.section-tab{
  padding:7px 18px;font-size:.82rem;font-weight:600;cursor:pointer;
  background:transparent;border:none;color:var(--sub);font-family:inherit;
  transition:all .15s;
}
.section-tab.active{background:var(--green);color:#000}
.section-tab:hover:not(.active){background:rgba(255,255,255,.05);color:var(--text)}

/* ── RESULTS COUNT ── */
.results-count{font-size:.8rem;color:var(--sub);margin-bottom:10px}
.results-count b{color:var(--text)}

/* ── SCORES TICKER ── */
.ticker-wrap{
  background:#080c14;border-bottom:1px solid var(--border);
  padding:0;height:34px;display:flex;align-items:center;
}
.ticker-label{
  background:var(--green);color:#000;font-weight:800;font-size:.72rem;
  letter-spacing:.8px;padding:0 12px;height:100%;display:flex;
  align-items:center;white-space:nowrap;flex-shrink:0;text-transform:uppercase;
}
.ticker-outer{
  flex:1;overflow:hidden;height:100%;display:flex;align-items:center;
}
.ticker-track{
  display:inline-flex;white-space:nowrap;
  animation:ticker 35s linear infinite;
  will-change:transform;
}
.ticker-track:hover{animation-play-state:paused}
@keyframes ticker{
  0%  {transform:translateX(0)}
  100%{transform:translateX(-50%)}
}
.ticker-item{
  display:inline-flex;align-items:center;gap:6px;
  padding:0 24px;font-size:.78rem;font-weight:600;color:var(--text);
  border-right:1px solid var(--border);
}
.ticker-score{font-weight:800;font-size:.85rem}
.ticker-score.win{color:var(--green)}
.ticker-score.loss{color:var(--sub)}
.ticker-final{font-size:.65rem;color:var(--sub);text-transform:uppercase;letter-spacing:.5px}
.ticker-live{font-size:.65rem;color:#ff6b35;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.ticker-empty{color:var(--sub);font-size:.78rem;padding:0 20px}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* ── PROPS SECTION ── */
.props-filter-row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:16px}
.props-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(310px,1fr));gap:14px;margin-bottom:36px}
.prop-card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;transition:transform .15s,border-color .15s;position:relative;overflow:hidden;
}
.prop-card:hover{transform:translateY(-2px)}
.prop-card.tier-LOCK  {border-top:3px solid var(--gold)}
.prop-card.tier-STRONG{border-top:3px solid var(--blue)}
.prop-card.tier-LEAN  {border-top:3px solid var(--green)}
.prop-card.hidden{display:none}
.prop-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px}
.prop-type-badge{
  font-size:.7rem;font-weight:700;letter-spacing:.8px;padding:3px 9px;border-radius:4px;text-transform:uppercase;
}
.badge-HR   {background:rgba(239,83,80,.15);color:#ef9a9a;border:1px solid rgba(239,83,80,.3)}
.badge-HITS {background:rgba(66,165,245,.15);color:#90caf9;border:1px solid rgba(66,165,245,.3)}
.badge-K    {background:rgba(171,71,188,.15);color:#ce93d8;border:1px solid rgba(171,71,188,.3)}
.badge-TB   {background:rgba(255,167,38,.15);color:#ffcc80;border:1px solid rgba(255,167,38,.3)}
.badge-RBI  {background:rgba(38,166,154,.15);color:#80cbc4;border:1px solid rgba(38,166,154,.3)}
.badge-R    {background:rgba(102,187,106,.15);color:#a5d6a7;border:1px solid rgba(102,187,106,.3)}
.badge-SB   {background:rgba(41,182,246,.15);color:#81d4fa;border:1px solid rgba(41,182,246,.3)}

/* ── TEAMS TAB ── */
.team-btn{padding:7px 13px;border-radius:6px;border:1px solid var(--border);background:var(--card);
  color:var(--sub);font-size:.8rem;font-weight:600;cursor:pointer;transition:all .15s;white-space:nowrap}
.team-btn:hover{border-color:var(--green);color:var(--text)}
.team-btn.active{background:var(--green);color:#fff !important;border-color:var(--green)}
.team-btn.playing{border-color:rgba(0,230,118,.4);color:var(--green)}
.team-btn.playing.active{color:#fff !important}

.team-game-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:24px;margin-bottom:20px}
.team-matchup-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;flex-wrap:wrap;gap:12px}
.team-matchup-teams{display:flex;align-items:center;gap:16px;flex:1}
.team-name-block{text-align:center;min-width:140px}
.team-name-big{font-size:1.15rem;font-weight:800;color:var(--text)}
.team-name-sp{font-size:.75rem;color:var(--sub);margin-top:3px}
.team-vs{font-size:1.4rem;color:var(--sub);font-weight:300;padding:0 8px}
.team-game-meta{text-align:right;font-size:.8rem;color:var(--sub);line-height:1.7}
.team-day-label{font-size:1rem;font-weight:700;color:var(--green);display:block}

.team-pred-row{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:18px}
.team-pred-box{background:var(--bg);border-radius:8px;padding:14px;text-align:center}
.team-pred-label{font-size:.72rem;color:var(--sub);margin-bottom:6px;letter-spacing:.5px;text-transform:uppercase}
.team-pred-conf{font-size:1.6rem;font-weight:800}
.team-pred-bar{height:6px;background:rgba(255,255,255,.08);border-radius:3px;margin:8px 0 4px}
.team-pred-fill{height:6px;border-radius:3px;transition:width .4s}
.team-pred-pick{font-size:.75rem;font-weight:700;margin-top:4px}

.team-props-section{margin-top:18px}
.team-props-header{font-size:.85rem;font-weight:700;color:var(--sub);margin-bottom:10px;
  text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid var(--border);padding-bottom:6px}
.team-props-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.team-prop-mini{background:var(--bg);border-radius:8px;padding:11px 14px;border:1px solid var(--border)}
.team-prop-mini-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:4px}
.team-prop-mini-name{font-size:.9rem;font-weight:700;color:var(--text)}
.team-prop-mini-type{font-size:.7rem;padding:2px 7px;border-radius:4px;font-weight:700}
.team-prop-mini-line{font-size:.78rem;color:var(--sub);margin-bottom:6px}
.team-prop-mini-conf{font-size:.85rem;font-weight:700}
.team-no-props{color:var(--sub);font-size:.85rem;padding:12px 0;font-style:italic}

.prop-type-filters{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px}
.ptype-btn{padding:4px 11px;border-radius:20px;border:1px solid var(--border);background:transparent;
  color:var(--sub);font-size:.75rem;font-weight:600;cursor:pointer;transition:all .15s}
.ptype-btn:hover{border-color:var(--green);color:var(--text)}
.ptype-btn.active{background:rgba(0,230,118,.15);border-color:var(--green);color:var(--green)}

.prop-player{font-size:1.05rem;font-weight:700;color:var(--text);margin-bottom:2px}
.prop-game  {font-size:.75rem;color:var(--sub);margin-bottom:10px}
.prop-line-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;
  background:rgba(255,255,255,.04);border-radius:6px;padding:6px 10px}
.prop-line-label{font-size:.72rem;color:var(--sub);font-weight:600}
.prop-line-val{font-size:.92rem;font-weight:700;color:var(--text)}
.prop-proj-val{font-size:.8rem;font-weight:600;color:var(--green)}
.prop-reasoning{
  font-size:.73rem;color:var(--sub);line-height:1.5;
  border-top:1px solid var(--border);padding-top:8px;margin-top:4px;
}
.section-nav{
  display:flex;gap:0;margin-bottom:24px;border-bottom:2px solid var(--border);
}
.section-nav-btn{
  padding:10px 22px;font-size:.88rem;font-weight:700;cursor:pointer;
  background:transparent;border:none;border-bottom:2px solid transparent;
  color:var(--sub);font-family:inherit;transition:all .15s;margin-bottom:-2px;
}
.section-nav-btn.active{color:var(--green);border-bottom-color:var(--green)}
.section-nav-btn:hover:not(.active){color:var(--text)}
.section-panel{display:none}
.section-panel.active{display:block}

/* ── TODAY'S GAMES ── */
.schedule-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px;margin-bottom:36px}
.sched-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.sched-status-bar{
  padding:5px 14px;font-size:.7rem;font-weight:700;letter-spacing:.6px;text-transform:uppercase;
  display:flex;align-items:center;gap:6px;
}
.status-upcoming{background:rgba(66,165,245,.12);color:var(--blue)}
.status-live    {background:rgba(255,107,53,.15);color:#ff6b35}
.status-final   {background:rgba(255,255,255,.06);color:var(--sub)}
.live-dot{width:7px;height:7px;border-radius:50%;background:#ff6b35;animation:pulse 1.5s infinite;flex-shrink:0}
.sched-matchup{padding:14px 16px 10px}
.sched-team-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.sched-team-info{display:flex;flex-direction:column;gap:2px}
.sched-team-name{font-size:1rem;font-weight:700;color:var(--text)}
.sched-team-record{font-size:.78rem;font-weight:600;color:var(--sub)}
.sched-team-streak{font-size:.68rem;color:var(--sub)}
.sched-score-block{text-align:center;min-width:54px}
.sched-score{font-size:1.5rem;font-weight:800;color:var(--text)}
.sched-score.winner{color:var(--green)}
.sched-vs{font-size:.85rem;color:var(--sub);font-weight:600}
.sched-at-label{font-size:.62rem;color:var(--sub);text-transform:uppercase;letter-spacing:.5px;margin-top:2px}
.sched-divider{height:1px;background:var(--border);margin:0 16px}
.sched-footer{padding:8px 16px;display:flex;justify-content:space-between;align-items:center;font-size:.75rem}
.sched-time{font-weight:700;color:var(--text)}
.sched-venue{color:var(--sub);text-align:right;max-width:55%}
.sched-pitchers{padding:4px 16px 10px;font-size:.72rem;color:var(--sub);
  display:flex;justify-content:space-between}

/* ── SCROLLBAR ── */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#2e4060}

/* ── SHARP ACTION TAB ── */
.sharp-action-card{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:20px 24px;margin-bottom:16px;transition:border-color .15s}
.sharp-action-card:hover{border-color:rgba(239,83,80,.3)}
.sharp-explain-box{background:rgba(239,83,80,.06);border:1px solid rgba(239,83,80,.2);
  border-radius:10px;padding:14px 18px;margin-bottom:24px;font-size:.82rem;color:var(--sub);line-height:1.6}
.sharp-odds-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.sharp-odds-box{background:var(--bg);border-radius:8px;padding:12px;text-align:center}
.sharp-odds-team-lbl{font-size:.68rem;color:var(--sub);margin-bottom:8px;
  text-transform:uppercase;letter-spacing:.5px;font-weight:700}
.sharp-odds-row{display:flex;justify-content:space-around;align-items:center;gap:4px}
.sharp-odds-cell{text-align:center}
.sharp-odds-cell-lbl{font-size:.6rem;color:var(--sub)}
.sharp-odds-cell-num{font-size:1.05rem;font-weight:800}
.sharp-odds-arrow{color:var(--sub);font-size:1.1rem;align-self:center}
.sharp-total-row{background:var(--bg);border-radius:8px;padding:10px 14px;margin-bottom:12px;
  display:flex;align-items:center;gap:12px;font-size:.82rem;flex-wrap:wrap}
.sharp-bet-call{background:rgba(0,230,118,.06);border:1px solid rgba(0,230,118,.2);
  border-radius:8px;padding:12px 16px;display:flex;justify-content:space-between;
  align-items:center;flex-wrap:wrap;gap:8px}
.sharp-bet-call-lbl{font-size:.68rem;color:var(--sub);text-transform:uppercase;
  letter-spacing:.5px;margin-bottom:4px}
.sharp-bet-call-text{font-size:.95rem;font-weight:800;color:var(--text)}
.sharp-empty{text-align:center;padding:60px 20px}

/* ── STATUS LINK ── */
a.status-link{
  font-size:.72rem;color:var(--sub);text-decoration:none;
  border:1px solid var(--border);border-radius:12px;padding:3px 11px;
  transition:all .2s;
}
a.status-link:hover{color:var(--green);border-color:var(--green)}

/* ── PARLAY EV ── */
.parlay-ev{font-size:.72rem;font-weight:700;color:var(--green);
  background:rgba(0,230,118,.1);border:1px solid rgba(0,230,118,.2);
  border-radius:4px;padding:2px 9px;white-space:nowrap;margin-top:4px;display:inline-block}
.parlay-breakeven{font-size:.7rem;color:var(--sub);margin-top:4px}
</style>
</head>
<body>

<div class="header">
  <h1>⚾ Sports Betting Parlay Genius</h1>
  <div class="header-sub" id="dateStr"></div>
  <div class="header-stats">
    <div class="stat-pill">Games <span id="gameCount">—</span></div>
    <div class="stat-pill">Picks <span id="pickCount">—</span></div>
    <div class="stat-pill">Locks <span id="lockCount">—</span></div>
    <div class="stat-pill">Top Pick <span id="topPick">—</span></div>
    <a href="/status" class="status-link">⚙ Status</a>
    <button id="refreshBtn" onclick="doRefresh()">
      <svg id="refreshIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
      </svg>
      Refresh Picks
    </button>
  </div>
  <div id="refreshStatus"></div>
</div>

<div class="ticker-wrap" id="scoreTicker">
  <div class="ticker-label">⚾ Scores</div>
  <div class="ticker-outer">
    <div class="ticker-track" id="tickerTrack">
      <span class="ticker-empty">No completed games yet today</span>
    </div>
  </div>
</div>

<div class="filters">
  <div class="filter-group">
    <span class="filter-label">Bet Type</span>
    <button class="filter-btn active" data-group="type" data-val="all">All</button>
    <button class="filter-btn" data-group="type" data-val="ML">Win Bet (Moneyline)</button>
    <button class="filter-btn" data-group="type" data-val="RL">Spread (Run Line)</button>
    <button class="filter-btn" data-group="type" data-val="TOTAL">Over/Under</button>
  </div>
  <div class="filter-group">
    <span class="filter-label">Confidence</span>
    <button class="filter-btn active" data-group="tier" data-val="all">All</button>
    <button class="filter-btn" data-group="tier" data-val="LOCK">🔒 Lock — Best Bets</button>
    <button class="filter-btn" data-group="tier" data-val="STRONG">⭐⭐ Strong</button>
    <button class="filter-btn" data-group="tier" data-val="LEAN">⭐ Lean — Watch Only</button>
  </div>
  <div class="search-wrap">
    <input class="search-input" id="teamSearch" placeholder="Search team…" type="text"/>
  </div>
</div>

<div class="main">

  <!-- SECTION NAV -->
  <div class="section-nav">
    <button class="section-nav-btn active" data-panel="panel-picks">🎯 Game Picks</button>
    <button class="section-nav-btn" data-panel="panel-schedule">📅 Today's Games</button>
    <button class="section-nav-btn" data-panel="panel-props">👤 Player Props</button>
    <button class="section-nav-btn" data-panel="panel-teams">⚾ Teams</button>
    <button class="section-nav-btn" data-panel="panel-games">📊 Game Breakdown</button>
    <button class="section-nav-btn" data-panel="panel-yesterday" id="yesterdayTab" style="display:none">📈 Yesterday</button>
    <button class="section-nav-btn" data-panel="panel-sharp">🔥 Sharp Action</button>
  </div>

  <!-- PANEL: GAME PICKS -->
  <div class="section-panel active" id="panel-picks">
    <div id="yesterdayBanner"></div>
    <div class="section-title">🎯 Individual Picks</div>
    <div class="results-count" id="pickResults"></div>
    <div class="picks-grid" id="picksGrid"></div>

    <!-- Sharp Money Panel — only shown when movement data exists -->
    <div id="sharpMoneySection" style="display:none">
      <div class="section-title">💰 Where the Pro Money Is Going</div>
      <div class="sharp-panel" id="sharpMoneyGrid"></div>
    </div>

    <div class="section-title">🔥 Parlay Recommendations</div>
    <div class="section-tabs">
      <button class="section-tab active" data-parlay="2">2-Leg (+260)</button>
      <button class="section-tab" data-parlay="3">3-Leg (+595)</button>
    </div>
    <div class="parlay-grid" id="parlayGrid"></div>
  </div>

  <!-- PANEL: TODAY'S GAMES -->
  <div class="section-panel" id="panel-schedule">
    <div class="schedule-grid" id="scheduleGrid"></div>
  </div>

  <!-- PANEL: PLAYER PROPS -->
  <div class="section-panel" id="panel-props">
    <div class="props-filter-row">
      <span class="filter-label">Prop Type</span>
      <button class="filter-btn active" data-pgroup="ptype" data-pval="all">All</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="HR">⚡ HRs</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="HITS">🎯 Hits</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="TB">💥 Tot Bases</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="RBI">🏅 RBIs</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="R">🏃 Runs</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="SB">💨 Stolen Bases</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="K">🔥 Strikeouts</button>
      <span class="filter-label" style="margin-left:12px">Confidence</span>
      <button class="filter-btn active" data-pgroup="ptier" data-pval="all">All</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="LOCK">🔒 Lock</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="STRONG">⭐⭐ Strong</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="LEAN">⭐ Lean</button>
    </div>
    <div class="results-count" id="propResults"></div>
    <div class="props-grid" id="propsGrid"></div>
  </div>

  <!-- PANEL: TEAMS -->
  <div class="section-panel" id="panel-teams">
    <div class="section-title">⚾ Team Lookup</div>
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:12px;flex-wrap:wrap">
      <span style="font-size:.75rem;color:var(--sub)">
        <span style="color:var(--green);font-weight:700">●</span> Playing today
        &nbsp;&nbsp;
        <span style="color:var(--sub);font-weight:700">●</span> Off today
      </span>
      <span style="font-size:.72rem;color:var(--sub)">Lineups post ~2–3 hrs before first pitch · Props appear once confirmed</span>
    </div>
    <div id="teamButtonGrid" style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px"></div>
    <div id="teamGameView"></div>
  </div>

  <!-- PANEL: GAME BREAKDOWN -->
  <div class="section-panel" id="panel-games">
    <div class="section-title">📊 Game Breakdown</div>
    <div class="games-grid" id="gamesGrid"></div>
  </div>

  <!-- PANEL: YESTERDAY'S RESULTS -->
  <div class="section-panel" id="panel-yesterday">
    <div id="yesterdayFull"></div>
  </div>

  <!-- PANEL: SHARP ACTION -->
  <div class="section-panel" id="panel-sharp">
    <div id="sharpActionContent"></div>
  </div>

</div>

<script>
// ── Embedded Data ────────────────────────────────────────────────────────────
const DATA_DATE      = "__DATE__";
const DATA_PICKS     = __PICKS__;
const DATA_GAMES     = __GAMES__;
const DATA_TEAM_SCHED = __TEAM_SCHED__;
const DATA_P2        = __P2__;
const DATA_P3        = __P3__;
const DATA_SCORES    = __SCORES__;
const DATA_PROPS     = __PROPS__;
const DATA_SCHEDULE  = __SCHEDULE__;
const DATA_YESTERDAY     = __YESTERDAY__;
const DATA_MOVEMENT      = __MOVEMENT__;
const DATA_PROJ_LINEUPS  = __PROJ_LINEUPS__;

// ── State ────────────────────────────────────────────────────────────────────
let filterType = "all", filterTier = "all", filterTeam = "";
let showParlay = 2;

// ── Init ─────────────────────────────────────────────────────────────────────
document.getElementById("dateStr").textContent =
  new Date(DATA_DATE + "T12:00:00").toLocaleDateString("en-US",
    {weekday:"long",year:"numeric",month:"long",day:"numeric"});

document.getElementById("gameCount").textContent = DATA_GAMES.length;
document.getElementById("pickCount").textContent = DATA_PICKS.length;
document.getElementById("lockCount").textContent = DATA_PICKS.filter(p=>p.tier==="LOCK").length;
if(DATA_PICKS.length){
  const tp = DATA_PICKS[0];
  const awayCity = tp.away ? tp.away.split(" ").slice(-1)[0] : "";
  const homeCity = tp.home ? tp.home.split(" ").slice(-1)[0] : "";
  const gameCtx  = (awayCity && homeCity) ? ` · ${awayCity} @ ${homeCity}` : "";
  document.getElementById("topPick").innerHTML =
    `<span style="color:var(--gold)">${tp.label} (${tp.conf}%)</span><span style="color:var(--sub);font-size:.75rem">${gameCtx}</span>`;
}

// ── Render Picks ─────────────────────────────────────────────────────────────
function renderPicks(){
  const grid = document.getElementById("picksGrid");
  grid.innerHTML = "";
  let visible = 0;
  DATA_PICKS.forEach(p=>{
    const show = (filterType==="all" || p.type===filterType)
              && (filterTier==="all" || p.tier===filterTier)
              && (!filterTeam || p.away.toLowerCase().includes(filterTeam)
                             || p.home.toLowerCase().includes(filterTeam)
                             || p.team.toLowerCase().includes(filterTeam));
    if(!show) return;
    visible++;

    // Confidence bar — scaled from 50% baseline so visual = edge above coin flip
    const barPct = Math.min(100, Math.max(0, (p.conf - 50) * 2));

    // Warning badges — always shown, lit when active / dim when not
    const wb = (cls, active, label) =>
      `<span class="warn-badge ${cls} ${active ? 'active' : 'inactive'}">${label}</span>`;
    const warnHtml = `<div class="warn-row">
      ${wb('warn-tbd',    p.tbd_sp,      '⚠ Starter Unknown')}
      ${wb('warn-thin',   p.thin_edge,   'Close Call')}
      ${wb('warn-heavy',  p.heavy_fav,   'Heavy Favorite')}
      ${wb('warn-lineup', p.unconfirmed, 'Lineup Not Set')}
    </div>`;

    // Favorite tier — always shown on every card
    const favLabel = p.fav_tier === 'HEAVY'   ? '🔴 Heavy Fav'
                   : p.fav_tier === 'MEDIUM'  ? '🟡 Medium Fav'
                   : '🟢 Neutral';
    const favHtml = `<div class="fav-row">
      <span class="fav-label">Line:</span>
      <span class="fav-badge fav-${p.fav_tier||'NEUTRAL'}">${favLabel}</span>
    </div>`;

    // Kalshi signal
    let kalshiHtml = "";
    if(p.kalshi_prob !== null && p.kalshi_prob !== undefined){
      const sigClass = `signal-${p.kalshi_signal||"NEUTRAL"}`;
      const sigLabel = p.kalshi_signal === "AGREE"    ? "✓ Market Agrees"
                     : p.kalshi_signal === "DISAGREE" ? "⚠ Market Disagrees"
                     : "Market Neutral";
      kalshiHtml = `
        <div class="kalshi-row">
          <span class="kalshi-label">Kalshi</span>
          <span class="kalshi-prob">${p.kalshi_prob}%</span>
          <span class="${sigClass}">${sigLabel}</span>
        </div>`;
    }

    // Line movement for this game
    let moveHtml = "";
    const mv = DATA_MOVEMENT.find(m => m.away === p.away && m.home === p.home);
    if(mv){
      const sig     = mv.ml_signal === "STEAM" ? mv.ml_signal
                    : mv.total_signal === "STEAM" ? mv.total_signal
                    : mv.ml_signal === "DRIFT" ? mv.ml_signal : mv.total_signal;
      const sigBadge = sig === "STEAM"
        ? `<span class="move-badge-steam">🔥 Heavy Action</span>`
        : `<span class="move-badge-drift">📈 Notable Shift</span>`;

      const sharp     = mv.sharp_side || "";
      const isML      = p.type === "ML" || p.type === "RL";
      const pickedTeam = p.team || "";

      // Did sharp money go WITH or AGAINST our pick?
      const sharpAgrees = sharp && pickedTeam &&
        (sharp.toLowerCase().includes(pickedTeam.toLowerCase()) ||
         pickedTeam.toLowerCase().includes(sharp.split(" ").slice(-1)[0].toLowerCase()));
      const sharpLabel = !sharp ? ""
        : sharpAgrees
          ? `<span class="move-confirm">✓ Pro money agrees</span>`
          : `<span class="move-reverse">⚠ Pro money going other way</span>`;

      // Build readable movement description
      let moveDesc = "";
      if(mv.ml_signal === "STEAM" || mv.ml_signal === "DRIFT"){
        const isSideAway = sharp && mv.away && sharp.toLowerCase().includes(mv.away.split(" ").slice(-1)[0].toLowerCase());
        const openOdds = isSideAway ? mv.ml_away_open : mv.ml_home_open;
        const nowOdds  = isSideAway ? mv.ml_away_now  : mv.ml_home_now;
        const pts      = Math.abs(isSideAway ? mv.ml_away_move : mv.ml_home_move || 0);
        const fmtOdds  = o => o ? (o > 0 ? `+${Math.round(o)}` : Math.round(o)) : "?";
        if(openOdds && nowOdds && sharp){
          moveDesc = `${sharp.split(" ").slice(-1)[0]} odds: opened ${fmtOdds(openOdds)} → now ${fmtOdds(nowOdds)} (${pts} pt shift)`;
        }
      } else if(mv.total_signal === "STEAM" || mv.total_signal === "DRIFT"){
        const dir = (mv.total_move || 0) > 0 ? "higher" : "lower";
        moveDesc = `Total moved ${Math.abs(mv.total_move || 0).toFixed(1)} runs ${dir}: opened ${mv.total_open} → now ${mv.total_now}`;
      }

      moveHtml = `<div class="move-row">
        <span class="move-icon">💰</span>
        ${sigBadge}
        ${moveDesc ? `<span class="move-detail">${moveDesc}</span>` : ""}
        ${sharpLabel}
      </div>`;
    }

    grid.innerHTML += `
      <div class="pick-card tier-${p.tier}" data-type="${p.type}" data-tier="${p.tier}">
        <div class="pick-top">
          <span class="pick-type-badge badge-${p.type}">${p.type==="TOTAL"?"Over/Under":p.type==="ML"?"Win Bet":p.type==="RL"?"Spread":p.type}</span>
          <span class="tier-badge tb-${p.tier}">${tierIcon(p.tier)} ${p.tier}</span>
        </div>
        <div class="pick-label">${p.label}</div>
        <div class="pick-game">${p.game}</div>
        <div class="conf-row">
          <div class="conf-bar-wrap">
            <div class="conf-bar bar-${p.tier}" style="width:${barPct}%"></div>
          </div>
          <span class="conf-pct pct-${p.tier}">${p.conf}%</span>
        </div>
        ${favHtml}
        ${warnHtml}
        ${kalshiHtml}
        ${moveHtml}
        <div class="pick-reasoning">${p.reasoning}</div>
        <div class="pick-card-props-toggle" onclick="toggleCardProps(event, this)">
          <span class="toggle-arrow">▶</span> View Player Props for this game
        </div>
        <div class="pick-card-props-panel">
          ${buildInlineProps(p.away, p.home)}
        </div>
      </div>`;
  });
  document.getElementById("pickResults").innerHTML =
    `Showing <b>${visible}</b> of <b>${DATA_PICKS.length}</b> picks`;
  if(visible===0) grid.innerHTML = `<div class="empty">No picks match the current filters.</div>`;
}

function tierIcon(t){ return t==="LOCK"?"🔒":t==="STRONG"?"⭐⭐":"⭐"; }

// ── Inline props for pick card ────────────────────────────────────────────────
function buildInlineProps(away, home){
  if(!DATA_PROPS || !DATA_PROPS.length){
    return `<div class="inline-props-empty">Player props will appear here once lineups are confirmed — check back closer to game time.</div>`;
  }
  const gameProps = DATA_PROPS.filter(p =>
    (p.away === away && p.home === home) ||
    (p.game && p.game.includes(away) && p.game.includes(home))
  );
  if(!gameProps.length){
    return `<div class="inline-props-empty">No props available for this game yet.</div>`;
  }
  // Sort by conf descending, show top 8
  const sorted = [...gameProps].sort((a,b) => (b.conf||0)-(a.conf||0)).slice(0,8);
  return sorted.map(p => {
    const betDesc = propLabel(p.prop_type, p.line);   // e.g. "Hits Over 0.5"
    const tierCls = `inline-prop-tier-${p.tier||"LEAN"}`;
    return `<div class="inline-prop">
      <span class="inline-prop-player">${p.player_name||"—"}</span>
      <span class="inline-prop-line">${betDesc}</span>
      <span class="inline-prop-conf ${tierCls}">${p.conf||"—"}% ${tierIcon(p.tier||"LEAN")}</span>
    </div>`;
  }).join("");
}

function toggleCardProps(event, el){
  event.stopPropagation();
  const panel = el.nextElementSibling;
  const arrow = el.querySelector(".toggle-arrow");
  const isOpen = panel.classList.toggle("open");
  arrow.textContent = isOpen ? "▼" : "▶";
  el.closest(".pick-card").classList.toggle("expanded", isOpen);
}

// ── Sharp Money Panel ─────────────────────────────────────────────────────────
function renderSharpMoney(){
  const section = document.getElementById("sharpMoneySection");
  const grid    = document.getElementById("sharpMoneyGrid");
  if(!DATA_MOVEMENT || DATA_MOVEMENT.length === 0){
    section.style.display = "none";
    return;
  }
  section.style.display = "block";
  grid.innerHTML = "";

  DATA_MOVEMENT.forEach(mv => {
    const awayName = mv.away.split(" ").slice(-1)[0];
    const homeName = mv.home.split(" ").slice(-1)[0];

    // Determine the dominant signal
    const sig = mv.ml_signal === "STEAM" ? "STEAM"
              : mv.total_signal === "STEAM" ? "STEAM"
              : mv.ml_signal === "DRIFT"  ? "DRIFT" : "DRIFT";
    const sigBadge = sig === "STEAM"
      ? `<span class="sharp-badge sharp-badge-steam">🔥 Heavy Action</span>`
      : `<span class="sharp-badge sharp-badge-drift">📈 Notable Shift</span>`;

    // Plain-English description
    let desc = "";
    const sharp = mv.sharp_side || "";
    const sharpNick = sharp ? sharp.split(" ").slice(-1)[0] : "";

    const fmtO = o => o ? (o > 0 ? `+${Math.round(o)}` : `${Math.round(o)}`) : "?";
    if((mv.ml_signal === "STEAM" || mv.ml_signal === "DRIFT") && sharp){
      const isSideAway = mv.away && sharpNick.toLowerCase() === mv.away.split(" ").slice(-1)[0].toLowerCase();
      const openOdds   = isSideAway ? mv.ml_away_open : mv.ml_home_open;
      const nowOdds    = isSideAway ? mv.ml_away_now  : mv.ml_home_now;
      const pts        = Math.abs((isSideAway ? mv.ml_away_move : mv.ml_home_move) || 0);
      const word       = sig === "STEAM" ? "heavily" : "noticeably";
      desc = `Professional bettors are ${word} backing the <strong>${sharpNick}</strong> to win. `;
      desc += `The win odds opened at ${fmtO(openOdds)} and have moved to ${fmtO(nowOdds)} — a ${pts}-point shift in their favor`;
      if(mv.total_signal === "STEAM" || mv.total_signal === "DRIFT"){
        const tdir = (mv.total_move||0) > 0 ? "higher" : "lower";
        desc += `. The game total also shifted ${Math.abs(mv.total_move||0).toFixed(1)} runs ${tdir} (${mv.total_open} → ${mv.total_now})`;
      }
      desc += ".";
    } else if(mv.total_signal === "STEAM" || mv.total_signal === "DRIFT"){
      const tdir = (mv.total_move||0) > 0 ? "higher (more runs expected)" : "lower (fewer runs expected)";
      const pts  = Math.abs(mv.total_move||0).toFixed(1);
      desc = `The expected total runs for this game shifted ${pts} runs ${tdir} — opened at ${mv.total_open}, now at ${mv.total_now}. Large bets came in on the ${(mv.total_move||0)>0?"Over":"Under"}.`;
    } else if(mv.kalshi_signal === "STEAM" || mv.kalshi_signal === "DRIFT"){
      const kSharp = mv.kalshi_sharp_side || "";
      const kNick  = kSharp ? kSharp.split(" ").slice(-1)[0] : "";
      const kWord  = mv.kalshi_signal === "STEAM" ? "strongly" : "noticeably";
      desc = kNick
        ? `The Kalshi prediction market shifted ${kWord} toward the <strong>${kNick}</strong>. Sportsbook lines haven't moved yet — this can be an early signal before books adjust.`
        : `The Kalshi prediction market repriced this game — sportsbook lines haven't moved yet.`;
    }

    // ── Kalshi confirmation badge ─────────────────────────────────────────
    let kalshiHtml = "";
    if(mv.kalshi_signal === "STEAM" || mv.kalshi_signal === "DRIFT"){
      const kSharp    = mv.kalshi_sharp_side || "";
      const kNick     = kSharp ? kSharp.split(" ").slice(-1)[0] : "";
      const kMovePct  = mv.kalshi_away_move != null
        ? Math.round(Math.abs(mv.kalshi_away_move) * 100) : 0;
      const kSigColor = mv.kalshi_signal === "STEAM" ? "#ef5350" : "#ffa726";
      const kLabel    = mv.kalshi_signal === "STEAM" ? "STEAM" : "DRIFT";

      // Does Kalshi sharp side match sportsbook sharp side?
      const sbSharpNick = (mv.sharp_side||"").split(" ").slice(-1)[0].toLowerCase();
      const ksMatch = kNick && sbSharpNick && kNick.toLowerCase() === sbSharpNick;
      const ksBadge = ksMatch
        ? `<span style="color:#69f0ae;font-size:.72rem;font-weight:700">✓ Confirms sportsbook signal</span>`
        : (mv.sharp_side
          ? `<span style="color:#ffa726;font-size:.72rem;font-weight:700">⚠ Diverges from sportsbook</span>`
          : "");

      kalshiHtml = `<div style="margin-top:8px;padding:8px 10px;background:rgba(100,181,246,.07);
        border-left:3px solid #42a5f5;border-radius:4px;font-size:.78rem">
        <span style="color:#42a5f5;font-weight:700">📊 Kalshi Market</span>
        <span style="margin-left:8px;color:${kSigColor};font-weight:700">${kLabel}</span>
        ${kNick ? `<span style="color:#e2e8f0"> — Market moving toward <strong>${kNick}</strong>` +
          (kMovePct ? ` (+${kMovePct}pp)` : "") + `</span>` : ""}
        ${ksBadge ? `<span style="margin-left:10px">${ksBadge}</span>` : ""}
      </div>`;
    }

    // Does sharp money agree with any of our picks for this game?
    const gamePick = DATA_PICKS.find(p => p.away === mv.away && p.home === mv.home);
    let agreeHtml = "";
    if(gamePick && sharp){
      const pickTeamNick = gamePick.team ? gamePick.team.split(" ").slice(-1)[0] : "";
      const sharpMatch   = sharpNick && pickTeamNick &&
        (sharpNick.toLowerCase() === pickTeamNick.toLowerCase());
      if(sharpMatch){
        agreeHtml = `<span class="sharp-agree">✓ Agrees with our ${gamePick.tier} pick</span>`;
      } else if(gamePick.type === "ML" || gamePick.type === "RL"){
        agreeHtml = `<span class="sharp-reverse">⚠ Goes against our ${gamePick.tier} pick — use caution</span>`;
      }
    } else if(gamePick){
      agreeHtml = `<span class="sharp-neutral">No model pick for this game</span>`;
    }

    grid.innerHTML += `<div class="sharp-game">
      <div>
        <div class="sharp-matchup">${awayName} @ ${homeName} ${sigBadge}</div>
        ${agreeHtml ? `<div style="margin-top:4px">${agreeHtml}</div>` : ""}
      </div>
      <div class="sharp-desc">${desc}${kalshiHtml}</div>
    </div>`;
  });
}

// ── Yesterday's Results Banner ────────────────────────────────────────────────
function renderYesterday(){
  if(!DATA_YESTERDAY || !DATA_YESTERDAY.date) return;

  const d   = DATA_YESTERDAY;
  const m   = d.metrics && d.metrics.overall;
  if(!m) return;

  const wr  = m.win_rate ? (m.win_rate * 100).toFixed(1) : "—";
  const roi = m.roi      ? (m.roi * 100).toFixed(1)      : "—";
  const roiColor = (m.roi || 0) >= 0 ? "var(--green)" : "var(--red)";
  const roiSign  = (m.roi || 0) >= 0 ? "+" : "";

  // Top 3 findings
  const topFindings = (d.findings || []).slice(0, 4).map(f => {
    const isWarn = f.includes("⚠");
    return `<div class="yday-finding${isWarn ? " warn" : ""}">${f}</div>`;
  }).join("");

  // Mini banner on picks panel
  document.getElementById("yesterdayBanner").innerHTML = `
    <div class="yesterday-banner">
      <div class="yesterday-title">📈 Yesterday (${d.date})</div>
      <div class="yesterday-stats">
        <div class="yday-stat yday-wins">
          <div class="yday-num">${m.wins}</div><div class="yday-lbl">Wins</div>
        </div>
        <div class="yday-stat yday-losses">
          <div class="yday-num">${m.losses}</div><div class="yday-lbl">Losses</div>
        </div>
        <div class="yday-stat">
          <div class="yday-num">${wr}%</div><div class="yday-lbl">Win Rate</div>
        </div>
        <div class="yday-stat yday-roi">
          <div class="yday-num" style="color:${roiColor}">${roiSign}${roi}%</div>
          <div class="yday-lbl">ROI</div>
        </div>
        <div class="yday-stat">
          <div class="yday-num">${(m.profit || 0) >= 0 ? "+" : ""}${(m.profit || 0).toFixed(2)}u</div>
          <div class="yday-lbl">Profit</div>
        </div>
      </div>
      <div class="yday-findings">${topFindings}</div>
    </div>`;

  // Show Yesterday tab
  document.getElementById("yesterdayTab").style.display = "";

  // Full yesterday panel
  const tierRows = Object.entries((d.metrics && d.metrics.by_tier) || {}).map(([t, b]) => {
    if(!b.total) return "";
    const twr = b.win_rate ? (b.win_rate*100).toFixed(1)+"%" : "—";
    return `<tr>
      <td>${t}</td><td>${b.wins}-${b.losses}</td><td>${twr}</td>
      <td>${b.roi ? (b.roi*100).toFixed(1)+"%" : "—"}</td>
      <td>${(b.profit||0) >= 0 ? "+":""  }${(b.profit||0).toFixed(2)}u</td>
    </tr>`;
  }).join("");

  const typeRows = Object.entries((d.metrics && d.metrics.by_type) || {}).map(([t, b]) => {
    if(!b.total) return "";
    const twr = b.win_rate ? (b.win_rate*100).toFixed(1)+"%" : "—";
    return `<tr>
      <td>${t}</td><td>${b.wins}-${b.losses}</td><td>${twr}</td>
      <td>${b.roi ? (b.roi*100).toFixed(1)+"%" : "—"}</td>
      <td>${(b.profit||0) >= 0 ? "+" : ""}${(b.profit||0).toFixed(2)}u</td>
    </tr>`;
  }).join("");

  const tblStyle = "width:100%;border-collapse:collapse;font-size:.8rem;margin-bottom:20px";
  const thStyle  = "text-align:left;padding:6px 10px;color:var(--sub);font-size:.72rem;font-weight:700;text-transform:uppercase;border-bottom:1px solid var(--border)";
  const tdStyle  = "padding:6px 10px;border-bottom:1px solid rgba(255,255,255,.04)";

  const allFindings = (d.findings || []).map(f => {
    const isWarn = f.includes("⚠");
    return `<div class="yday-finding${isWarn ? " warn" : ""}" style="padding:4px 0">${f}</div>`;
  }).join("");

  const allRecs = (d.recommendations || []).map(r => `
    <div style="margin-bottom:12px;padding:10px 14px;background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:8px">
      <div style="font-size:.72rem;font-weight:700;color:${r.priority==='HIGH'?'var(--red)':r.priority==='MED'?'var(--gold)':'var(--sub)'};text-transform:uppercase;margin-bottom:4px">[${r.priority}] ${r.area}</div>
      <div style="font-size:.78rem;color:var(--sub)">${r.action}</div>
    </div>`).join("");

  document.getElementById("yesterdayFull").innerHTML = `
    <div class="section-title">📈 Yesterday's Performance — ${d.date}</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px">
      <div>
        <div class="section-title" style="font-size:.78rem">By Tier</div>
        <table style="${tblStyle}">
          <thead><tr>
            <th style="${thStyle}">Tier</th><th style="${thStyle}">Record</th>
            <th style="${thStyle}">Win%</th><th style="${thStyle}">ROI</th><th style="${thStyle}">P/L</th>
          </tr></thead>
          <tbody style="">${tierRows.replace(/<td>/g, `<td style="${tdStyle}">`).replace(/<th>/g, `<th style="${thStyle}">`)}</tbody>
        </table>
      </div>
      <div>
        <div class="section-title" style="font-size:.78rem">By Type</div>
        <table style="${tblStyle}">
          <thead><tr>
            <th style="${thStyle}">Type</th><th style="${thStyle}">Record</th>
            <th style="${thStyle}">Win%</th><th style="${thStyle}">ROI</th><th style="${thStyle}">P/L</th>
          </tr></thead>
          <tbody>${typeRows.replace(/<td>/g, `<td style="${tdStyle}">`).replace(/<th>/g, `<th style="${thStyle}">`)}</tbody>
        </table>
      </div>
    </div>
    <div class="section-title" style="font-size:.78rem">Findings</div>
    <div style="margin-bottom:20px">${allFindings}</div>
    <div class="section-title" style="font-size:.78rem">Recommendations</div>
    ${allRecs}`;
}

// ── Render Parlays ────────────────────────────────────────────────────────────
function renderParlays(){
  const data = showParlay===2 ? DATA_P2 : DATA_P3;
  const grid = document.getElementById("parlayGrid");
  grid.innerHTML = "";
  if(!data.length){
    grid.innerHTML=`<div class="empty">Not enough qualified legs for ${showParlay}-leg parlays today.</div>`;
    return;
  }
  // Break-even thresholds by payout string
  const BREAKEVEN = {"+260": 27.8, "+595": 14.4, "+1228": 7.5, "+2435": 3.9};

  data.forEach((par,i)=>{
    const legsHtml = par.legs.map(l=>`
      <div class="parlay-leg">
        <span class="leg-conf leg-${l.tier}">${l.conf}%</span>
        <div class="leg-info">
          <div class="leg-label">${l.label}</div>
          <div class="leg-game">${l.game}</div>
        </div>
      </div>`).join("");

    // Edge vs break-even — makes clear why a "low" combined % is still a great bet
    const be    = BREAKEVEN[par.payout] || (100 / (par.n_legs * 3.6));
    const edge  = (par.combined - be).toFixed(1);
    const evHtml = `<div class="parlay-ev">+${edge}% edge vs break-even</div>
      <div class="parlay-breakeven">Break-even at ${par.payout} payout: ${be}% — you're at ${par.combined}%</div>`;

    grid.innerHTML += `
      <div class="parlay-card">
        <div class="parlay-header">
          <span class="parlay-tag">Parlay ${i+1} &bull; ${par.n_legs} Legs</span>
          <span class="parlay-payout">${par.payout}</span>
        </div>
        <div class="parlay-conf">${par.combined}%<span>combined confidence</span></div>
        ${evHtml}
        <div class="parlay-legs" style="margin-top:12px">${legsHtml}</div>
      </div>`;
  });
}

// ── Render Games ─────────────────────────────────────────────────────────────
function parkClass(tag){
  if(tag.includes("Hitter")) return "park-hitter";
  if(tag.includes("Pitcher")) return "park-pitcher";
  return "park-neutral";
}

function weatherDisplay(g){
  if(g.has_roof) return {text:"Retractable Roof / Dome", color:"var(--sub)", icon:"🏟️"};
  const flag = g.weather_flag;
  const temp = g.temp_f + "°F";
  const precip = g.precip_prob > 10 ? ` | ${g.precip_prob}% precip` : "";
  if(flag==="WIND_OUT"){
    const mph = Math.abs(g.wind_component).toFixed(1);
    return {text:`${temp} | Wind ${mph} mph blowing OUT (+${(g.wind_component*0.04*0.5).toFixed(2)} runs)${precip}`, color:"#ef9a9a", icon:"💨"};
  }
  if(flag==="WIND_IN"){
    const mph = Math.abs(g.wind_component).toFixed(1);
    return {text:`${temp} | Wind ${mph} mph blowing IN (suppresses scoring)${precip}`, color:"#90caf9", icon:"🌬️"};
  }
  if(flag==="COLD"){
    return {text:`${temp} — Cold conditions suppress scoring${precip}`, color:"#90caf9", icon:"🥶"};
  }
  if(flag==="PRECIP"){
    return {text:`${temp} | ${g.precip_prob}% chance of rain`, color:"#ffcc80", icon:"🌧️"};
  }
  // NORMAL
  if(g.wind_speed > 5){
    const dir = g.wind_label || "";
    return {text:`${temp} | Wind ${g.wind_speed} mph ${dir}`, color:"var(--sub)", icon:"⛅"};
  }
  return {text:`${temp} | Clear conditions`, color:"var(--sub)", icon:"☀️"};
}

function renderGames(){
  const grid = document.getElementById("gamesGrid");
  grid.innerHTML = "";
  DATA_GAMES.forEach(g=>{
    const homeW    = g.home_wp, awayW = g.away_wp;
    const totalDir = g.exp_total > g.total_line ? "↑ OVER" : "↓ UNDER";
    const totalCol = g.exp_total > g.total_line ? "color:var(--red)" : "color:var(--blue)";
    const wx       = weatherDisplay(g);

    // Lineup OPS display
    const lineupRow = g.lineup_confirmed
      ? `<div class="game-row"><span class="row-label">Lineup OPS</span>
           <span class="row-val" style="color:var(--green)">
             ${g.away} <b>${g.away_lineup_ops||"—"}</b> &nbsp;|&nbsp;
             ${g.home} <b>${g.home_lineup_ops||"—"}</b>
             <span style="color:var(--sub);font-size:.68rem"> (confirmed)</span>
           </span></div>`
      : "";

    grid.innerHTML += `
      <div class="game-card">
        <div class="game-header">
          <span class="matchup">${g.away} @ ${g.home}</span>
          <span class="game-time">${g.time}</span>
        </div>
        <div class="game-body">
          <div class="game-row">
            <span class="row-label">Park</span>
            <span class="row-val">${g.venue.length>30?g.venue.slice(0,28)+"…":g.venue}
              <span class="park-badge ${parkClass(g.park_tag)}">${g.park_tag} (${g.park_runs})</span>
            </span>
          </div>
          <div class="game-row">
            <span class="row-label">Weather</span>
            <span class="row-val" style="color:${wx.color}">${wx.icon} ${wx.text}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Away SP</span>
            <span class="row-val">${g.away_sp} &bull; ERA ${g.away_era} / FIP ${g.away_fip}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Home SP</span>
            <span class="row-val">${g.home_sp} &bull; ERA ${g.home_era} / FIP ${g.home_fip}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Bullpen</span>
            <span class="row-val">${g.away} BP ERA ${g.away_bp_era} &nbsp;|&nbsp; ${g.home} BP ERA ${g.home_bp_era}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Offense</span>
            <span class="row-val">${g.away} ${g.away_rpg} RPG &nbsp;|&nbsp; ${g.home} ${g.home_rpg} RPG</span>
          </div>
          ${lineupRow}
          <div class="game-row">
            <span class="row-label">Projected</span>
            <span class="row-val">${g.away} <b>${g.exp_away}</b> &nbsp;|&nbsp; ${g.home} <b>${g.exp_home}</b></span>
          </div>
          <div class="game-row">
            <span class="row-label">Total</span>
            <span class="row-val">
              <b style="${totalCol}">${totalDir}</b> &nbsp;
              Model ${g.exp_total} vs ${g.total_line} line &nbsp;
              <span style="color:var(--sub)">(${g.total_conf}% conf)</span>
            </span>
          </div>
          ${g.rl_team ? `<div class="game-row"><span class="row-label">Run Line</span>
            <span class="row-val">${g.rl_pick} &nbsp;<span style="color:var(--sub)">(${g.rl_conf}% conf)</span></span></div>` : ""}
          <div style="margin-top:6px">
            <div style="display:flex;justify-content:space-between;font-size:.76rem;color:var(--sub);margin-bottom:4px">
              <span>${g.away} ${g.away_wp}%</span><span>${g.home} ${g.home_wp}%</span>
            </div>
            <div style="display:flex;height:8px;border-radius:4px;overflow:hidden;background:rgba(255,255,255,.06)">
              <div style="width:${g.away_wp}%;background:var(--blue)"></div>
              <div style="width:${g.home_wp}%;background:var(--green)"></div>
            </div>
            <div style="display:flex;justify-content:space-between;font-size:.68rem;color:var(--sub);margin-top:3px">
              <span>Away</span><span>Home</span>
            </div>
          </div>
        </div>
      </div>`;
  });
}

// ── Section Nav ───────────────────────────────────────────────────────────────
document.querySelectorAll(".section-nav-btn").forEach(btn=>{
  btn.addEventListener("click",()=>{
    document.querySelectorAll(".section-nav-btn").forEach(b=>b.classList.remove("active"));
    document.querySelectorAll(".section-panel").forEach(p=>p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(btn.dataset.panel).classList.add("active");
  });
});

// ── Render Today's Games ──────────────────────────────────────────────────────
function localGameTime(utcStr){
  if(!utcStr) return "TBD";
  try{
    const d = new Date(utcStr.endsWith("Z") ? utcStr : utcStr + "Z");
    return d.toLocaleTimeString("en-US", {
      hour:"numeric", minute:"2-digit",
      timeZone:"America/New_York", timeZoneName:"short"
    });
  } catch(e){ return utcStr; }
}

// ── TEAMS TAB ─────────────────────────────────────────────────────────────────
// Alphabetical — Athletics listed by nickname since they moved to Sacramento
const ALL_TEAMS = [
  "Arizona Diamondbacks","Athletics","Atlanta Braves","Baltimore Orioles",
  "Boston Red Sox","Chicago Cubs","Chicago White Sox","Cincinnati Reds",
  "Cleveland Guardians","Colorado Rockies","Detroit Tigers","Houston Astros",
  "Kansas City Royals","Los Angeles Angels","Los Angeles Dodgers","Miami Marlins",
  "Milwaukee Brewers","Minnesota Twins","New York Mets","New York Yankees",
  "Philadelphia Phillies","Pittsburgh Pirates","San Diego Padres","San Francisco Giants",
  "Seattle Mariners","St. Louis Cardinals","Tampa Bay Rays","Texas Rangers",
  "Toronto Blue Jays","Washington Nationals"
];

// Unambiguous short labels — no duplicate "Sox", "Angels", etc.
const TEAM_SHORT = {
  "Arizona Diamondbacks":  "ARI D-backs",
  "Athletics":             "ATH Athletics",
  "Atlanta Braves":        "ATL Braves",
  "Baltimore Orioles":     "BAL Orioles",
  "Boston Red Sox":        "BOS Red Sox",
  "Chicago Cubs":          "CHC Cubs",
  "Chicago White Sox":     "CWS White Sox",
  "Cincinnati Reds":       "CIN Reds",
  "Cleveland Guardians":   "CLE Guards",
  "Colorado Rockies":      "COL Rockies",
  "Detroit Tigers":        "DET Tigers",
  "Houston Astros":        "HOU Astros",
  "Kansas City Royals":    "KC Royals",
  "Los Angeles Angels":    "LAA Angels",
  "Los Angeles Dodgers":   "LAD Dodgers",
  "Miami Marlins":         "MIA Marlins",
  "Milwaukee Brewers":     "MIL Brewers",
  "Minnesota Twins":       "MIN Twins",
  "New York Mets":         "NYM Mets",
  "New York Yankees":      "NYY Yankees",
  "Philadelphia Phillies": "PHI Phillies",
  "Pittsburgh Pirates":    "PIT Pirates",
  "San Diego Padres":      "SD Padres",
  "San Francisco Giants":  "SF Giants",
  "Seattle Mariners":      "SEA Mariners",
  "St. Louis Cardinals":   "STL Cards",
  "Tampa Bay Rays":        "TB Rays",
  "Texas Rangers":         "TEX Rangers",
  "Toronto Blue Jays":     "TOR Blue Jays",
  "Washington Nationals":  "WSH Nats",
};

let activeTeam = null;
let teamPropTypeFilter = "all";

function initTeamsTab(){
  const grid = document.getElementById("teamButtonGrid");
  ALL_TEAMS.forEach(team => {
    const hasGame = !!DATA_TEAM_SCHED[team];
    const isToday = hasGame && DATA_TEAM_SCHED[team].is_today;
    const btn = document.createElement("button");
    btn.className = "team-btn" + (isToday ? " playing" : "");
    btn.textContent = teamShort(team);
    btn.title = team + (hasGame ? " — " + DATA_TEAM_SCHED[team].day_label : " — No upcoming game found");
    btn.addEventListener("click", () => {
      document.querySelectorAll(".team-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      activeTeam = team;
      teamPropTypeFilter = "all";
      renderTeamView(team);
    });
    grid.appendChild(btn);
  });
}

function teamShort(full){
  return TEAM_SHORT[full] || full.split(" ").pop();
}

function renderTeamView(team){
  const view = document.getElementById("teamGameView");
  const game = DATA_TEAM_SCHED[team];

  if(!game){
    view.innerHTML = `<div class="team-game-card" style="text-align:center;color:var(--sub);padding:40px">
      No upcoming game found for ${team} in the next 7 days.</div>`;
    return;
  }

  const away = game.away_team;
  const home = game.home_team;
  const isAway = away === team;
  const opp   = isAway ? home : away;

  // Find pick for this game
  const gamePicks = DATA_PICKS.filter(p =>
    (p.away === away && p.home === home) ||
    (p.game && p.game.includes(away) && p.game.includes(home))
  );
  const mlPick    = gamePicks.find(p => p.type === "ML");
  const totalPick = gamePicks.find(p => p.type === "TOTAL");

  // Look up raw win probabilities from DATA_GAMES (available for ALL games)
  const gameData = DATA_GAMES.find(g => g.away === away && g.home === home);

  // Prediction bars — use DATA_GAMES wp for all games, overlay pick tier if one exists
  let awayConf = gameData ? gameData.away_wp : 50;
  let homeConf = gameData ? gameData.home_wp : 50;
  let awayPick = "", homePick = "";
  if(mlPick){
    const pickedHome = mlPick.label && mlPick.label.includes(home.split(" ").pop());
    if(pickedHome){
      homePick = `✓ Model Pick (${mlPick.tier})`;
    } else {
      awayPick = `✓ Model Pick (${mlPick.tier})`;
    }
  }

  const favColor = c => c >= 68 ? "var(--red)" : c >= 58 ? "#ffb74d" : "var(--green)";
  const isToday = game.is_today;

  // Props for this game — both teams
  const gameProps = DATA_PROPS.filter(p =>
    (p.away_team === away && p.home_team === home) ||
    (p.game && p.game.includes(away) && p.game.includes(home))
  );
  // Filter by side field — each prop knows which lineup it came from
  const awayProps = gameProps.filter(p => p.side === "away");
  const homeProps = gameProps.filter(p => p.side === "home");
  // If side data missing fall back to show all under clicked team only
  const clickedIsAway = team === away;
  const primaryProps  = clickedIsAway
    ? (awayProps.length ? awayProps : gameProps)
    : (homeProps.length ? homeProps : gameProps);
  const otherProps    = clickedIsAway
    ? (homeProps.length ? homeProps : [])
    : (awayProps.length ? awayProps : []);
  const primaryLabel  = clickedIsAway ? away : home;
  const otherLabel    = clickedIsAway ? home : away;

  const propTypes = [...new Set(gameProps.map(p => p.prop_type))].sort();
  const typeFilterHtml = propTypes.length > 1 ? `
    <div class="prop-type-filters" id="teamPropFilters">
      <button class="ptype-btn active" data-ptype="all">All</button>
      ${propTypes.map(t => `<button class="ptype-btn" data-ptype="${t}">${propIcon(t)} ${t}</button>`).join("")}
    </div>` : "";

  function miniProps(props){
    const filtered = teamPropTypeFilter === "all" ? props : props.filter(p => p.prop_type === teamPropTypeFilter);
    if(!filtered.length) return `<div class="team-no-props">No props available yet</div>`;
    return filtered.map(p => {
      const barPct = p.prop_type === "HR" ? Math.min(100, p.conf * 5)
        : p.prop_type === "SB" ? Math.min(100, p.conf * 3.57)
        : p.prop_type === "RBI" || p.prop_type === "R" ? Math.min(100, Math.max(0,(p.conf-30)*1.43))
        : p.prop_type === "TB" ? Math.min(100, Math.max(0,(p.conf-40)*1.67))
        : Math.min(100, Math.max(0,(p.conf-50)*2));
      const tc = p.tier==="LOCK"?"var(--green)":p.tier==="STRONG"?"var(--blue)":"#ffb74d";
      const projTag = p.projected ? `<span style="font-size:.6rem;background:rgba(255,183,77,.18);
        color:#ffb74d;border:1px solid rgba(255,183,77,.35);border-radius:3px;padding:1px 4px;
        margin-left:4px;letter-spacing:.03em">PROJ</span>` : "";
      return `<div class="team-prop-mini" style="${p.projected ? 'border:1px dashed rgba(255,183,77,.25)' : ''}">
        <div class="team-prop-mini-top">
          <span class="team-prop-mini-name">${p.player_name}${projTag}</span>
          <span class="team-prop-mini-type badge-${p.prop_type}">${propIcon(p.prop_type)} ${p.prop_type}</span>
        </div>
        <div class="team-prop-mini-line">${propLabel(p.prop_type, p.line)}</div>
        <div style="display:flex;align-items:center;gap:8px">
          <div style="flex:1;height:5px;background:rgba(255,255,255,.08);border-radius:3px">
            <div style="width:${barPct}%;height:5px;border-radius:3px;background:${tc}"></div>
          </div>
          <span class="team-prop-mini-conf" style="color:${tc}">${p.conf}%</span>
          <span style="font-size:.68rem;color:var(--sub)">${p.tier}</span>
        </div>
      </div>`;
    }).join("");
  }

  // Projected props: gameProps exist but are all tagged projected
  const allProjected = gameProps.length > 0 && gameProps.every(p => p.projected);

  const noLineupMsg = !isToday ? `
    <div style="background:rgba(66,165,245,.07);border:1px solid rgba(66,165,245,.2);border-radius:8px;
      padding:12px 16px;font-size:.82rem;color:#90caf9;margin-bottom:14px">
      📅 Next game: ${game.day_label} — full prop analysis runs on game day once lineups confirm.
    </div>` :
    allProjected ? `
    <div style="background:rgba(255,183,77,.07);border:1px solid rgba(255,183,77,.25);border-radius:8px;
      padding:12px 16px;margin-bottom:14px;display:flex;align-items:flex-start;gap:10px">
      <div style="font-size:1.1rem;flex-shrink:0">📋</div>
      <div>
        <div style="font-size:.85rem;font-weight:700;color:#ffb74d;margin-bottom:4px">
          Projected Props — Official Lineup Pending
        </div>
        <div style="font-size:.78rem;color:var(--sub);line-height:1.6">
          These props are based on the most recent confirmed batting order. If today's
          lineup matches yesterday's, the numbers stay the same. Cards marked
          <span style="background:rgba(255,183,77,.18);color:#ffb74d;border:1px solid rgba(255,183,77,.35);
            border-radius:3px;padding:1px 5px;font-size:.68rem">PROJ</span>
          will automatically update when the official order posts.
        </div>
      </div>
    </div>` :
    (gameProps.length === 0 ? (() => {
      // Try to show projected lineup from most recent confirmed order
      const projAway = DATA_PROJ_LINEUPS[away];
      const projHome = DATA_PROJ_LINEUPS[home];
      const proj     = projAway || projHome;
      const projTeam = projAway ? away : home;

      const projSection = proj ? `
        <div style="margin-top:14px">
          <div style="font-size:.78rem;font-weight:700;color:#90caf9;margin-bottom:8px;letter-spacing:.04em;text-transform:uppercase">
            📋 Projected Lineup — ${projTeam}
            <span style="font-weight:400;color:var(--sub);margin-left:6px;text-transform:none">Based on ${proj.date} · Updates when official order posts</span>
          </div>
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:6px">
            ${proj.players.map(p => `
              <div style="background:rgba(66,165,245,.07);border:1px solid rgba(66,165,245,.15);border-radius:6px;
                padding:6px 10px;font-size:.78rem;display:flex;align-items:center;gap:8px">
                <span style="color:var(--sub);font-size:.7rem;min-width:14px;text-align:right">${p.order}</span>
                <span style="color:var(--text)">${p.name}</span>
                ${p.pos ? `<span style="color:var(--sub);font-size:.7rem;margin-left:auto">${p.pos}</span>` : ""}
              </div>`).join("")}
          </div>
        </div>` : "";

      return `
      <div style="background:rgba(255,152,0,.08);border:1px solid rgba(255,152,0,.2);border-radius:8px;
        padding:12px 16px;margin-bottom:14px">
        <div style="font-size:.85rem;font-weight:700;color:#ffb74d;margin-bottom:6px">⏳ Batting Order Not Yet Posted</div>
        <div style="font-size:.78rem;color:var(--sub);line-height:1.6">
          Probable starters are shown above. Teams typically post their batting order
          2–3 hours before first pitch. Player props will populate automatically once
          the lineup is confirmed — no refresh needed.
        </div>
        ${projSection}
      </div>`;
    })() : "");

  view.innerHTML = `
    <div class="team-game-card">
      <!-- Matchup header -->
      <div class="team-matchup-header">
        <div class="team-matchup-teams">
          <div class="team-name-block">
            <div class="team-name-big">${away}</div>
            <div class="team-name-sp">SP: ${game.away_sp}</div>
          </div>
          <div class="team-vs">@</div>
          <div class="team-name-block">
            <div class="team-name-big">${home}</div>
            <div class="team-name-sp">SP: ${game.home_sp}</div>
          </div>
        </div>
        <div class="team-game-meta">
          <span class="team-day-label">${game.day_label}</span>
          ${game.time_str}<br>
          ${game.venue || ""}
        </div>
      </div>

      <!-- Prediction bars (today's games only — always shown when DATA_GAMES has the game) -->
      ${isToday && gameData ? `
      <div class="team-pred-row">
        <div class="team-pred-box">
          <div class="team-pred-label">${away}</div>
          <div class="team-pred-conf" style="color:${favColor(awayConf)}">${awayConf}%</div>
          <div class="team-pred-bar"><div class="team-pred-fill" style="width:${awayConf}%;background:${favColor(awayConf)}"></div></div>
          <div class="team-pred-pick" style="color:${favColor(awayConf)}">${awayPick}</div>
        </div>
        <div class="team-pred-box">
          <div class="team-pred-label">${home}</div>
          <div class="team-pred-conf" style="color:${favColor(homeConf)}">${homeConf}%</div>
          <div class="team-pred-bar"><div class="team-pred-fill" style="width:${homeConf}%;background:${favColor(homeConf)}"></div></div>
          <div class="team-pred-pick" style="color:${favColor(homeConf)}">${homePick}</div>
        </div>
      </div>
      ${totalPick ? `<div style="text-align:center;font-size:.82rem;color:var(--sub);margin:-4px 0 14px">
        📊 Total: ${totalPick.label} — proj ${totalPick.exp_total || "?"} runs
        <span class="tier-badge tb-${totalPick.tier}" style="margin-left:8px">${totalPick.tier}</span>
      </div>` : ""}` :
      (isToday ? `<div style="color:var(--sub);font-size:.85rem;padding:10px 0 14px;text-align:center">
        Prediction not available for this game.</div>` :
      `<div style="background:rgba(0,230,118,.05);border:1px solid rgba(0,230,118,.15);border-radius:8px;
        padding:12px 16px;font-size:.82rem;color:var(--sub);margin-bottom:14px">
        📅 Full model prediction runs on game day.</div>`)}

      <!-- Props -->
      <div class="team-props-section">
        ${noLineupMsg}
        ${typeFilterHtml}
        <div id="teamPropsContent">
          <div class="team-props-header">${primaryLabel} — Player Props</div>
          <div class="team-props-grid" id="awayPropsGrid">${miniProps(primaryProps)}</div>
          ${otherProps.length ? `
          <div class="team-props-header" style="margin-top:16px">${otherLabel} — Player Props</div>
          <div class="team-props-grid" id="homePropsGrid">${miniProps(otherProps)}</div>` : ""}
        </div>
      </div>
    </div>`;

  // Prop type filter buttons
  document.querySelectorAll("#teamPropFilters .ptype-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("#teamPropFilters .ptype-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      teamPropTypeFilter = btn.dataset.ptype;
      document.getElementById("awayPropsGrid").innerHTML = miniProps(primaryProps);
      const hpg = document.getElementById("homePropsGrid");
      if(hpg) hpg.innerHTML = miniProps(otherProps);
    });
  });
}

// Auto-select a team playing today when tab is opened
document.querySelectorAll(".section-nav-btn[data-panel='panel-teams']").forEach(btn => {
  btn.addEventListener("click", () => {
    if(!activeTeam){
      // Pick first team with a game today
      const todayTeam = ALL_TEAMS.find(t => DATA_TEAM_SCHED[t] && DATA_TEAM_SCHED[t].is_today);
      if(todayTeam){
        activeTeam = todayTeam;
        const teamBtns = document.querySelectorAll(".team-btn");
        teamBtns.forEach((b,i) => { if(ALL_TEAMS[i] === todayTeam) b.classList.add("active"); });
        renderTeamView(todayTeam);
      }
    }
  });
});

function renderSchedule(){
  const grid = document.getElementById("scheduleGrid");
  grid.innerHTML = "";
  if(!DATA_SCHEDULE || DATA_SCHEDULE.length === 0){
    grid.innerHTML = `<div class="empty">No games found for today.</div>`;
    return;
  }
  DATA_SCHEDULE.forEach(g=>{
    const isLive    = g.status.startsWith("Live");
    const isFinal   = g.status === "Final";
    const isUpcoming= g.status === "Upcoming";

    const statusClass = isLive ? "status-live" : isFinal ? "status-final" : "status-upcoming";
    const statusLabel = isLive
      ? `<span class="live-dot"></span> LIVE &bull; ${g.status.replace("Live — ","")}`
      : isFinal ? "⚾ Final" : `⏰ ${localGameTime(g.game_time_utc)}`;

    // Score display
    let awayScoreHtml = "", homeScoreHtml = "", vsHtml = "";
    if(isFinal || isLive){
      const awayW = parseInt(g.away_score) > parseInt(g.home_score);
      const homeW = parseInt(g.home_score) > parseInt(g.away_score);
      awayScoreHtml = `<div class="sched-score${awayW?' winner':''}">${g.away_score}</div>`;
      homeScoreHtml = `<div class="sched-score${homeW?' winner':''}">${g.home_score}</div>`;
      vsHtml        = `<div class="sched-vs">-</div>`;
    } else {
      vsHtml = `<div><div class="sched-vs">@</div><div class="sched-time">${localGameTime(g.game_time_utc)}</div></div>`;
    }

    const awayStreak = g.away_streak ? ` &bull; ${g.away_streak}` : "";
    const homeStreak = g.home_streak ? ` &bull; ${g.home_streak}` : "";
    const awayL10    = g.away_last10 ? ` (L10: ${g.away_last10})` : "";
    const homeL10    = g.home_last10 ? ` (L10: ${g.home_last10})` : "";

    const venueTrunc = g.venue.length > 28 ? g.venue.slice(0,26)+"…" : g.venue;

    grid.innerHTML += `
      <div class="sched-card">
        <div class="sched-status-bar ${statusClass}">${statusLabel}</div>
        <div class="sched-matchup">
          <div class="sched-team-row">
            <div class="sched-team-info">
              <div class="sched-team-name">${g.away_team}</div>
              <div class="sched-team-record">${g.away_record}${awayStreak}</div>
              ${awayL10 ? `<div class="sched-team-streak">${awayL10}</div>` : ""}
              <div class="sched-at-label">Away</div>
            </div>
            <div class="sched-score-block">
              ${awayScoreHtml}
              ${vsHtml}
              ${homeScoreHtml}
            </div>
            <div class="sched-team-info" style="text-align:right;align-items:flex-end">
              <div class="sched-team-name">${g.home_team}</div>
              <div class="sched-team-record">${g.home_record}${homeStreak}</div>
              ${homeL10 ? `<div class="sched-team-streak">${homeL10}</div>` : ""}
              <div class="sched-at-label">Home</div>
            </div>
          </div>
        </div>
        <div class="sched-divider"></div>
        <div class="sched-pitchers">
          <span>SP: ${g.away_sp||"TBD"}</span>
          <span>SP: ${g.home_sp||"TBD"}</span>
        </div>
        <div class="sched-divider"></div>
        <div class="sched-footer">
          <span class="sched-time">${isFinal||isLive ? g.status : localGameTime(g.game_time_utc)}</span>
          <span class="sched-venue">${venueTrunc}</span>
        </div>
      </div>`;
  });
}

// ── Render Props ──────────────────────────────────────────────────────────────
let propFilterType = "all", propFilterTier = "all";

function propIcon(t){
  const icons = {HR:"⚡",HITS:"🎯",TB:"💥",RBI:"🏅",R:"🏃",SB:"💨",K:"🔥"};
  return icons[t] || "📊";
}
function propLabel(t,line){
  if(t==="HR")   return `HR Over ${line}`;
  if(t==="HITS") return `Hits Over ${line}`;
  if(t==="TB")   return `Total Bases Over ${line}`;
  if(t==="RBI")  return `RBIs Over ${line}`;
  if(t==="R")    return `Runs Scored Over ${line}`;
  if(t==="SB")   return `Stolen Bases Over ${line}`;
  if(t==="K")    return `Ks Over ${line}`;
  return `Over ${line}`;
}

function renderProps(){
  const grid = document.getElementById("propsGrid");
  grid.innerHTML = "";
  let visible = 0;
  DATA_PROPS.forEach(p=>{
    const show = (propFilterType==="all" || p.prop_type===propFilterType)
              && (propFilterTier==="all" || p.tier===propFilterTier);
    if(!show) return;
    visible++;
    const label    = propLabel(p.prop_type, p.line);
    const overUnder= p.proj >= p.line ? "OVER" : "UNDER";
    const projColor= overUnder==="OVER" ? "var(--green)" : "var(--blue)";
    const orderStr = p.batting_order ? ` &bull; Bats ${p.batting_order}` : "";
    const sideStr  = p.side==="pitcher" ? " (SP)" : (p.side ? ` (${p.side})` : "");

    // Edge tier badge (always shown)
    const edgeLabel = p.fav_tier==="HEAVY"  ? "🔴 Strong Edge"
                    : p.fav_tier==="MEDIUM" ? "🟡 Solid Edge"
                    : "🟢 Slight Edge";
    const propFavHtml = `<div class="fav-row">
      <span class="fav-label">Edge:</span>
      <span class="fav-badge fav-${p.fav_tier||'NEUTRAL'}">${edgeLabel}</span>
    </div>`;

    // Warning badges — always shown, lit when active
    const pwb = (cls, active, label) =>
      `<span class="warn-badge ${cls} ${active ? 'active' : 'inactive'}">${label}</span>`;
    const propWarnHtml = `<div class="warn-row">
      ${pwb('warn-tbd',    p.tbd_sp,      '⚠ TBD SP')}
      ${pwb('warn-lineup', p.unconfirmed, 'Lineups TBD')}
    </div>`;

    // Confidence bar — each prop type scaled to its realistic ceiling
    let propBarPct;
    if(p.prop_type === "HR"){
      propBarPct = Math.min(100, p.conf * 5);      // 20% max → 100%
    } else if(p.prop_type === "SB"){
      propBarPct = Math.min(100, p.conf * 3.57);   // 28% max → 100%
    } else if(p.prop_type === "RBI" || p.prop_type === "R"){
      propBarPct = Math.min(100, Math.max(0, (p.conf - 30) * 1.43)); // 30-100% range
    } else if(p.prop_type === "TB"){
      propBarPct = Math.min(100, Math.max(0, (p.conf - 40) * 1.67)); // 40-100% range
    } else {
      propBarPct = Math.min(100, Math.max(0, (p.conf - 50) * 2));    // HITS/K: 50-100%
    }

    const projBanner = p.projected ? `<div style="background:rgba(255,183,77,.1);border-bottom:1px solid rgba(255,183,77,.2);
      padding:4px 10px;font-size:.68rem;color:#ffb74d;letter-spacing:.04em">
      📋 PROJECTED — based on last confirmed lineup</div>` : "";
    grid.innerHTML += `
      <div class="prop-card tier-${p.tier}" style="${p.projected ? 'border:1px dashed rgba(255,183,77,.3)' : ''}">
        ${projBanner}
        <div class="prop-top">
          <span class="prop-type-badge badge-${p.prop_type}">${propIcon(p.prop_type)} ${p.prop_type}</span>
          <span class="tier-badge tb-${p.tier}">${tierIcon(p.tier)} ${p.tier}</span>
        </div>
        <div class="prop-player">${p.player_name}${sideStr}</div>
        <div class="prop-game">${p.game}${orderStr}</div>
        <div class="prop-line-row">
          <div>
            <div class="prop-line-label">Line</div>
            <div class="prop-line-val">${label}</div>
          </div>
          <div style="text-align:right">
            <div class="prop-line-label">Projection</div>
            <div class="prop-proj-val" style="color:${projColor}">${p.proj} (${overUnder})</div>
          </div>
        </div>
        <div class="conf-row">
          <div class="conf-bar-wrap">
            <div class="conf-bar bar-${p.tier}" style="width:${propBarPct}%"></div>
          </div>
          <span class="conf-pct pct-${p.tier}">${p.conf}%</span>
        </div>
        ${propFavHtml}
        ${propWarnHtml}
        <div class="prop-reasoning">${p.reasoning}</div>
      </div>`;
  });
  document.getElementById("propResults").innerHTML =
    `Showing <b>${visible}</b> of <b>${DATA_PROPS.length}</b> props`;
  if(!visible){
    const msg = DATA_PROPS.length===0
      ? "Player props require confirmed lineups — check back closer to game time."
      : "No props match the current filters.";
    grid.innerHTML = `<div class="empty">${msg}</div>`;
  }
}

// ── Filter Handlers ───────────────────────────────────────────────────────────
document.querySelectorAll(".filter-btn[data-pgroup]").forEach(btn=>{
  btn.addEventListener("click",()=>{
    const group = btn.dataset.pgroup, val = btn.dataset.pval;
    document.querySelectorAll(`.filter-btn[data-pgroup="${group}"]`).forEach(b=>{
      b.classList.remove("active","active-gold","active-blue");
    });
    if(val==="LOCK") btn.classList.add("active-gold");
    else if(val==="STRONG") btn.classList.add("active-blue");
    else btn.classList.add("active");
    if(group==="ptype") propFilterType = val;
    else                propFilterTier = val;
    renderProps();
  });
});

document.querySelectorAll(".filter-btn[data-group]").forEach(btn=>{
  btn.addEventListener("click",()=>{
    const group = btn.dataset.group, val = btn.dataset.val;
    document.querySelectorAll(`.filter-btn[data-group="${group}"]`).forEach(b=>{
      b.classList.remove("active","active-gold","active-blue");
    });
    if(val==="LOCK") btn.classList.add("active-gold");
    else if(val==="STRONG") btn.classList.add("active-blue");
    else btn.classList.add("active");
    if(group==="type") filterType = val;
    else               filterTier = val;
    renderPicks();
  });
});

document.getElementById("teamSearch").addEventListener("input", e=>{
  filterTeam = e.target.value.trim().toLowerCase();
  renderPicks();
});

document.querySelectorAll(".section-tab[data-parlay]").forEach(tab=>{
  tab.addEventListener("click",()=>{
    document.querySelectorAll(".section-tab").forEach(t=>t.classList.remove("active"));
    tab.classList.add("active");
    showParlay = parseInt(tab.dataset.parlay);
    renderParlays();
  });
});

// ── Refresh Button (only shown when served locally) ───────────────────────────
const LOCAL_API = "http://localhost:8765";
const isLocal   = location.hostname === "localhost" || location.hostname === "127.0.0.1";

if(isLocal){
  document.getElementById("refreshBtn").style.display = "inline-flex";
}

async function doRefresh(){
  const btn    = document.getElementById("refreshBtn");
  const icon   = document.getElementById("refreshIcon");
  const status = document.getElementById("refreshStatus");

  btn.disabled = true;
  icon.classList.add("spin");
  status.style.color = "var(--sub)";
  status.textContent = "Running pipeline… this takes about 30 seconds";

  try{
    const res = await fetch(LOCAL_API + "/refresh", {method:"GET"});
    if(res.ok){
      status.style.color = "var(--green)";
      status.textContent = "✓ Done! Reloading picks...";
      setTimeout(()=> location.reload(), 1200);
    } else {
      const msg = await res.text();
      status.style.color = "var(--red)";
      status.textContent = "Error: " + msg.slice(0,120);
      btn.disabled = false;
      icon.classList.remove("spin");
    }
  } catch(e){
    status.style.color = "#ef9a9a";
    status.textContent = "Could not reach local server. Is serve_picks.py running?";
    btn.disabled = false;
    icon.classList.remove("spin");
  }
}

// ── Scores Ticker ─────────────────────────────────────────────────────────────
function renderTicker(){
  // Live scores are fetched directly from MLB Stats API in the browser
  // so they update in real time without needing the server to regenerate HTML
  const track = document.getElementById("tickerTrack");

  function cityName(fullName) {
    return fullName ? fullName.split(" ").slice(0,-1).join(" ") || fullName : fullName;
  }

  function buildTickerHTML(games) {
    if (!games || games.length === 0) {
      return `<span class="ticker-empty">No games in progress yet today — check back later</span>`;
    }
    let html = "";
    for (let pass = 0; pass < 2; pass++) {
      games.forEach(g => {
        const awayScore = g.away_score;
        const homeScore = g.home_score;
        const awayWon   = awayScore > homeScore;
        const isLive    = g.is_live;
        const liveStyle = isLive ? "color:#ff6b35" : "";
        const liveClass = isLive ? "ticker-live" : "ticker-final";
        html += `
          <div class="ticker-item">
            <span class="${awayWon ? 'ticker-score win' : 'ticker-score loss'}">${g.away_city} ${awayScore}</span>
            <span style="color:var(--sub)">@</span>
            <span class="${!awayWon ? 'ticker-score win' : 'ticker-score loss'}">${g.home_city} ${homeScore}</span>
            <span class="${liveClass}" style="${liveStyle}">${g.label}</span>
          </div>`;
      });
    }
    return html;
  }

  async function fetchLiveScores() {
    const today = new Date().toLocaleDateString("en-CA"); // YYYY-MM-DD in local time
    const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${today}&hydrate=linescore&gameType=R`;
    try {
      const resp = await fetch(url);
      const data = await resp.json();
      const games = [];
      for (const dateEntry of (data.dates || [])) {
        for (const game of (dateEntry.games || [])) {
          const abstract = game?.status?.abstractGameState;
          if (abstract !== "Final" && abstract !== "Live") continue;
          const away      = game.teams.away;
          const home      = game.teams.home;
          const linescore = game.linescore || {};
          const inning    = linescore.currentInning || 9;
          const half      = linescore.inningHalf || "";
          const isLive    = abstract === "Live";
          let label       = isLive ? `${half.slice(0,3)} ${inning}` : "Final";
          if (!isLive && inning > 9) label = `F/${inning}`;
          games.push({
            away_city:  cityName(away.team.name),
            home_city:  cityName(home.team.name),
            away_score: away.score ?? 0,
            home_score: home.score ?? 0,
            is_live:    isLive,
            label:      label,
          });
        }
      }
      return games;
    } catch(e) {
      console.warn("Live scores fetch failed:", e);
      return null;
    }
  }

  async function refreshTicker() {
    const games = await fetchLiveScores();
    if (games !== null) {
      track.innerHTML = buildTickerHTML(games);
      const duration = Math.max(20, games.length * 5);
      track.style.animationDuration = duration + "s";
    }
  }

  // Initial load + refresh every 2 minutes
  refreshTicker();
  setInterval(refreshTicker, 2 * 60 * 1000);
}

// ── Force Odds Snapshot (AJAX — stays on page) ───────────────────────────────
async function forceOddsSnapshot(){
  const btn    = document.getElementById("forceOddsBtn");
  const status = document.getElementById("forceOddsStatus");
  if(!btn) return;

  btn.disabled = true;
  btn.textContent = "⏳ Requesting snapshot…";
  if(status) status.textContent = "";

  try{
    const res = await fetch("/force-odds");
    if(res.ok){
      let secs = 70;
      if(status){
        status.style.color = "var(--green)";
        status.textContent = `✓ Snapshot started — dashboard refreshes in ${secs}s`;
        const timer = setInterval(() => {
          secs--;
          if(secs <= 0){
            clearInterval(timer);
            location.reload(true);
          } else {
            status.textContent = `✓ Snapshot started — dashboard refreshes in ${secs}s`;
          }
        }, 1000);
      } else {
        setTimeout(() => location.reload(true), 70000);
      }
    } else {
      if(status){ status.style.color="var(--red)"; status.textContent="Request failed — try again in a moment."; }
      btn.disabled = false;
      btn.textContent = "🔄 Pull Odds Snapshot Now";
    }
  } catch(e){
    if(status){ status.style.color="var(--red)"; status.textContent="Could not reach server."; }
    btn.disabled = false;
    btn.textContent = "🔄 Pull Odds Snapshot Now";
  }
}

// ── Sharp Action Tab ──────────────────────────────────────────────────────────
function renderSharpAction(){
  const el = document.getElementById("sharpActionContent");
  if(!DATA_MOVEMENT || DATA_MOVEMENT.length === 0){
    el.innerHTML = `<div class="sharp-empty">
      <div style="font-size:2rem;margin-bottom:16px">📊</div>
      <div style="color:var(--text);font-size:1.1rem;font-weight:700;margin-bottom:8px">No Line Movement Data Yet</div>
      <div style="color:var(--sub);font-size:.88rem;max-width:420px;margin:0 auto;line-height:1.7">
        Sharp money tracking needs two odds snapshots to compare — the 6am pipeline takes the first one,
        then the server checks every 2 hours (8am–10pm ET).<br><br>
        If you're seeing this mid-day, hit the button below to pull a fresh snapshot right now.
        Line movement will appear on the next dashboard refresh (~60 seconds).
      </div>
      <div style="margin-top:24px">
        <button onclick="forceOddsSnapshot()" id="forceOddsBtn" style="display:inline-block;padding:10px 26px;
          background:linear-gradient(135deg,var(--green),#00b248);border-radius:24px;
          color:#000;font-weight:800;font-size:.88rem;border:none;cursor:pointer;font-family:inherit">
          🔄 Pull Odds Snapshot Now
        </button>
        <div id="forceOddsStatus" style="margin-top:12px;color:var(--sub);font-size:.82rem"></div>
      </div>
    </div>`;
    return;
  }

  const fmtO = o => (!o && o !== 0) ? "—" : (o > 0 ? `+${Math.round(o)}` : `${Math.round(o)}`);
  const fmtSnap = t => {
    if(!t) return "—";
    try{
      const d = new Date(t.endsWith("Z") ? t : t+"Z");
      return d.toLocaleTimeString("en-US",{hour:"numeric",minute:"2-digit",timeZone:"America/New_York"}) + " ET";
    } catch(e){ return t; }
  };

  let html = `<div class="sharp-explain-box">
    <strong style="color:var(--text)">What is sharp money?</strong>
    Professional bettors — called "sharps" — place large, well-researched bets.
    When a line moves significantly, it signals where pros are putting their money.
    Sportsbook signals: <span style="color:#ef5350;font-weight:700">STEAM</span> = 8+ point move,
    <span style="color:#ffa726;font-weight:700">DRIFT</span> = 3–7 points.
    Kalshi signals: <span style="color:#ef5350;font-weight:700">STEAM</span> = 5+ pp probability shift,
    <span style="color:#ffa726;font-weight:700">DRIFT</span> = 2–4 pp.
    <span style="color:#42a5f5;font-weight:700">Kalshi</span> is a regulated prediction market — it often moves before sportsbooks adjust.
    When both sources agree, that's the highest-conviction signal.
  </div>`;

  DATA_MOVEMENT.forEach(mv => {
    const awayNick = mv.away ? mv.away.split(" ").slice(-1)[0] : mv.away;
    const homeNick = mv.home ? mv.home.split(" ").slice(-1)[0] : mv.home;
    const mlSig    = mv.ml_signal    || "";
    const totSig   = mv.total_signal || "";
    const hasML    = mlSig === "STEAM" || mlSig === "DRIFT";
    const hasTot   = totSig === "STEAM" || totSig === "DRIFT";
    const dominant = (mlSig === "STEAM" || totSig === "STEAM") ? "STEAM" : "DRIFT";

    const sigBadge = dominant === "STEAM"
      ? `<span class="sharp-badge sharp-badge-steam">🔥 STEAM — Heavy Sharp Action</span>`
      : `<span class="sharp-badge sharp-badge-drift">📈 DRIFT — Notable Movement</span>`;

    // Moneyline odds comparison table
    let oddsHtml = "";
    if(hasML){
      const awayMove = mv.ml_away_move || 0;
      const homeMove = mv.ml_home_move || 0;
      const awayMoveColor = Math.abs(awayMove) >= 8 ? "#ef5350" : Math.abs(awayMove) >= 3 ? "#ffa726" : "var(--sub)";
      const homeMoveColor = Math.abs(homeMove) >= 8 ? "#ef5350" : Math.abs(homeMove) >= 3 ? "#ffa726" : "var(--sub)";
      const awayDir = awayMove > 0 ? "↑" : awayMove < 0 ? "↓" : "";
      const homeDir = homeMove > 0 ? "↑" : homeMove < 0 ? "↓" : "";
      oddsHtml = `<div class="sharp-odds-grid">
        <div class="sharp-odds-box">
          <div class="sharp-odds-team-lbl">${mv.away}</div>
          <div class="sharp-odds-row">
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Opening</div>
              <div class="sharp-odds-cell-num" style="color:var(--sub)">${fmtO(mv.ml_away_open)}</div>
            </div>
            <div class="sharp-odds-arrow">→</div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Current</div>
              <div class="sharp-odds-cell-num" style="color:var(--text)">${fmtO(mv.ml_away_now)}</div>
            </div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Move</div>
              <div class="sharp-odds-cell-num" style="color:${awayMoveColor}">${awayDir}${Math.abs(awayMove)}</div>
            </div>
          </div>
        </div>
        <div class="sharp-odds-box">
          <div class="sharp-odds-team-lbl">${mv.home}</div>
          <div class="sharp-odds-row">
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Opening</div>
              <div class="sharp-odds-cell-num" style="color:var(--sub)">${fmtO(mv.ml_home_open)}</div>
            </div>
            <div class="sharp-odds-arrow">→</div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Current</div>
              <div class="sharp-odds-cell-num" style="color:var(--text)">${fmtO(mv.ml_home_now)}</div>
            </div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Move</div>
              <div class="sharp-odds-cell-num" style="color:${homeMoveColor}">${homeDir}${Math.abs(homeMove)}</div>
            </div>
          </div>
        </div>
      </div>`;
    }

    // Total line movement row
    let totalHtml = "";
    if(hasTot){
      const tdir   = (mv.total_move || 0) > 0 ? "▲" : "▼";
      const tColor = totSig === "STEAM" ? "#ef5350" : "#ffa726";
      const tSide  = (mv.total_move || 0) > 0 ? "Over" : "Under";
      totalHtml = `<div class="sharp-total-row">
        <span style="color:var(--sub);font-weight:600">Over/Under</span>
        <span style="color:var(--sub)">${mv.total_open} → ${mv.total_now}</span>
        <span style="color:${tColor};font-weight:800">${tdir} ${Math.abs(mv.total_move||0).toFixed(1)} pts (${totSig})</span>
        <span style="color:var(--sub)">Heavy action on the ${tSide}</span>
      </div>`;
    }

    // ── Kalshi market signal panel ─────────────────────────────────────────
    let kalshiCardHtml = "";
    const kSig   = mv.kalshi_signal || "";
    const hasKal = kSig === "STEAM" || kSig === "DRIFT";
    if(hasKal){
      const kSharp    = mv.kalshi_sharp_side || "";
      const kNick     = kSharp ? kSharp.split(" ").slice(-1)[0] : "";
      const kMovePct  = mv.kalshi_away_move != null
        ? Math.round(Math.abs(mv.kalshi_away_move) * 100) : 0;
      const kSigColor = kSig === "STEAM" ? "#ef5350" : "#ffa726";
      const kLabel    = kSig === "STEAM" ? "🔥 STEAM" : "📈 DRIFT";

      const kAwayOpen = mv.kalshi_away_open != null ? Math.round(mv.kalshi_away_open * 100) + "%" : "—";
      const kAwayNow  = mv.kalshi_away_now  != null ? Math.round(mv.kalshi_away_now  * 100) + "%" : "—";
      const kHomeOpen = mv.kalshi_home_open != null ? Math.round(mv.kalshi_home_open * 100) + "%" : "—";
      const kHomeNow  = mv.kalshi_home_now  != null ? Math.round(mv.kalshi_home_now  * 100) + "%" : "—";

      // Kalshi vs sportsbook agreement
      const sbSharp   = mv.sharp_side || "";
      const sbNick    = sbSharp ? sbSharp.split(" ").slice(-1)[0].toLowerCase() : "";
      const kMatch    = kNick && sbNick && kNick.toLowerCase() === sbNick;
      const kMismatch = kNick && sbNick && kNick.toLowerCase() !== sbNick;
      const agreeLbl  = kMatch
        ? `<span style="background:rgba(105,240,174,.12);color:#69f0ae;border:1px solid rgba(105,240,174,.3);
             padding:2px 8px;border-radius:4px;font-size:.68rem;font-weight:700">✓ Confirms sportsbook</span>`
        : (kMismatch
          ? `<span style="background:rgba(255,167,38,.1);color:#ffa726;border:1px solid rgba(255,167,38,.3);
               padding:2px 8px;border-radius:4px;font-size:.68rem;font-weight:700">⚠ Diverges from sportsbook</span>`
          : "");

      kalshiCardHtml = `<div style="margin:12px 0;padding:12px;background:rgba(66,165,245,.07);
        border:1px solid rgba(66,165,245,.25);border-radius:8px">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap">
          <span style="color:#42a5f5;font-weight:800;font-size:.85rem">📊 Kalshi Prediction Market</span>
          <span style="color:${kSigColor};font-weight:800;font-size:.82rem">${kLabel}</span>
          ${agreeLbl}
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:.8rem">
          <div style="background:rgba(66,165,245,.08);border-radius:6px;padding:8px;text-align:center">
            <div style="color:var(--sub);font-size:.68rem;margin-bottom:4px">${mv.away}</div>
            <div style="color:var(--sub)">${kAwayOpen}</div>
            <div style="color:#42a5f5;font-size:.7rem">→</div>
            <div style="color:var(--text);font-weight:700">${kAwayNow}</div>
          </div>
          <div style="background:rgba(66,165,245,.08);border-radius:6px;padding:8px;text-align:center">
            <div style="color:var(--sub);font-size:.68rem;margin-bottom:4px">${mv.home}</div>
            <div style="color:var(--sub)">${kHomeOpen}</div>
            <div style="color:#42a5f5;font-size:.7rem">→</div>
            <div style="color:var(--text);font-weight:700">${kHomeNow}</div>
          </div>
        </div>
        ${kNick ? `<div style="margin-top:8px;color:var(--sub);font-size:.75rem">
          Market shifted ${kMovePct}pp toward <strong style="color:var(--text)">${kSharp}</strong>
          ${kSig === "STEAM" ? "— strong early signal before books adjust" : "— moderate market repricing"}
        </div>` : ""}
      </div>`;
    }

    // Bet call + model agreement
    const sharp = mv.sharp_side || "";
    let betCallHtml = "";
    if(sharp && hasML){
      const sharpNick  = sharp.split(" ").slice(-1)[0];
      const gamePick   = DATA_PICKS.find(p => p.away === mv.away && p.home === mv.home && (p.type==="ML"||p.type==="RL"));
      let modelNoteHtml = "";
      if(gamePick){
        const pickNick = gamePick.team ? gamePick.team.split(" ").slice(-1)[0] : "";
        const agrees   = sharpNick.toLowerCase() === pickNick.toLowerCase();
        modelNoteHtml  = agrees
          ? `<span style="color:#69f0ae;font-weight:700;font-size:.78rem">✓ Our ${gamePick.tier} pick agrees</span>`
          : `<span style="color:#ef9a9a;font-weight:700;font-size:.78rem">⚠ Our ${gamePick.tier} pick is ${gamePick.team} — use caution</span>`;
      }
      const urgency = dominant === "STEAM" ? "Strong Sharp Signal" : "Moderate Sharp Signal";
      betCallHtml = `<div class="sharp-bet-call">
        <div>
          <div class="sharp-bet-call-lbl">${urgency}</div>
          <div class="sharp-bet-call-text">Follow: Bet <span style="color:var(--green)">${sharp} ML</span></div>
        </div>
        ${modelNoteHtml}
      </div>`;
    } else if(hasTot){
      const tBetSide = (mv.total_move || 0) > 0 ? "Over" : "Under";
      const tLine    = mv.total_now || mv.total_open;
      betCallHtml = `<div class="sharp-bet-call">
        <div>
          <div class="sharp-bet-call-lbl">${dominant === "STEAM" ? "Strong Sharp Signal" : "Moderate Sharp Signal"}</div>
          <div class="sharp-bet-call-text">Follow: Bet <span style="color:var(--green)">${tBetSide} ${tLine}</span></div>
        </div>
      </div>`;
    }

    html += `<div class="sharp-action-card">
      <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:16px">
        <div>
          <div style="font-size:1.05rem;font-weight:800;color:var(--text)">${mv.away} @ ${mv.home}</div>
          <div style="font-size:.72rem;color:var(--sub);margin-top:3px">
            Compared: ${fmtSnap(mv.snap1_time)} vs ${fmtSnap(mv.snap2_time)}
          </div>
        </div>
        ${sigBadge}
      </div>
      ${oddsHtml}
      ${totalHtml}
      ${kalshiCardHtml}
      ${betCallHtml}
    </div>`;
  });

  el.innerHTML = html;
}

// ── Boot ──────────────────────────────────────────────────────────────────────
renderTicker();
renderYesterday();
renderPicks();
renderSharpMoney();
renderSharpAction();
renderParlays();
renderSchedule();
renderGames();
renderProps();
initTeamsTab();

// Auto-reload every 15 minutes to pick up fresh odds, lineups, and scores
// Only runs between 9am and midnight ET
(function() {
  const RELOAD_MS = 15 * 60 * 1000;
  function scheduleReload() {
    const now = new Date();
    const etHour = new Date(now.toLocaleString("en-US", {timeZone: "America/New_York"})).getHours();
    if (etHour >= 9 && etHour < 24) {
      setTimeout(() => { location.reload(true); }, RELOAD_MS);
    }
  }
  scheduleReload();
})();
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(date=None, no_open=False):
    # argparse is handled in __main__ block only — never called from here.
    target = date or datetime.now().strftime("%Y-%m-%d")

    # ── Refresh odds before loading the model ────────────────────────────────
    # Runs every time so total lines always reflect the current market, not
    # whatever was in the CSV from when the pipeline last ran.
    if not date:   # only refresh for today — backfill dates use historical data
        try:
            from scrapers.mlb_odds_scraper import run as run_odds
            odds_result = run_odds()
            log.info(f"Odds refreshed: {odds_result}")
        except Exception as e:
            log.warning(f"Odds refresh failed (non-fatal): {e}")

    from model.mlb_model import MLBModel
    from model.mlb_picks import generate_picks, build_parlays
    from model.mlb_props_model import score_all_props

    model = MLBModel()
    model.load()

    # score_today filters to upcoming games only — we also need all games for the schedule tab
    all_schedule = [g for g in model.schedule if g.get("game_date") == (
        date or datetime.now().strftime("%Y-%m-%d")
    )]

    scored, actual_date = model.score_today(target)
    if not scored and not all_schedule:
        print(f"No games found for {target}. Run python run_pipeline.py first.")
        sys.exit(0)

    # Fetch live scores FIRST — filter finished games before generating picks
    today_scores = fetch_live_scores(actual_date)
    if not today_scores:
        today_scores = model.get_today_scores(actual_date)

    # Remove games the MLB API confirms as Final BEFORE picks are generated
    finished = set(
        (s["away_team"], s["home_team"])
        for s in today_scores
        if s.get("status") == "Final"
    )
    if finished:
        before = len(scored)
        scored  = [g for g in scored
                   if (g["away_team"], g["home_team"]) not in finished]
        removed = before - len(scored)
        if removed:
            log.info(f"Removed {removed} finished game(s) confirmed by live API")

    # Now generate picks from the fully filtered game list
    picks     = generate_picks(scored)
    parlays_2 = build_parlays(picks, legs=2, max_parlays=5)
    parlays_3 = build_parlays(picks, legs=3, max_parlays=5)

    # Load standings for team records
    standings = load_standings()

    # Build schedule view (all games today, not just unstarted)
    schedule_games = all_schedule if all_schedule else scored
    schedule_json  = json.dumps(prep_schedule_view(schedule_games, today_scores, standings))

    # ── Refresh lineups + hitter stats before props ───────────────────────────
    # Lineups refresh every run. Hitter stats only re-fetch if the file is
    # missing or older than 2 hours — avoids the slow 270-player API call on
    # every 10-minute cache refresh (the file persists within a deployment).
    try:
        from scrapers.mlb_lineup_scraper import run as run_lineups
        lineups = run_lineups(target_date=actual_date)
        confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
        log.info(f"Lineup refresh: {len(lineups)} games, {confirmed} confirmed")
        if confirmed > 0:
            import time as _time
            _stats_path = os.path.join(os.path.dirname(__file__), "data", "raw",
                                       f"mlb_hitter_stats_{actual_date}.json")
            _stats_age  = (_time.time() - os.path.getmtime(_stats_path)
                           if os.path.exists(_stats_path) else float("inf"))
            if _stats_age > 4 * 3600:   # Re-fetch if missing or older than 4 hours
                from scrapers.mlb_hitter_scraper import run as run_hitters
                run_hitters(target_date=actual_date)
                log.info("Hitter stats refreshed for props")
            else:
                log.info(f"Hitter stats are {int(_stats_age/60)}m old — skipping re-fetch")
    except Exception as e:
        log.warning(f"Lineup refresh failed (non-fatal): {e}")

    # Projected lineups (loaded early — needed for projected props below)
    proj_lineups_data = load_projected_lineups(actual_date)

    # Player props (non-fatal — works only when lineups confirmed)
    try:
        props = score_all_props(actual_date)
        log.info(f"Props generated: {len(props)} confirmed props")
    except Exception as e:
        log.warning(f"Props generation failed (non-fatal): {e}")
        props = []

    # Projected props — fill in any teams that don't have confirmed props yet
    try:
        from model.mlb_props_model import score_projected_props
        # Find teams that already have confirmed props
        confirmed_games = {(p["away_team"], p["home_team"]) for p in props}
        # Only run projected scoring if we have projected lineups AND some games are uncovered
        if proj_lineups_data:
            projected = score_projected_props(proj_lineups_data, actual_date)
            # Merge: keep projected props only for games without confirmed props
            added = 0
            for p in projected:
                key = (p.get("away_team", ""), p.get("home_team", ""))
                if key not in confirmed_games:
                    props.append(p)
                    added += 1
            if added:
                log.info(f"Projected props added: {added} props for {len(set((p['away_team'],p['home_team']) for p in props if p.get('projected')))} games")
    except Exception as e:
        log.warning(f"Projected props generation failed (non-fatal): {e}")

    props_json = json.dumps(prep_props(props))
    log.info(f"Props total: {len(props)} ({sum(1 for p in props if not p.get('projected'))} confirmed, {sum(1 for p in props if p.get('projected'))} projected)")

    # Kalshi market data (optional — works only when API key configured)
    kalshi_data = load_kalshi(actual_date)
    if not kalshi_data:
        # Try to run Kalshi scraper on-the-fly if key is available
        try:
            from scrapers.mlb_kalshi_scraper import run as run_kalshi
            run_kalshi(target_date=actual_date)
            kalshi_data = load_kalshi(actual_date)
        except Exception:
            pass  # Kalshi not configured — skip silently

    # Yesterday's analysis (for dashboard panel)
    yesterday_data = load_yesterday_analysis(actual_date)
    yesterday_json = json.dumps(yesterday_data)

    # Line movement (sharp money signals)
    movement_data = load_line_movement(actual_date)
    movement_json = json.dumps(movement_data)

    # Serialize projected lineups (loaded earlier for props; reuse here for JS injection)
    proj_lineups_json = json.dumps(proj_lineups_data)
