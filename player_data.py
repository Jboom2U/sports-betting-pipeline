"""
player_data.py — Player search + per-game history for the Players section.

Backs two views:
  - /players : searchable directory (by name / team) of everyone we have logs for
  - /player/<id> : per-game trend charts (hits, TB, HR, RBI, K, SB) L5/L10/L20

Reads from player_game_logs (per-game box-score stats) and player_prop_history
(prop line vs actual). All queries read-only.
"""
import logging

log = logging.getLogger(__name__)

# Stat columns we chart, in display order: (col, label)
STAT_COLS = [
    ("h",   "Hits"),
    ("tb",  "Total Bases"),
    ("hr",  "Home Runs"),
    ("rbi", "RBIs"),
    ("k",   "Strikeouts"),
    ("sb",  "Stolen Bases"),
]


def search_players(query: str = "", team: str = "", limit: int = 300) -> list:
    """Distinct players from the logs, most-recently-seen first, name/team filtered."""
    from db.connection import db_conn
    out = []
    try:
        with db_conn() as conn:
            if conn is None:
                return out
            cur = conn.cursor()
            where, params = [], []
            if query:
                where.append("player_name ILIKE %s")
                params.append(f"%{query.strip()}%")
            if team:
                where.append("team ILIKE %s")
                params.append(f"%{team.strip()}%")
            wsql = ("WHERE " + " AND ".join(where)) if where else ""
            cur.execute(f"""
                SELECT player_id, player_name, MAX(team) AS team,
                       MAX(game_date) AS last_seen, COUNT(*) AS games
                FROM player_game_logs
                {wsql}
                GROUP BY player_id, player_name
                ORDER BY last_seen DESC, games DESC
                LIMIT %s
            """, (*params, limit))
            cols = [d[0] for d in cur.description]
            out = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as e:
        log.warning(f"search_players failed: {e}")
    return out


def get_player_season(player_id) -> dict:
    """Compact SEASON stat line to show above the per-game trends.
    Pitchers come from the pitcher stats master (keyed by MLBAM player_id) and carry
    the SEASON year, so stale data is visible at a glance (the Tyler-Phillips-2024 bug).
    Batters are aggregated from this season's game logs. Returns {} if nothing found."""
    import os
    pid = str(player_id)
    # 1) Pitcher — look up the stats master by player_id (shows the season year).
    try:
        path = os.path.join(os.path.dirname(__file__), "data", "clean",
                            "mlb_pitcher_stats_master.csv")
        prows = []
        if os.path.exists(path):
            import csv as _csv
            with open(path, encoding="utf-8-sig") as f:
                for r in _csv.DictReader(f):
                    if str(r.get("player_id", "")).strip() == pid:
                        prows.append(r)
        if prows:
            latest = max(prows, key=lambda r: str(r.get("season", "")))
            def _f(k):
                try:
                    return round(float(latest.get(k)), 2)
                except Exception:
                    return None
            return {"type": "pitcher", "season": str(latest.get("season", "")),
                    "era": _f("era"), "whip": _f("whip"), "k_per_9": _f("k_per_9"),
                    "fip": _f("fip"), "gs": latest.get("games_started"),
                    "ip": latest.get("innings_pitched"),
                    "w": latest.get("wins"), "l": latest.get("losses")}
    except Exception as e:
        log.warning(f"get_player_season pitcher lookup failed: {e}")
    # 2) Batter — aggregate this season's game logs.
    try:
        from db.connection import db_conn
        from datetime import datetime
        yr = str(datetime.now().year)
        with db_conn() as conn:
            if conn is None:
                return {}
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(ab),0), COALESCE(SUM(h),0),
                       COALESCE(SUM(hr),0), COALESCE(SUM(rbi),0), COALESCE(SUM(bb),0),
                       COALESCE(SUM(k),0), COALESCE(SUM(tb),0), COALESCE(SUM(sb),0)
                FROM player_game_logs
                WHERE player_id::text = %s AND game_date >= %s
            """, (pid, yr + "-01-01"))
            row = cur.fetchone()
        if row and row[0]:
            games, ab, h, hr, rbi, bb, k, tb, sb = row
            ab, h, tb, bb = float(ab), float(h), float(tb), float(bb)
            avg = h / ab if ab else 0.0
            obp = (h + bb) / (ab + bb) if (ab + bb) else 0.0
            slg = tb / ab if ab else 0.0
            return {"type": "batter", "season": yr, "games": games,
                    "avg": round(avg, 3), "obp": round(obp, 3), "slg": round(slg, 3),
                    "ops": round(obp + slg, 3), "hr": int(hr), "rbi": int(rbi),
                    "sb": int(sb), "k": int(k), "ab": int(ab),
                    "h": int(h), "bb": int(bb), "tb": int(tb)}
    except Exception as e:
        log.warning(f"get_player_season batter agg failed: {e}")
    return {}


def get_player_statcast(player_id) -> dict:
    """Advanced Statcast metrics for the DISPLAY-ONLY visual block on the player page.
    Pitchers from mlb_pitcher_statcast_master.csv (contact quality allowed); batters
    from mlb_statcast_master.csv. Matched by MLBAM player_id. Returns {} if none.
    NOTE: purely visual context — the eye may catch a number that jogs a read. It does
    NOT feed the pick model (the model uses its own xwOBA/whiff/barrel internally)."""
    import os
    import csv as _csv
    pid = str(player_id)
    clean = os.path.join(os.path.dirname(__file__), "data", "clean")

    def _row(fname):
        path = os.path.join(clean, fname)
        if not os.path.exists(path):
            return None
        try:
            with open(path, encoding="utf-8-sig") as f:
                for r in _csv.DictReader(f):
                    if str(r.get("player_id", "")).strip() == pid:
                        return r
        except Exception as e:
            log.warning(f"statcast read {fname} failed: {e}")
        return None

    def _f(r, k):
        try:
            return round(float(r.get(k)), 3)
        except Exception:
            return None

    def _pct(r, k):
        # Some Savant rates come as 0-1, some as 0-100 — normalize to a % number.
        v = _f(r, k)
        if v is None:
            return None
        return round(v * 100, 1) if v <= 1 else round(v, 1)

    pr = _row("mlb_pitcher_statcast_master.csv")
    if pr:
        return {"type": "pitcher", "label": "contact quality allowed", "metrics": [
            ("Velo mph", _f(pr, "avg_velocity")), ("Whiff %", _pct(pr, "whiff_percent")),
            ("xwOBA", _f(pr, "xwoba")), ("Exit Velo", _f(pr, "exit_velocity_avg")),
            ("Barrel %", _pct(pr, "barrel_batted_rate")), ("Hard-Hit %", _pct(pr, "hard_hit_percent")),
            ("K %", _pct(pr, "k_percent")), ("BB %", _pct(pr, "bb_percent"))]}

    br = _row("mlb_statcast_master.csv")
    if br:
        return {"type": "batter", "label": "contact quality", "metrics": [
            ("Barrel %", _pct(br, "barrel_batted_rate")), ("Hard-Hit %", _pct(br, "hard_hit_percent")),
            ("Exit Velo", _f(br, "exit_velocity_avg")), ("xwOBA", _f(br, "xwoba")),
            ("xBA", _f(br, "xba")), ("xSLG", _f(br, "xslg")),
            ("Launch °", _f(br, "launch_angle_avg")), ("K %", _pct(br, "k_percent"))]}
    return {}


def get_player_bvp(player_id, team: str = "") -> dict:
    """This hitter's CAREER line vs TODAY's opposing probable pitcher.
    Finds today's game in the schedule master by the player's team, gets the other
    side's probable pitcher, then hits the MLB vsPlayerTotal split. DISPLAY-ONLY:
    batter-vs-pitcher is small-sample and low-signal, shown as color, never a model
    input. Returns {} when the player has no game today or no matchup is found."""
    import os
    import csv as _csv
    from datetime import datetime
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    today = datetime.now(et).strftime("%Y-%m-%d")
    tl = (team or "").lower()
    tnick = tl.split(" ")[-1] if tl else ""
    if not tnick:
        return {}

    def _match(sched_team):
        s = (sched_team or "").lower()
        if tnick == "sox":   # red sox vs white sox — nick alone is ambiguous
            return ("red sox" in s and "red sox" in tl) or ("white sox" in s and "white sox" in tl)
        return tnick in s

    opp_pid, opp_name = "", ""
    try:
        path = os.path.join(os.path.dirname(__file__), "data", "clean", "mlb_schedule_master.csv")
        if not os.path.exists(path):
            return {}
        with open(path, encoding="utf-8-sig") as f:
            for r in _csv.DictReader(f):
                if r.get("game_date") != today:
                    continue
                if _match(r.get("away_team", "")):
                    opp_pid = r.get("home_probable_pitcher_id", "")
                    opp_name = r.get("home_probable_pitcher", "")
                    break
                if _match(r.get("home_team", "")):
                    opp_pid = r.get("away_probable_pitcher_id", "")
                    opp_name = r.get("away_probable_pitcher", "")
                    break
    except Exception as e:
        log.warning(f"bvp schedule read failed: {e}")
        return {}
    if not opp_pid:
        return {}

    try:
        import requests
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
        params = {"stats": "vsPlayerTotal", "group": "hitting", "opposingPlayerId": opp_pid}
        resp = requests.get(url, params=params,
                            headers={"User-Agent": "mlb-betting-pipeline/1.0"}, timeout=8)
        resp.raise_for_status()
        splits = ((resp.json().get("stats") or [{}])[0].get("splits") or [])
        if not splits:
            return {"pitcher": opp_name, "ab": 0, "empty": True}
        st = splits[0].get("stat", {})
        return {"pitcher": opp_name, "ab": int(st.get("atBats", 0) or 0),
                "h": int(st.get("hits", 0) or 0), "hr": int(st.get("homeRuns", 0) or 0),
                "rbi": int(st.get("rbi", 0) or 0), "bb": int(st.get("baseOnBalls", 0) or 0),
                "k": int(st.get("strikeOuts", 0) or 0),
                "avg": st.get("avg", ""), "ops": st.get("ops", "")}
    except Exception as e:
        log.warning(f"bvp api failed: {e}")
        return {"pitcher": opp_name, "error": True}


def get_player(player_id: int) -> dict:
    """Player header + last-20 game logs (most recent first)."""
    from db.connection import db_conn
    info = {"player_id": player_id, "player_name": "", "team": "", "games": []}
    try:
        with db_conn() as conn:
            if conn is None:
                return info
            cur = conn.cursor()
            cur.execute("""
                SELECT game_date, opponent, venue, pitcher_hand,
                       ab, h, hr, rbi, bb, k, tb, sb, team, player_name
                FROM player_game_logs
                WHERE player_id = %s
                ORDER BY game_date DESC
                LIMIT 20
            """, (player_id,))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if rows:
                info["player_name"] = rows[0].get("player_name", "")
                info["team"]        = rows[0].get("team", "")
            # oldest-first for left-to-right charting
            info["games"] = list(reversed(rows))
    except Exception as e:
        log.warning(f"get_player failed: {e}")
    return info
