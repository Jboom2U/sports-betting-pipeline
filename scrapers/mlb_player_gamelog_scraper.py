"""
scrapers/mlb_player_gamelog_scraper.py
Pulls per-game hitting stats (HR, H, RBI, TB, SB, K, BB) from the MLB Stats API
for every player in today's confirmed lineups.

Stores in the player_game_logs PostgreSQL table — powers the analytics
query interface ("Yordan Alvarez HRs at Minute Maid last 30 days", etc.)

Runs during the pipeline after lineup confirmation.
No auth needed — MLB Stats API is public.
"""

import os
import json
import logging
import time
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests

from db.connection import db_conn

log = logging.getLogger(__name__)

ET       = ZoneInfo("America/New_York")
BASE_URL = "https://statsapi.mlb.com/api/v1"
SEASON   = str(datetime.now(ET).year)
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")


# ── MLB Stats API helpers ──────────────────────────────────────────────────────

def _get(url: str, params: dict = None) -> dict:
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"MLB API request failed: {url} — {e}")
        return {}


def get_player_id(name: str) -> int | None:
    """Look up MLB player ID by full name via the search endpoint."""
    data = _get(f"{BASE_URL}/people/search", {"names": name, "sportId": 1})
    people = data.get("people", [])
    if not people:
        return None
    # Return first match
    return people[0].get("id")


def fetch_game_log(player_id: int, season: str = SEASON, group: str = "hitting") -> list:
    """
    Fetch season game log for a player. group="hitting" (batters) or "pitching"
    (pitchers). For pitchers, `k` is strikeouts THROWN, `h`/`hr`/`bb` are allowed;
    ab/rbi/tb/sb are 0. This lets K-prop pitchers get a meaningful strikeouts chart.
    """
    data = _get(
        f"{BASE_URL}/people/{player_id}/stats",
        {"stats": "gameLog", "group": group, "season": season, "gameType": "R"}
    )
    results = []
    for stat_block in data.get("stats", []):
        for split in stat_block.get("splits", []):
            stat   = split.get("stat", {})
            game   = split.get("game", {})
            team   = split.get("team", {}).get("name", "")
            opp    = split.get("opponent", {}).get("name", "")
            venue  = split.get("venue", {}).get("name", "")
            raw_date = game.get("officialDate") or game.get("gameDate", "")[:10]

            def _i(key): return int(stat.get(key, 0) or 0)
            if group == "pitching":
                row = {"ab": 0, "h": _i("hits"), "hr": _i("homeRuns"), "rbi": 0,
                       "bb": _i("baseOnBalls"), "k": _i("strikeOuts"), "tb": 0, "sb": 0}
            else:
                row = {"ab": _i("atBats"), "h": _i("hits"), "hr": _i("homeRuns"),
                       "rbi": _i("rbi"), "bb": _i("baseOnBalls"), "k": _i("strikeOuts"),
                       "tb": _i("totalBases"), "sb": _i("stolenBases")}
            results.append({
                "game_date": raw_date, "team": team, "opponent": opp,
                "venue": venue, "pitcher_hand": "R", **row,
            })
    return results


def run_for_players(players: list) -> dict:
    """
    Seed game logs for an explicit player list (decoupled from confirmed lineups).
    players = [{player_id, player_name, is_pitcher}]. Always populates because the
    caller passes today's props players (which always exist and carry IDs).
    """
    ok = rows = fail = 0
    seen = set()
    for p in players:
        pid = p.get("player_id")
        if not pid or pid in seen:
            continue
        seen.add(pid)
        try:
            logs = fetch_game_log(int(pid), SEASON,
                                  group="pitching" if p.get("is_pitcher") else "hitting")
            if logs:
                rows += upsert_game_logs(p.get("player_name", ""), int(pid), logs)
                ok += 1
            time.sleep(0.12)
        except Exception as e:
            log.warning(f"gamelog seed failed for {p.get('player_name')}: {e}")
            fail += 1
    log.info(f"Game-log seed: {ok} players, {rows} rows, {fail} failed")
    return {"status": "ok", "players": ok, "rows": rows, "failed": fail}


def upsert_game_logs(player_name: str, player_id: int, logs: list) -> int:
    """Insert/update game log rows. Returns count of rows upserted."""
    if not logs:
        return 0
    count = skipped = failed = 0
    with db_conn() as conn:
        if conn is None:
            return 0
        cur = conn.cursor()
        for row in logs:
            # An empty game_date raises 'invalid input syntax for type date: ""',
            # which aborts the transaction and makes every subsequent row in this
            # loop fail with 'current transaction is aborted'. Skip them up front.
            gd = row.get("game_date")
            if isinstance(gd, str):
                gd = gd.strip()
            if not gd:
                skipped += 1
                continue
            try:
                # SAVEPOINT so a single bad row cannot poison the whole batch.
                cur.execute("SAVEPOINT gl_row")
                cur.execute("""
                    INSERT INTO player_game_logs
                        (game_date, player_name, player_id, team, opponent, venue,
                         pitcher_hand, ab, h, hr, rbi, bb, k, tb, sb)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (game_date, player_name, team) DO UPDATE SET
                        hr   = EXCLUDED.hr,
                        h    = EXCLUDED.h,
                        rbi  = EXCLUDED.rbi,
                        tb   = EXCLUDED.tb,
                        ab   = EXCLUDED.ab,
                        bb   = EXCLUDED.bb,
                        k    = EXCLUDED.k,
                        sb   = EXCLUDED.sb
                """, (
                    gd, player_name, player_id,
                    row["team"], row["opponent"], row["venue"],
                    row["pitcher_hand"],
                    row["ab"], row["h"], row["hr"], row["rbi"],
                    row["bb"], row["k"], row["tb"], row["sb"],
                ))
                cur.execute("RELEASE SAVEPOINT gl_row")
                count += 1
            except Exception as e:
                failed += 1
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT gl_row")
                except Exception:
                    pass
                # Log the first few only -- this used to emit hundreds of
                # identical lines per player and drown the deploy logs.
                if failed <= 3:
                    log.warning(f"Row upsert failed for {player_name} {gd}: {e}")
    if skipped or failed:
        log.warning(f"player_game_logs [{player_name}]: {count} upserted, "
                    f"{skipped} skipped (no game_date), {failed} failed")
    return count


def seed_all_players() -> dict:
    """
    Seed game logs for EVERY active MLB player (all 30 team rosters), so the
    Players search covers everyone regardless of who's playing today. Background
    job (~780 players). Pitchers get pitching logs, position players hitting.
    """
    teams = _get(f"{BASE_URL}/teams", {"sportId": 1, "season": SEASON}).get("teams", [])
    players = []
    for t in teams:
        tid = t.get("id")
        if not tid:
            continue
        roster = _get(f"{BASE_URL}/teams/{tid}/roster",
                      {"season": SEASON, "rosterType": "active"}).get("roster", [])
        for r in roster:
            person = r.get("person", {}) or {}
            pos = (r.get("position", {}) or {}).get("abbreviation", "")
            pid = person.get("id")
            if pid:
                players.append({"player_id": pid,
                                "player_name": person.get("fullName", ""),
                                "is_pitcher": pos == "P"})
    log.info(f"seed_all_players: {len(players)} players across {len(teams)} teams")
    return run_for_players(players)


def get_lineup_players() -> list:
    """
    Load today's lineup players from the raw lineup JSON.
    Returns list of {player_name, player_id, team} dicts.
    """
    today_str = datetime.now(ET).strftime("%Y-%m-%d")
    lineup_file = os.path.join(RAW_DIR, f"mlb_lineups_{today_str}.json")

    if not os.path.exists(lineup_file):
        log.warning(f"No lineup file found: {lineup_file}")
        return []

    with open(lineup_file, encoding="utf-8") as f:
        lineup_data = json.load(f)

    players = []
    seen = set()
    for game in lineup_data:
        away = game.get("away_team", "")
        home = game.get("home_team", "")
        for side, team_name in [("away_lineup", away), ("home_lineup", home)]:
            for player in game.get(side, []):
                name = player.get("player_name") or player.get("name", "")
                pid  = player.get("player_id") or player.get("id")
                if name and name not in seen:
                    players.append({
                        "player_name": name,
                        "player_id":   int(pid) if pid else None,
                        "team":        team_name,
                    })
                    seen.add(name)

    log.info(f"Lineup players found: {len(players)}")
    return players


def run() -> dict:
    """
    Main entry point. Pull game logs for all of today's lineup players
    and store in player_game_logs table.
    """
    players = get_lineup_players()
    if not players:
        return {"status": "no_lineups", "players": 0, "rows": 0}

    total_rows  = 0
    total_ok    = 0
    total_fail  = 0

    for p in players:
        name = p["player_name"]
        pid  = p["player_id"]

        # Look up player_id if missing
        if not pid:
            pid = get_player_id(name)
            if not pid:
                log.debug(f"Could not find player_id for {name} — skipping")
                total_fail += 1
                continue

        try:
            logs = fetch_game_log(pid, SEASON)
            if logs:
                n = upsert_game_logs(name, pid, logs)
                total_rows += n
                total_ok   += 1
                log.debug(f"  {name}: {n} game log rows upserted")
            else:
                log.debug(f"  {name}: no game log data returned")
            time.sleep(0.15)   # be polite to MLB API
        except Exception as e:
            log.warning(f"Game log fetch failed for {name}: {e}")
            total_fail += 1

    result = {
        "status":  "ok",
        "players": total_ok,
        "failed":  total_fail,
        "rows":    total_rows,
    }
    log.info(f"Player game logs: {total_ok} players, {total_rows} rows, {total_fail} failed")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run())
