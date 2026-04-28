"""
mlb_model.py
Core MLB betting model.

Scores each game for moneyline, run line, and totals using:
  - Pitcher season stats, home/away splits, LHB/RHB platoon splits
  - Last 3 starts recent form vs season average
  - Team offensive and pitching stats
  - Park run/HR factors
  - Recent team form (last 10 games)
  - Weather: wind component (out/in to CF), temperature, precipitation

Pythagorean win expectation model (exponent 1.83) drives win probability.
Expected runs uses: team RPG * pitcher suppression * park factor * weather adj.
"""

import csv
import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)

BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
DATA_DIR  = os.path.join(BASE_DIR, "data")

# ── League-average baselines (MLB 2023-2025) ──────────────────────────────────
LEAGUE = {
    "era":          4.20,
    "fip":          4.20,
    "rpg":          4.50,   # runs per game per team
    "ops":          0.720,
}

PYTHAGOREAN_EXP   = 1.83
HOME_FIELD_BOOST  = 0.025
RECENT_WEIGHT     = 0.35
SEASON_WEIGHT     = 0.65
SPLIT_WEIGHT      = 0.30
SEASON_ERA_WEIGHT = 0.70

# Weather adjustment constants
WIND_RUNS_PER_MPH = 0.04   # each 1 mph blowing OUT adds ~0.04 expected runs to total
COLD_PENALTY      = 0.012  # each 1°F below 65 reduces expected runs by 1.2%
PRECIP_PENALTY    = 0.003  # each 1% precip probability reduces expected runs slightly

# Recent starts blending (last 3 starts vs season)
RECENT_STARTS_WEIGHT = 0.30
SEASON_ERA_VS_RECENT = 0.70


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def read_csv(path: str) -> list:
    if not os.path.exists(path):
        log.warning(f"File not found: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def sf(val, default=None):
    """Safe float conversion."""
    try:
        v = float(val)
        return v if v == v else default   # guard against NaN
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# MODEL
# ─────────────────────────────────────────────────────────────────────────────
class MLBModel:
    """
    Loads all clean master data once, then scores any game on demand.
    Call load() once, then score_today() or score_game() as needed.
    """

    def __init__(self):
        self.pitchers        = {}   # name -> {season -> stats_dict}
        self.pitcher_splits  = {}   # name -> {season -> {"home"->dict, "away"->dict}}
        self.pitcher_platoon = {}   # name -> {season -> {"vs. Left"->dict, "vs. Right"->dict}}
        self.pitcher_recent  = {}   # name -> [last 3 starts dicts]
        self.team_hitting    = {}   # name -> {season -> stats_dict}
        self.team_pitching   = {}   # name -> {season -> stats_dict}
        self.park_factors    = {}   # venue -> stats_dict
        self.weather         = {}   # game_id -> weather_dict
        self.odds            = {}   # (away, home) -> latest odds snapshot
        self.line_movement   = {}   # (away, home) -> movement dict
        self.scores          = []   # all historical game rows
        self.schedule        = []   # upcoming schedule rows
        self.bullpen         = {}   # team_name -> stats_dict (current season)
        self.lineups         = {}   # game_id -> {away_lineup, home_lineup, confirmed}
        self.umpires         = {}   # game_id -> umpire enriched dict
        self.pitcher_statcast = {}  # pitcher_name_lower -> statcast stat dict
        self._loaded         = False

    # ── Data Loading ──────────────────────────────────────────────────────────
    def load(self):
        if self._loaded:
            return

        log.info("Loading model data...")

        # Pitcher season stats: name -> season -> row
        for row in read_csv(os.path.join(CLEAN_DIR, "mlb_pitcher_stats_master.csv")):
            name   = row.get("player_name", "").strip()
            season = row.get("season", "")
            if name and season:
                self.pitchers.setdefault(name, {})[season] = row

        # Pitcher home/away splits: name -> season -> {home/away -> row}
        for row in read_csv(os.path.join(CLEAN_DIR, "mlb_pitcher_splits_master.csv")):
            name   = row.get("player_name", "").strip()
            season = row.get("season", "")
            split  = row.get("split", "").lower()
            if not name or not season:
                continue
            bucket = "home" if "home" in split else ("away" if "away" in split else None)
            if bucket:
                self.pitcher_splits.setdefault(name, {}).setdefault(season, {})[bucket] = row

        # Team hitting: name -> season -> row
        for row in read_csv(os.path.join(CLEAN_DIR, "mlb_team_hitting_master.csv")):
            name   = row.get("team_name", "").strip()
            season = row.get("season", "")
            if name and season:
                self.team_hitting.setdefault(name, {})[season] = row

        # Team pitching: name -> season -> row
        for row in read_csv(os.path.join(CLEAN_DIR, "mlb_team_pitching_master.csv")):
            name   = row.get("team_name", "").strip()
            season = row.get("season", "")
            if name and season:
                self.team_pitching.setdefault(name, {})[season] = row

        # Pitcher platoon splits: name -> season -> {vs. Left / vs. Right -> row}
        for row in read_csv(os.path.join(CLEAN_DIR, "mlb_pitcher_platoon_master.csv")):
            name   = row.get("player_name", "").strip()
            season = row.get("season", "")
            split  = row.get("split", "")
            if name and season and split:
                self.pitcher_platoon.setdefault(name, {}).setdefault(season, {})[split] = row

        # Pitcher recent starts: name -> list of start dicts (sorted recent first)
        for row in read_csv(os.path.join(CLEAN_DIR, "mlb_pitcher_recent_master.csv")):
            name = row.get("player_name", "").strip()
            if name:
                self.pitcher_recent.setdefault(name, []).append(row)
        # Sort each pitcher's starts by date desc
        for name in self.pitcher_recent:
            self.pitcher_recent[name].sort(key=lambda x: x.get("game_date",""), reverse=True)

        # Park factors: venue -> row
        for row in read_csv(os.path.join(DATA_DIR, "park_factors.csv")):
            venue = row.get("venue", "").strip()
            if venue:
                self.park_factors[venue] = row

        # Weather: game_id -> row (today's file if available)
        today = datetime.now().strftime("%Y-%m-%d")
        raw_dir = os.path.join(BASE_DIR, "data", "raw")
        weather_file = os.path.join(raw_dir, f"mlb_weather_{today}.csv")
        if not os.path.exists(weather_file):
            # Try most recent weather file
            import glob
            files = sorted(glob.glob(os.path.join(raw_dir, "mlb_weather_*.csv")), reverse=True)
            weather_file = files[0] if files else None
        if weather_file and os.path.exists(weather_file):
            for row in read_csv(weather_file):
                gid = row.get("game_id", "")
                if gid:
                    self.weather[gid] = row
            log.info(f"Weather loaded: {len(self.weather)} games")

        # Odds snapshots and line movement
        odds_master = os.path.join(CLEAN_DIR, "mlb_odds_master.csv")
        if os.path.exists(odds_master):
            today = datetime.now().strftime("%Y-%m-%d")
            latest_snap = {}
            for row in read_csv(odds_master):
                if row.get("game_date") != today:
                    continue
                k = (row.get("away_team",""), row.get("home_team",""))
                if k not in latest_snap or row.get("snapshot_time","") > latest_snap[k].get("snapshot_time",""):
                    latest_snap[k] = row
            self.odds = latest_snap
            log.info(f"Odds loaded: {len(self.odds)} games")

        import glob
        movement_files = sorted(
            glob.glob(os.path.join(CLEAN_DIR, "mlb_line_movement_*.csv")), reverse=True
        )
        if movement_files:
            for row in read_csv(movement_files[0]):
                k = (row.get("away_team",""), row.get("home_team",""))
                self.line_movement[k] = row
            log.info(f"Line movement loaded: {len(self.line_movement)} records")

        # Bullpen stats: team_name -> row (current season preferred)
        bullpen_master = os.path.join(CLEAN_DIR, "mlb_bullpen_master.csv")
        if os.path.exists(bullpen_master):
            season_str = str(datetime.now().year)
            all_bp     = read_csv(bullpen_master)
            # Prefer current season, fall back to most recent
            for row in all_bp:
                tname = row.get("team_name", "").strip()
                if not tname:
                    continue
                if row.get("season") == season_str:
                    self.bullpen[tname] = row
                elif tname not in self.bullpen:
                    self.bullpen[tname] = row
            log.info(f"Bullpen data loaded: {len(self.bullpen)} teams")

        # Confirmed lineups for today
        today_str = datetime.now().strftime("%Y-%m-%d")
        raw_dir   = os.path.join(BASE_DIR, "data", "raw")
        lineup_file = os.path.join(raw_dir, f"mlb_lineups_{today_str}.json")
        if os.path.exists(lineup_file):
            import json
            with open(lineup_file, encoding="utf-8") as f:
                lineup_data = json.load(f)
            for game in lineup_data:
                gid = str(game.get("game_id", ""))
                if gid:
                    self.lineups[gid] = game
            log.info(f"Lineups loaded: {len(self.lineups)} games "
                     f"({sum(1 for g in self.lineups.values() if g.get('lineup_confirmed'))} confirmed)")

        # Pitcher Statcast stuff metrics (xwOBA against, whiff%, velocity)
        try:
            from scrapers.mlb_statcast_pitcher_scraper import load_pitcher_statcast
            self.pitcher_statcast = load_pitcher_statcast()
            log.info(f"Pitcher Statcast loaded: {len(self.pitcher_statcast)} pitchers")
        except Exception as e:
            log.debug(f"Pitcher Statcast not available (non-fatal): {e}")

        # Umpire assignments for today
        ump_file = os.path.join(raw_dir, f"mlb_umpires_{today_str}.json")
        if os.path.exists(ump_file):
            with open(ump_file, encoding="utf-8") as f:
                ump_data = json.load(f)
            for g in ump_data:
                gid = str(g.get("game_id", ""))
                if gid:
                    self.umpires[gid] = g
            log.info(f"Umpires loaded: {len(self.umpires)} games")

        # Historical scores and upcoming schedule
        self.scores   = read_csv(os.path.join(CLEAN_DIR, "mlb_scores_master.csv"))
        self.schedule = read_csv(os.path.join(CLEAN_DIR, "mlb_schedule_master.csv"))

        self._loaded = True
        log.info(f"Loaded: {len(self.pitchers)} pitchers | {len(self.team_hitting)} teams | "
                 f"{len(self.park_factors)} parks | {len(self.scores)} historical games | "
                 f"{len(self.weather)} weather | {len(self.bullpen)} bullpens | "
                 f"{len(self.lineups)} lineups | {len(self.umpires)} umpires | "
                 f"{len(self.pitcher_statcast)} pitcher Statcast")

    # ── Pitcher Lookup ────────────────────────────────────────────────────────
    def get_pitcher(self, name: str, is_home: bool) -> dict:
        """
        Return pitcher stats for the most recent available season,
        with ERA/FIP blended toward the relevant home-or-away split.
        """
        if not name or name not in self.pitchers:
            return {"era_adj": LEAGUE["era"], "fip_adj": LEAGUE["fip"],
                    "whip": 1.30, "k_per_9": 8.5, "name": name or "TBD",
                    "era": LEAGUE["era"], "fip": LEAGUE["fip"], "missing": True}

        season  = sorted(self.pitchers[name].keys())[-1]
        base    = self.pitchers[name][season]
        era_s   = sf(base.get("era"),   LEAGUE["era"])
        fip_s   = sf(base.get("fip"),   LEAGUE["fip"])
        whip_s  = sf(base.get("whip"),  1.30)
        k9_s    = sf(base.get("k_per_9"), 8.5)

        # Blend with home/away split if available
        split_key = "home" if is_home else "away"
        split_row = (self.pitcher_splits.get(name, {})
                         .get(season, {})
                         .get(split_key, {}))

        if split_row:
            era_sp  = sf(split_row.get("era"))
            fip_sp  = sf(split_row.get("fip"))
            era_adj = (SEASON_ERA_WEIGHT * era_s + SPLIT_WEIGHT * era_sp
                       if era_sp is not None else era_s)
            fip_adj = (SEASON_ERA_WEIGHT * fip_s + SPLIT_WEIGHT * fip_sp
                       if fip_sp is not None else fip_s)
        else:
            era_adj = era_s
            fip_adj = fip_s

        return {
            "name":        name,
            "season":      season,
            "era":         era_s,
            "fip":         fip_s,
            "whip":        whip_s,
            "k_per_9":     k9_s,
            "era_adj":     round(era_adj, 3),
            "fip_adj":     round(fip_adj, 3),
            "gs":          sf(base.get("games_started"), 0),
            "split_used":  bool(split_row),
            "missing":     False,
        }

    # ── Team Offense Lookup ───────────────────────────────────────────────────
    def get_offense(self, team: str) -> dict:
        """Return team's most recent season offensive stats."""
        if team not in self.team_hitting:
            return {"rpg": LEAGUE["rpg"], "ops": LEAGUE["ops"], "k_rate": 0.22, "missing": True}
        season = sorted(self.team_hitting[team].keys())[-1]
        row    = self.team_hitting[team][season]
        return {
            "season":  season,
            "rpg":     sf(row.get("runs_per_game"), LEAGUE["rpg"]),
            "ops":     sf(row.get("ops"), LEAGUE["ops"]),
            "obp":     sf(row.get("obp")),
            "slg":     sf(row.get("slg")),
            "k_rate":  sf(row.get("k_rate"), 0.22),
            "bb_rate": sf(row.get("bb_rate"), 0.08),
            "missing": False,
        }

    # ── Park Factor Lookup ────────────────────────────────────────────────────
    def get_park(self, venue: str) -> dict:
        """Return park factors, falling back to neutral (100) if not found."""
        if venue in self.park_factors:
            return self.park_factors[venue]
        # Partial match
        v_low = venue.lower()
        for k, v in self.park_factors.items():
            if k.lower() in v_low or v_low in k.lower():
                return v
        return {"park_factor_runs": "100", "park_factor_hr": "100",
                "notes": "Unknown park - neutral assumed"}

    # ── Platoon Split Lookup ─────────────────────────────────────────────────
    def get_platoon(self, name: str, vs: str) -> dict:
        """
        Get pitcher stats vs left or right handed batters.
        vs: 'vs. Left' or 'vs. Right'
        """
        if not name or name not in self.pitcher_platoon:
            return {}
        season = sorted(self.pitcher_platoon[name].keys())[-1]
        return self.pitcher_platoon[name][season].get(vs, {})

    # ── Recent Starts Summary ─────────────────────────────────────────────────
    def get_recent_form_pitcher(self, name: str, n: int = 3) -> dict:
        """
        Summarize last N starts: avg ERA, avg game score, trend vs season.
        Returns empty dict if no data.
        """
        if not name or name not in self.pitcher_recent:
            return {}
        starts = self.pitcher_recent[name][:n]
        if not starts:
            return {}

        ips     = [sf(s.get("innings_pitched"), 0) for s in starts]
        ers     = [sf(s.get("earned_runs"), 0) for s in starts]
        scores  = [sf(s.get("game_score"), 50) for s in starts]
        total_ip = sum(ips)

        recent_era  = round((sum(ers) * 9 / total_ip), 2) if total_ip > 0 else None
        avg_gs      = round(sum(scores) / len(scores), 1)
        trend       = "HOT" if avg_gs >= 60 else ("COLD" if avg_gs <= 40 else "NEUTRAL")

        return {
            "n":           len(starts),
            "recent_era":  recent_era,
            "avg_gs":      avg_gs,
            "trend":       trend,
            "last_dates":  [s.get("game_date","") for s in starts],
        }

    # ── Weather Lookup ────────────────────────────────────────────────────────
    def get_weather(self, game_id: str) -> dict:
        return self.weather.get(str(game_id), {})

    def get_ump(self, game_id: str) -> dict:
        return self.umpires.get(str(game_id), {})

    # ── Odds and Line Movement Lookup ─────────────────────────────────────────
    def get_odds(self, away: str, home: str) -> dict:
        return self.odds.get((away, home), {})

    def get_movement(self, away: str, home: str) -> dict:
        return self.line_movement.get((away, home), {})

    def line_movement_confidence_adj(self, away: str, home: str,
                                      ml_pick: str, total_pick: str) -> tuple:
        """
        Returns (ml_adj, total_adj) confidence adjustments based on line movement.
        Positive = boost, negative = reduce.
        STEAM/DRIFT in same direction as model pick = +0.03 to +0.05 boost
        STEAM/DRIFT against model pick = -0.04 to -0.07 reduction
        """
        mov = self.get_movement(away, home)
        if not mov:
            return 0.0, 0.0

        ml_adj    = 0.0
        total_adj = 0.0

        ml_sig    = mov.get("ml_signal", "STABLE")
        tot_sig   = mov.get("total_signal", "STABLE")
        sharp     = mov.get("sharp_side", "")

        # ML adjustment
        if ml_sig in ("STEAM", "DRIFT") and sharp:
            magnitude = 0.05 if ml_sig == "STEAM" else 0.03
            if sharp == ml_pick:
                ml_adj = +magnitude   # sharp money agrees with our pick
            else:
                ml_adj = -magnitude   # sharp money opposes our pick

        # Totals adjustment
        if tot_sig in ("STEAM", "DRIFT"):
            total_move = sf(mov.get("total_move"), 0)
            magnitude  = 0.04 if tot_sig == "STEAM" else 0.02
            if (total_move > 0 and total_pick == "OVER") or \
               (total_move < 0 and total_pick == "UNDER"):
                total_adj = +magnitude  # line moved same direction as our pick
            else:
                total_adj = -magnitude  # line moved against our pick

        return ml_adj, total_adj

    def book_discrepancy_adj(self, away: str, home: str,
                              ml_pick: str, total_pick: str) -> tuple:
        """
        Returns (ml_adj, total_adj) based on DraftKings vs consensus discrepancy.
        Positive disc = DK is softer than market (more value on that side).
        10-19 pts better -> +0.02 | 20+ pts better -> +0.04 | 10+ pts worse -> -0.02
        """
        snap = self.get_odds(away, home)
        if not snap:
            return 0.0, 0.0

        ml_adj    = 0.0
        total_adj = 0.0

        # ML discrepancy
        disc = sf(snap.get("disc_ml_away") if ml_pick == away else snap.get("disc_ml_home"))
        if disc is not None:
            if disc >= 20:    ml_adj = +0.04
            elif disc >= 10:  ml_adj = +0.02
            elif disc <= -10: ml_adj = -0.02

        # Total discrepancy
        disc_t = sf(snap.get("disc_total"))
        if disc_t is not None:
            if total_pick == "OVER":
                if disc_t <= -0.5:   total_adj = +0.02
                elif disc_t >= 0.5:  total_adj = -0.02
            else:
                if disc_t >= 0.5:    total_adj = +0.02
                elif disc_t <= -0.5: total_adj = -0.02

        return ml_adj, total_adj

    # ── Rest Days ─────────────────────────────────────────────────────────────
    def get_rest_days(self, team: str, game_date: str) -> int | None:
        """
        Returns days of rest for team heading into game_date.
        0 = back-to-back, 1 = one day off, 2+ = well-rested.
        None = not enough data.
        """
        if not game_date:
            return None
        prev_games = []
        for row in self.schedule:
            if row.get("away_team") != team and row.get("home_team") != team:
                continue
            gd = row.get("game_date", "")
            if gd and gd < game_date:
                prev_games.append(gd)
        if not prev_games:
            # Also check scores master
            for row in self.scores:
                if row.get("away_team") != team and row.get("home_team") != team:
                    continue
                gd = row.get("game_date", "")
                if gd and gd < game_date:
                    prev_games.append(gd)
        if not prev_games:
            return None
        last_game = max(prev_games)
        try:
            from datetime import datetime as _dt
            d1 = _dt.strptime(game_date, "%Y-%m-%d")
            d2 = _dt.strptime(last_game, "%Y-%m-%d")
            return (d1 - d2).days - 1   # 0 = played yesterday (back-to-back)
        except Exception:
            return None

    def rest_adj(self, away: str, home: str,
                 game_date: str, ml_side: str) -> tuple:
        """
        Returns (ml_adj, total_adj) based on rest days differential.
        Back-to-back (0 rest): -0.02 to the tired team's pick
        2+ days rest vs back-to-back: +0.02 to the rested team
        """
        away_rest = self.get_rest_days(away, game_date)
        home_rest = self.get_rest_days(home, game_date)
        if away_rest is None or home_rest is None:
            return 0.0, 0.0

        ml_adj    = 0.0
        total_adj = 0.0

        # Back-to-back penalty
        if away_rest == 0 and home_rest > 0:
            if ml_side == "away": ml_adj = -0.02
            else:                 ml_adj = +0.02
        elif home_rest == 0 and away_rest > 0:
            if ml_side == "home": ml_adj = -0.02
            else:                 ml_adj = +0.02

        # Well-rested edge (2+ days vs 0-1 days)
        if away_rest >= 2 and home_rest == 0:
            if ml_side == "away": ml_adj += +0.01
        elif home_rest >= 2 and away_rest == 0:
            if ml_side == "home": ml_adj += +0.01

        # Totals: fatigue reduces run production slightly
        if away_rest == 0 or home_rest == 0:
            total_adj = -0.01  # slight UNDER lean when either team is tired

        return ml_adj, total_adj

    # ── Pitcher Quality Gap ────────────────────────────────────────────────────
    def pitcher_gap_adj(self, away_era_adj: float, home_era_adj: float,
                        ml_side: str) -> float:
        """
        Confidence boost when one starter is significantly better than the other.
        ERA gap of 0.75+ → +0.02 | gap of 1.50+ → +0.03 | gap of 2.50+ → +0.04
        Applied to the team facing the WORSE pitcher.
        """
        if away_era_adj is None or home_era_adj is None:
            return 0.0
        if away_era_adj <= 0 or home_era_adj <= 0:
            return 0.0

        # Lower ERA = better pitcher. Gap = how much worse one starter is.
        gap = abs(away_era_adj - home_era_adj)
        better_side = "home" if away_era_adj > home_era_adj else "away"  # lower ERA wins

        if gap < 0.75:
            return 0.0
        elif gap < 1.50:
            boost = 0.02
        elif gap < 2.50:
            boost = 0.03
        else:
            boost = 0.04

        # Boost applies to the team facing the worse pitcher (offensive edge)
        # which is ALSO the team with the better pitcher pitching for them
        if ml_side == better_side:
            return +boost
        else:
            return -boost   # we're picking the team facing the better arm — small penalty

    # ── Market Agreement ──────────────────────────────────────────────────────
    def market_agreement_adj(self, ml_conf_base: float,
                              away_odds: float | None,
                              home_odds: float | None,
                              ml_side: str) -> float:
        """
        Compare model's win probability to implied market probability.
        If model and market agree strongly → validate confidence.
        If model strongly disagrees with market → flag (could be edge OR error).

        Returns ml_adj float.
        Market implied prob from American odds:
          Negative line: |line| / (|line| + 100)
          Positive line: 100  / (line  + 100)
        """
        if away_odds is None or home_odds is None:
            return 0.0

        def _implied(odds: float) -> float:
            if odds < 0:
                return abs(odds) / (abs(odds) + 100)
            else:
                return 100.0 / (odds + 100)

        # Normalize to remove vig
        away_imp = _implied(away_odds)
        home_imp = _implied(home_odds)
        total    = away_imp + home_imp
        if total <= 0:
            return 0.0
        away_imp /= total
        home_imp /= total

        market_wp = home_imp if ml_side == "home" else away_imp
        model_gap = ml_conf_base - market_wp   # positive = model MORE confident than market

        # Model agrees with market (both like same side, gap small)
        if abs(model_gap) <= 0.03:
            return +0.01   # slight validation boost — market confirms our pick

        # Model is more confident than market — possible edge
        if 0.04 <= model_gap <= 0.10:
            return +0.02   # we see more edge than the market

        # Model significantly more confident — either big edge or model over-confident
        if model_gap > 0.10:
            return +0.01   # reduce boost (dangerous territory — market may know more)

        # Market more confident than model — we're fighting the market
        if model_gap < -0.05:
            return -0.02   # market sees something we don't — reduce confidence

        return 0.0

    # ── Convergence Multiplier ────────────────────────────────────────────────
    def convergence_adj(self, signals: list) -> float:
        """
        When multiple independent signals all agree, compound confidence.
        signals: list of booleans — True if signal agrees with pick, False if against.
        Returns additional confidence boost for convergence.

        3 signals agree  → +0.02
        4 signals agree  → +0.03
        5+ signals agree → +0.04
        Majority against → -0.02
        """
        if not signals:
            return 0.0
        agree   = sum(1 for s in signals if s is True)
        against = sum(1 for s in signals if s is False)
        total   = len(signals)

        if total < 3:
            return 0.0

        if agree >= 5:
            return +0.04
        elif agree >= 4:
            return +0.03
        elif agree >= 3:
            return +0.02
        elif against >= 3:
            return -0.02
        return 0.0

    # ── Bullpen Lookup ────────────────────────────────────────────────────────
    def get_bullpen(self, team: str) -> dict:
        """Return bullpen ERA and WHIP for the team. Falls back to league avg."""
        if team in self.bullpen:
            row = self.bullpen[team]
            return {
                "era":      sf(row.get("bullpen_era"),  4.20),
                "whip":     sf(row.get("bullpen_whip"), 1.30),
                "k9":       sf(row.get("bullpen_k9"),   9.0),
                "save_pct": sf(row.get("bullpen_save_pct"), 0.68),
                "found":    True,
            }
        # Partial name match
        tl = team.lower()
        for k, row in self.bullpen.items():
            if k.lower() in tl or tl in k.lower():
                return {
                    "era":      sf(row.get("bullpen_era"),  4.20),
                    "whip":     sf(row.get("bullpen_whip"), 1.30),
                    "k9":       sf(row.get("bullpen_k9"),   9.0),
                    "save_pct": sf(row.get("bullpen_save_pct"), 0.68),
                    "found":    True,
                }
        return {"era": 4.20, "whip": 1.30, "k9": 9.0, "save_pct": 0.68, "found": False}

    # ── Lineup OPS ────────────────────────────────────────────────────────────
    def get_lineup_ops(self, game_id: str, side: str) -> float | None:
        """
        If confirmed lineups exist, compute weighted OPS for the top 9 batters.
        side: 'away' or 'home'
        Returns None if lineup not confirmed.
        """
        gid = str(game_id)
        if gid not in self.lineups:
            return None
        game = self.lineups[gid]
        if not game.get("lineup_confirmed"):
            return None
        players = game.get(f"{side}_lineup", [])
        if not players:
            return None
        ops_vals = [p.get("ops") for p in players if p.get("ops") and float(p.get("ops", 0)) > 0]
        if not ops_vals:
            return None
        return round(sum(ops_vals) / len(ops_vals), 3)

    # ── Recent Form ───────────────────────────────────────────────────────────
    def recent_form(self, team: str, n: int = 10) -> dict:
        """Last N games avg runs scored/allowed and win pct."""
        games = []
        for row in reversed(self.scores):
            if row.get("away_team") == team or row.get("home_team") == team:
                games.append(row)
            if len(games) >= n:
                break

        if not games:
            return {"rpg": LEAGUE["rpg"], "rapg": LEAGUE["rpg"], "win_pct": 0.50, "n": 0}

        scored = allowed = wins = 0
        for g in games:
            if g.get("away_team") == team:
                rs = sf(g.get("away_score"), 0)
                ra = sf(g.get("home_score"), 0)
            else:
                rs = sf(g.get("home_score"), 0)
                ra = sf(g.get("away_score"), 0)
            scored  += rs
            allowed += ra
            if rs > ra:
                wins += 1

        n_g = len(games)
        return {
            "rpg":      round(scored  / n_g, 3),
            "rapg":     round(allowed / n_g, 3),
            "win_pct":  round(wins    / n_g, 3),
            "n":        n_g,
        }

    # ── Expected Runs ─────────────────────────────────────────────────────────
    def exp_runs(self, team: str, opp_pitcher: dict,
                 park_run_factor: float, is_home: bool,
                 weather: dict = None,
                 game_id: str = None,
                 opp_team: str = None) -> float:
        """
        Expected runs scored by `team` facing `opp_pitcher` at this park.

        Improvements:
          - Bullpen blending: SP covers ~60% of game, bullpen ~40%
          - Lineup OPS adjustment when confirmed lineups are available
          - Weather adjustments as before
        """
        offense = self.get_offense(team)
        form    = self.recent_form(team)

        # Blend season RPG with recent form
        base_rpg = (SEASON_WEIGHT * offense["rpg"] +
                    RECENT_WEIGHT * form["rpg"])

        # ── Lineup OPS adjustment ─────────────────────────────────────────────
        # If confirmed lineup is available, scale RPG by lineup OPS vs team avg OPS
        if game_id:
            side = "home" if is_home else "away"
            lineup_ops = self.get_lineup_ops(game_id, side)
            if lineup_ops:
                team_ops = offense.get("ops", LEAGUE["ops"]) or LEAGUE["ops"]
                ops_adj  = lineup_ops / team_ops
                # Dampen: lineup OPS can move RPG ±12% max
                ops_adj  = max(0.88, min(1.12, ops_adj))
                base_rpg  = base_rpg * ops_adj

        # ── SP era blended with recent starts ────────────────────────────────
        era_season   = opp_pitcher.get("era_adj", LEAGUE["era"])
        pitcher_name = opp_pitcher.get("name", "")
        recent_sp    = self.get_recent_form_pitcher(pitcher_name)
        if recent_sp and recent_sp.get("recent_era"):
            era_sp = (SEASON_ERA_VS_RECENT * era_season +
                      RECENT_STARTS_WEIGHT * recent_sp["recent_era"])
        else:
            era_sp = era_season

        # ── Bullpen ERA for the opponent team ────────────────────────────────
        # opp_team is the team fielding (SP + bullpen); if not passed, skip bullpen
        if opp_team:
            bp = self.get_bullpen(opp_team)
            bp_era = bp["era"]
        else:
            bp_era = LEAGUE["era"]

        # Blend: SP pitches ~60% of outs, bullpen ~40%
        SP_SHARE  = 0.60
        BP_SHARE  = 0.40
        blended_era = SP_SHARE * era_sp + BP_SHARE * bp_era
        suppression = blended_era / LEAGUE["era"]

        # ── Pitcher Statcast stuff adjustment ─────────────────────────────────
        # xwOBA against is the most reliable contact-quality metric.
        # A pitcher with xwOBA=0.280 (elite) is genuinely better than ERA implies;
        # one at 0.350 is worse. We blend 25% Statcast, 75% ERA-based suppression.
        LEAGUE_XWOBA   = 0.315   # MLB average xwOBA against, 2023-2025
        STUFF_WEIGHT   = 0.25    # how much Statcast adjusts suppression
        LEAGUE_WHIFF   = 25.0    # MLB average whiff%, 2023-2025
        WHIFF_WEIGHT   = 0.10    # secondary signal weight

        sc = self.pitcher_statcast.get((pitcher_name or "").lower(), {})
        xwoba = sc.get("xwoba")
        whiff = sc.get("whiff_percent")

        stuff_mult = 1.0
        if xwoba is not None and xwoba > 0:
            xwoba_factor = xwoba / LEAGUE_XWOBA
            # Blend: suppression = (1 - STUFF_WEIGHT) * ERA-based + STUFF_WEIGHT * xwoba-adjusted
            stuff_mult = 1 - STUFF_WEIGHT + STUFF_WEIGHT * xwoba_factor
        if whiff is not None and whiff > 0:
            # High whiff = fewer hard contact opps → lower suppression multiplier
            whiff_delta = (whiff - LEAGUE_WHIFF) / 100.0
            stuff_mult -= whiff_delta * WHIFF_WEIGHT
        # Cap total adjustment at ±15%
        stuff_mult = max(0.85, min(1.15, stuff_mult))
        suppression *= stuff_mult

        park_adj    = park_run_factor / 100.0
        loc_boost   = 1.02 if is_home else 1.0

        expected = base_rpg * suppression * park_adj * loc_boost

        # ── Weather adjustments ───────────────────────────────────────────────
        if weather and not weather.get("roof"):
            wc      = sf(weather.get("wind_component"), 0)
            temp    = sf(weather.get("temp_f"), 70)
            precip  = sf(weather.get("precip_prob"), 0)

            wind_adj = 1.0 + (wc * WIND_RUNS_PER_MPH * 0.5 / max(base_rpg, 1))

            cold_adj = 1.0
            if temp < 65:
                cold_adj = max(0.85, 1.0 - (65 - temp) * COLD_PENALTY)

            precip_adj = max(0.95, 1.0 - precip * PRECIP_PENALTY)

            expected *= wind_adj * cold_adj * precip_adj

        return max(0.5, round(expected, 3))

    # ── Score a Single Game ───────────────────────────────────────────────────
    def score_game(self, game: dict) -> dict:
        """
        Score one scheduled game. Returns a rich dict with all picks and reasoning.
        """
        away      = game.get("away_team", "")
        home      = game.get("home_team", "")
        venue     = game.get("venue", "")
        game_date = game.get("game_date", "")

        away_sp_name = game.get("away_probable_pitcher", "")
        home_sp_name = game.get("home_probable_pitcher", "")

        away_sp = self.get_pitcher(away_sp_name, is_home=False)
        home_sp = self.get_pitcher(home_sp_name, is_home=True)

        park          = self.get_park(venue)
        park_runs     = sf(park.get("park_factor_runs"), 100)
        park_hr       = sf(park.get("park_factor_hr"),   100)

        away_offense  = self.get_offense(away)
        home_offense  = self.get_offense(home)
        away_form     = self.recent_form(away)
        home_form     = self.recent_form(home)

        # Pitcher recent starts
        away_sp_recent = self.get_recent_form_pitcher(away_sp_name)
        home_sp_recent = self.get_recent_form_pitcher(home_sp_name)

        # Platoon splits
        away_vs_lhb = self.get_platoon(away_sp_name, "vs. Left")
        away_vs_rhb = self.get_platoon(away_sp_name, "vs. Right")
        home_vs_lhb = self.get_platoon(home_sp_name, "vs. Left")
        home_vs_rhb = self.get_platoon(home_sp_name, "vs. Right")

        # Weather
        weather = self.get_weather(game.get("game_id", ""))

        # Bullpen data
        away_bp = self.get_bullpen(away)
        home_bp = self.get_bullpen(home)
        game_id_str = str(game.get("game_id", ""))

        # Expected runs (now includes bullpen blend + lineup OPS + weather)
        exp_away = self.exp_runs(away, home_sp, park_runs, is_home=False,
                                 weather=weather, game_id=game_id_str, opp_team=home)
        exp_home = self.exp_runs(home, away_sp, park_runs, is_home=True,
                                 weather=weather, game_id=game_id_str, opp_team=away)
        exp_total = round(exp_away + exp_home, 2)

        # Umpire adjustment — apply blended run tendency vs. league average
        ump_data   = self.get_ump(game_id_str)
        ump_factor = float(ump_data.get("ump_factor", 0.0))
        ump_name   = ump_data.get("hp_ump", "Unknown")
        exp_total  = round(exp_total + ump_factor, 2)

        # Pythagorean win probability
        e = PYTHAGOREAN_EXP
        raw_home_wp = (exp_home ** e) / (exp_home ** e + exp_away ** e)
        home_wp = round(min(0.85, max(0.15, raw_home_wp + HOME_FIELD_BOOST)), 4)
        away_wp = round(1.0 - home_wp, 4)

        # Moneyline
        if home_wp >= away_wp:
            ml_team, ml_side, ml_conf_base = home, "home", home_wp
        else:
            ml_team, ml_side, ml_conf_base = away, "away", away_wp

        # Odds — fetch early so we can use the market total line
        odds_snap   = self.get_odds(away, home)
        movement    = self.get_movement(away, home)

        # Totals — use real market line when available, otherwise park-adjusted fallback
        # League-average O/U baseline is ~8.5; scale by park run factor for a sensible default
        market_line = sf(odds_snap.get("total_line"))
        if market_line:
            line = market_line
        else:
            # Park-adjusted fallback: 8.5 * (park_runs/100), capped at 12.5
            line = round(min(12.5, 8.5 * (park_runs / 100.0)), 1)

        diff = exp_total - line
        total_pick      = "OVER" if diff > 0 else "UNDER"
        total_conf_base = min(0.74, 0.50 + abs(diff) / 7.0)
        ml_adj, total_adj = self.line_movement_confidence_adj(
            away, home, ml_team, total_pick
        )
        disc_ml_adj, disc_total_adj = self.book_discrepancy_adj(
            away, home, ml_team, total_pick
        )
        ml_adj    += disc_ml_adj
        total_adj += disc_total_adj

        # Rest/travel adjustment
        rest_ml_adj, rest_total_adj = self.rest_adj(away, home, game_date, ml_side)
        ml_adj    += rest_ml_adj
        total_adj += rest_total_adj

        # Pitcher quality gap adjustment
        away_era_adj = away_sp.get("era_adj", LEAGUE["era"])
        home_era_adj = home_sp.get("era_adj", LEAGUE["era"])
        gap_adj = self.pitcher_gap_adj(away_era_adj, home_era_adj, ml_side)
        ml_adj += gap_adj

        # Market agreement adjustment
        market_ml_adj = self.market_agreement_adj(
            ml_conf_base,
            sf(odds_snap.get("ml_away")),
            sf(odds_snap.get("ml_home")),
            ml_side
        )
        ml_adj += market_ml_adj

        # Convergence multiplier — how many independent signals agree with the ML pick?
        mov = self.get_movement(away, home)
        sharp = mov.get("sharp_side", "") if mov else ""
        conv_signals = []
        # Signal 1: line movement direction
        if mov and mov.get("ml_signal") in ("STEAM","DRIFT"):
            conv_signals.append(sharp == ml_team)
        # Signal 2: pitcher gap favors our pick
        if gap_adj != 0.0:
            conv_signals.append(gap_adj > 0)
        # Signal 3: market agrees with model
        if market_ml_adj != 0.0:
            conv_signals.append(market_ml_adj > 0)
        # Signal 4: team recent form — picked team winning > 60% last 10
        picked_form = home_form if ml_side == "home" else away_form
        if picked_form.get("win_pct") is not None:
            conv_signals.append(picked_form["win_pct"] >= 0.60)
        # Signal 5: rest advantage
        if rest_ml_adj != 0.0:
            conv_signals.append(rest_ml_adj > 0)
        conv_adj  = self.convergence_adj(conv_signals)
        ml_adj   += conv_adj

        ml_conf    = round(min(0.90, max(0.10, ml_conf_base + ml_adj)), 4)
        total_conf = round(min(0.82, max(0.10, total_conf_base + total_adj)), 4)

        # Run line: only offer when one side has 60%+ ML confidence
        rl_threshold = 0.60
        if home_wp >= rl_threshold:
            rl_team = home
            rl_pick = f"{home} -1.5"
            rl_conf = round(min(0.70, 0.50 + (home_wp - rl_threshold) * 0.80), 4)
        elif away_wp >= rl_threshold:
            rl_team = away
            rl_pick = f"{away} -1.5"
            rl_conf = round(min(0.70, 0.50 + (away_wp - rl_threshold) * 0.80), 4)
        else:
            rl_team = None
            rl_pick = "No strong run line play"
            rl_conf = 0.0

        return {
            # Identity
            "game_id":        game.get("game_id", ""),
            "game_date":      game_date,
            "game_time_utc":  game.get("game_time_utc", ""),
            "away_team":      away,
            "home_team":      home,
            "venue":          venue,

            # Park
            "park_runs":      park_runs,
            "park_hr":        park_hr,
            "park_notes":     park.get("notes", ""),

            # Pitchers
            "away_sp":        away_sp_name or "TBD",
            "away_sp_era":    away_sp.get("era"),
            "away_sp_era_adj":away_sp.get("era_adj"),
            "away_sp_fip":    away_sp.get("fip_adj"),
            "away_sp_whip":   away_sp.get("whip"),
            "away_sp_k9":     away_sp.get("k_per_9"),
            "away_sp_missing":away_sp.get("missing", False),

            "home_sp":        home_sp_name or "TBD",
            "home_sp_era":    home_sp.get("era"),
            "home_sp_era_adj":home_sp.get("era_adj"),
            "home_sp_fip":    home_sp.get("fip_adj"),
            "home_sp_whip":   home_sp.get("whip"),
            "home_sp_k9":     home_sp.get("k_per_9"),
            "home_sp_missing":home_sp.get("missing", False),

            # Offense
            "away_rpg":        away_offense.get("rpg", LEAGUE["rpg"]),
            "away_ops":        away_offense.get("ops"),
            "away_form_rpg":   away_form["rpg"],
            "away_form_wpct":  away_form["win_pct"],

            "home_rpg":        home_offense.get("rpg", LEAGUE["rpg"]),
            "home_ops":        home_offense.get("ops"),
            "home_form_rpg":   home_form["rpg"],
            "home_form_wpct":  home_form["win_pct"],

            # Projections
            "exp_away":       exp_away,
            "exp_home":       exp_home,
            "exp_total":      exp_total,

            # Pitcher recent form
            "away_sp_trend":  away_sp_recent.get("trend", "N/A"),
            "away_sp_r_era":  away_sp_recent.get("recent_era"),
            "away_sp_gs":     away_sp_recent.get("avg_gs"),
            "home_sp_trend":  home_sp_recent.get("trend", "N/A"),
            "home_sp_r_era":  home_sp_recent.get("recent_era"),
            "home_sp_gs":     home_sp_recent.get("avg_gs"),

            # Platoon splits (ERA vs LHB / RHB)
            "away_era_vs_lhb": sf(away_vs_lhb.get("era")),
            "away_era_vs_rhb": sf(away_vs_rhb.get("era")),
            "home_era_vs_lhb": sf(home_vs_lhb.get("era")),
            "home_era_vs_rhb": sf(home_vs_rhb.get("era")),

            # Umpire
            "hp_ump":         ump_name,
            "ump_factor":     ump_factor,
            "ump_rpg":        ump_data.get("ump_rpg", 9.0),

            # Weather
            "weather_flag":   weather.get("weather_flag", "NORMAL"),
            "wind_component": sf(weather.get("wind_component"), 0),
            "wind_label":     weather.get("wind_label", "N/A"),
            "wind_speed":     sf(weather.get("wind_speed_mph"), 0),
            "temp_f":         sf(weather.get("temp_f"), 70),
            "precip_prob":    sf(weather.get("precip_prob"), 0),
            "has_roof":       weather.get("roof", False),

            # Odds and line movement
            "ml_away_odds":   sf(odds_snap.get("ml_away")),
            "ml_home_odds":   sf(odds_snap.get("ml_home")),
            "total_odds_line":sf(odds_snap.get("total_line")),
            "ml_signal":      movement.get("ml_signal", "NO_DATA"),
            "total_signal":   movement.get("total_signal", "NO_DATA"),
            "sharp_side":     movement.get("sharp_side", ""),
            "ml_move_away":   sf(movement.get("ml_away_move")),
            "ml_move_home":   sf(movement.get("ml_home_move")),
            "total_move":     sf(movement.get("total_move")),
            "ml_adj":         ml_adj,
            "total_adj":      total_adj,
            "rest_ml_adj":    rest_ml_adj,
            "gap_adj":        gap_adj,
            "market_ml_adj":  market_ml_adj,
            "conv_adj":       conv_adj,
            "away_rest":      self.get_rest_days(away, game_date),
            "home_rest":      self.get_rest_days(home, game_date),

            # Bullpen
            "away_bp_era":    away_bp["era"],
            "away_bp_whip":   away_bp["whip"],
            "away_bp_found":  away_bp["found"],
            "home_bp_era":    home_bp["era"],
            "home_bp_whip":   home_bp["whip"],
            "home_bp_found":  home_bp["found"],

            # Lineup OPS (if confirmed)
            "away_lineup_ops": self.get_lineup_ops(game_id_str, "away"),
            "home_lineup_ops": self.get_lineup_ops(game_id_str, "home"),
            "lineup_confirmed": bool(self.lineups.get(game_id_str, {}).get("lineup_confirmed")),

            # Picks
            "home_wp":        home_wp,
            "away_wp":        away_wp,

            "ml_team":        ml_team,
            "ml_side":        ml_side,
            "ml_conf":        ml_conf,

            "total_pick":     total_pick,
            "total_line":     line,
            "total_conf":     total_conf,

            "rl_team":        rl_team,
            "rl_pick":        rl_pick,
            "rl_conf":        rl_conf,
        }

    # ── Game Status Helpers ───────────────────────────────────────────────────
    def _game_is_over(self, game: dict) -> bool:
        """
        Returns True if the game has likely ended based on:
        1. Status field = 'Final'
        2. game_time_utc + 3.5 hours < current UTC time
        3. game_id found in today's scores master
        """
        from datetime import timezone, timedelta

        # Status field check
        status = game.get("status", "").lower()
        if status in ("final", "game over", "completed"):
            return True

        # Time-based check: if start time + 3.5 hrs has passed, likely over
        game_time = game.get("game_time_utc", "")
        if game_time:
            try:
                gt = datetime.strptime(game_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                cutoff = gt + timedelta(hours=3, minutes=30)
                if datetime.now(timezone.utc) > cutoff:
                    return True
            except ValueError:
                pass

        # Check scores master for this game_id
        game_id = str(game.get("game_id", ""))
        if game_id:
            for row in self.scores:
                if str(row.get("game_id", "")) == game_id and \
                   row.get("status", "").lower() == "final":
                    return True

        return False

    def get_today_scores(self, target_date: str = None) -> list:
        """Return completed game scores for target_date for the ticker."""
        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")
        return [r for r in self.scores
                if r.get("game_date") == target_date
                and r.get("status", "").lower() == "final"]

    # ── Score All Games for a Date ────────────────────────────────────────────
    def score_today(self, target_date: str = None) -> tuple:
        """
        Score all games for target_date. Returns (scored_games, actual_date).
        Automatically excludes games that have started or finished.
        Falls back to next available date if no upcoming games found.
        """
        if not self._loaded:
            self.load()

        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")

        all_today = [g for g in self.schedule if g.get("game_date") == target_date]
        games     = [g for g in all_today if not self._game_is_over(g)]

        skipped = len(all_today) - len(games)
        if skipped:
            log.info(f"Filtered out {skipped} completed/in-progress game(s)")

        # If nothing upcoming today, use next available future slate
        if not games:
            future = sorted(set(
                g["game_date"] for g in self.schedule
                if g.get("game_date", "") > target_date
            ))
            if future:
                target_date = future[0]
                games = [g for g in self.schedule
                         if g.get("game_date") == target_date
                         and not self._game_is_over(g)]
                log.info(f"All today's games done -- using next slate: {target_date}")

        log.info(f"Scoring {len(games)} upcoming games for {target_date}")

        scored = []
        for game in games:
            try:
                scored.append(self.score_game(game))
            except Exception as exc:
                log.warning(f"Could not score game {game.get('game_id')}: {exc}")

        return scored, target_date
