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

        # Historical scores and upcoming schedule
        self.scores   = read_csv(os.path.join(CLEAN_DIR, "mlb_scores_master.csv"))
        self.schedule = read_csv(os.path.join(CLEAN_DIR, "mlb_schedule_master.csv"))

        self._loaded = True
        log.info(f"Loaded: {len(self.pitchers)} pitchers | {len(self.team_hitting)} teams | "
                 f"{len(self.park_factors)} parks | {len(self.scores)} historical games | "
                 f"{len(self.weather)} weather records")

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
                 weather: dict = None) -> float:
        """
        Expected runs scored by `team` facing `opp_pitcher` at this park.
        Formula: (blended RPG) * pitcher_suppression * park_adj * home_boost * weather_adj
        """
        offense = self.get_offense(team)
        form    = self.recent_form(team)

        # Blend season RPG with recent form
        base_rpg = (SEASON_WEIGHT * offense["rpg"] +
                    RECENT_WEIGHT * form["rpg"])

        # Blend season ERA with recent starts ERA
        era_season = opp_pitcher.get("era_adj", LEAGUE["era"])
        pitcher_name = opp_pitcher.get("name", "")
        recent_sp = self.get_recent_form_pitcher(pitcher_name)
        if recent_sp and recent_sp.get("recent_era"):
            era_eff = (SEASON_ERA_VS_RECENT * era_season +
                       RECENT_STARTS_WEIGHT * recent_sp["recent_era"])
        else:
            era_eff = era_season

        suppression = era_eff / LEAGUE["era"]
        park_adj    = park_run_factor / 100.0
        loc_boost   = 1.02 if is_home else 1.0

        expected = base_rpg * suppression * park_adj * loc_boost

        # Weather adjustments (applied to each team's expected runs)
        if weather and not weather.get("roof"):
            wc      = sf(weather.get("wind_component"), 0)
            temp    = sf(weather.get("temp_f"), 70)
            precip  = sf(weather.get("precip_prob"), 0)

            # Wind: each mph blowing out adds WIND_RUNS_PER_MPH to expected total
            # We apply half per team
            wind_adj = 1.0 + (wc * WIND_RUNS_PER_MPH * 0.5 / max(base_rpg, 1))

            # Cold: reduce expected runs when below 65°F
            cold_adj = 1.0
            if temp < 65:
                cold_adj = max(0.85, 1.0 - (65 - temp) * COLD_PENALTY)

            # Precipitation: slight suppression if rain likely
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

        # Expected runs
        exp_away = self.exp_runs(away, home_sp, park_runs, is_home=False, weather=weather)
        exp_home = self.exp_runs(home, away_sp, park_runs, is_home=True,  weather=weather)
        exp_total = round(exp_away + exp_home, 2)

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

        # Totals
        line = 8.5
        diff = exp_total - line
        total_pick      = "OVER" if diff > 0 else "UNDER"
        total_conf_base = min(0.74, 0.50 + abs(diff) / 7.0)

        # Line movement confidence adjustments
        odds_snap   = self.get_odds(away, home)
        movement    = self.get_movement(away, home)
        ml_adj, total_adj = self.line_movement_confidence_adj(
            away, home, ml_team, total_pick
        )

        ml_conf    = round(min(0.90, max(0.10, ml_conf_base + ml_adj)), 4)
        total_conf = round(min(0.80, max(0.10, total_conf_base + total_adj)), 4)

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
                log.info(f"All today's games done — using next slate: {target_date}")

        log.info(f"Scoring {len(games)} upcoming games for {target_date}")

        scored = []
        for game in games:
            try:
                scored.append(self.score_game(game))
            except Exception as exc:
                log.warning(f"Could not score game {game.get('game_id')}: {exc}")

        return scored, target_date
