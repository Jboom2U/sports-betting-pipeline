"""
mlb_model.py
Core MLB betting model.

Scores each game for moneyline, run line, and totals using:
  - Pitcher season stats and home/away splits (most recent season)
  - Team offensive and pitching stats
  - Park run/HR factors
  - Recent team form (last 10 games)

Pythagorean win expectation model (exponent 1.83) drives win probability.
Expected runs uses: team RPG * pitcher suppression factor * park factor.
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

PYTHAGOREAN_EXP  = 1.83   # baseball Pythagorean exponent
HOME_FIELD_BOOST = 0.025  # small residual home advantage beyond captured stats
RECENT_WEIGHT    = 0.35   # how much recent form (last 10 games) influences expected runs
SEASON_WEIGHT    = 0.65
SPLIT_WEIGHT     = 0.30   # home/away split influence on pitcher ERA
SEASON_ERA_WEIGHT = 0.70


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
        self.pitchers       = {}   # name -> {season -> stats_dict}
        self.pitcher_splits = {}   # name -> {season -> {"home"->dict, "away"->dict}}
        self.team_hitting   = {}   # name -> {season -> stats_dict}
        self.team_pitching  = {}   # name -> {season -> stats_dict}
        self.park_factors   = {}   # venue -> stats_dict
        self.scores         = []   # all historical game rows
        self.schedule       = []   # upcoming schedule rows
        self._loaded        = False

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

        # Park factors: venue -> row
        for row in read_csv(os.path.join(DATA_DIR, "park_factors.csv")):
            venue = row.get("venue", "").strip()
            if venue:
                self.park_factors[venue] = row

        # Historical scores and upcoming schedule
        self.scores   = read_csv(os.path.join(CLEAN_DIR, "mlb_scores_master.csv"))
        self.schedule = read_csv(os.path.join(CLEAN_DIR, "mlb_schedule_master.csv"))

        self._loaded = True
        log.info(f"Loaded: {len(self.pitchers)} pitchers | {len(self.team_hitting)} teams | "
                 f"{len(self.park_factors)} parks | {len(self.scores)} historical games")

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
                 park_run_factor: float, is_home: bool) -> float:
        """
        Expected runs scored by `team` facing `opp_pitcher` at this park.
        Formula: (blended RPG) * pitcher_suppression * park_adj * home_boost
        """
        offense = self.get_offense(team)
        form    = self.recent_form(team)

        # Blend season RPG with recent form
        base_rpg = (SEASON_WEIGHT * offense["rpg"] +
                    RECENT_WEIGHT * form["rpg"])

        # Pitcher suppression: ERA ratio vs league average
        # ERA 3.00 / 4.20 = 0.714 -> team scores 71% of normal
        # ERA 5.50 / 4.20 = 1.310 -> team scores 131% of normal
        era_adj     = opp_pitcher.get("era_adj", LEAGUE["era"])
        suppression = era_adj / LEAGUE["era"]

        # Park adjustment
        park_adj = park_run_factor / 100.0

        # Small home offensive boost
        loc_boost = 1.02 if is_home else 1.0

        expected = base_rpg * suppression * park_adj * loc_boost
        return max(0.5, round(expected, 3))   # floor prevents division by zero

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

        # Expected runs
        exp_away = self.exp_runs(away, home_sp, park_runs, is_home=False)
        exp_home = self.exp_runs(home, away_sp, park_runs, is_home=True)
        exp_total = round(exp_away + exp_home, 2)

        # Pythagorean win probability
        e = PYTHAGOREAN_EXP
        raw_home_wp = (exp_home ** e) / (exp_home ** e + exp_away ** e)
        home_wp = round(min(0.85, max(0.15, raw_home_wp + HOME_FIELD_BOOST)), 4)
        away_wp = round(1.0 - home_wp, 4)

        # Moneyline
        if home_wp >= away_wp:
            ml_team, ml_side, ml_conf = home, "home", home_wp
        else:
            ml_team, ml_side, ml_conf = away, "away", away_wp

        # Totals
        line = 8.5
        diff = exp_total - line
        total_pick = "OVER" if diff > 0 else "UNDER"
        # Confidence scales with deviation from line; capped at 0.74
        total_conf = round(min(0.74, 0.50 + abs(diff) / 7.0), 4)

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

    # ── Score All Games for a Date ────────────────────────────────────────────
    def score_today(self, target_date: str = None) -> tuple:
        """
        Score all games for target_date. Returns (scored_games, actual_date).
        Falls back to next available date if no games found today.
        """
        if not self._loaded:
            self.load()

        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")

        games = [g for g in self.schedule if g.get("game_date") == target_date
                 and g.get("status", "").lower() not in ("final",)]

        # If nothing today, use next available future slate
        if not games:
            future = sorted(set(
                g["game_date"] for g in self.schedule
                if g.get("game_date", "") >= target_date
                and g.get("status", "").lower() not in ("final",)
            ))
            if future:
                target_date = future[0]
                games = [g for g in self.schedule if g.get("game_date") == target_date
                         and g.get("status", "").lower() not in ("final",)]
                log.info(f"Using next available slate: {target_date}")

        log.info(f"Scoring {len(games)} games for {target_date}")

        scored = []
        for game in games:
            try:
                scored.append(self.score_game(game))
            except Exception as exc:
                log.warning(f"Could not score game {game.get('game_id')}: {exc}")

        return scored, target_date
