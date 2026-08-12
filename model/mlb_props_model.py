"""
model/mlb_props_model.py
Player prop probability engine for MLB betting.

Prop types supported:
  HR   — player hits 0.5+ home runs  (Hard Rock Bet line)
  HITS — player records 0.5+ hits
  TB   — total bases over 1.5
  RBI  — RBIs over 0.5
  R    — runs scored over 0.5
  SB   — stolen bases over 0.5
  K    — starting pitcher strikeout total (over/under a given number)

Confidence scoring:
  Each prop returns a dict with:
    player_name, prop_type, line, proj, confidence (0-1),
    tier (LOCK/STRONG/LEAN), reasoning

Tier thresholds vary by prop type — see constants below.
"""

import os, json, math, logging, csv
from datetime import datetime
from zoneinfo import ZoneInfo

# Baseball days are ET days. Railway runs Python in UTC, so a bare
# datetime.now() rolls to tomorrow at 8pm ET and the pick path then looks for
# tomorrow's dated files. Always resolve "today" in ET.
_ET_TZ = ZoneInfo("America/New_York")


def _today_et() -> str:
    """Today's date in ET, as YYYY-MM-DD."""
    return datetime.now(_ET_TZ).strftime("%Y-%m-%d")


log = logging.getLogger(__name__)

SEASON   = datetime.now().year
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# League-average baselines (2024 MLB)
LEAGUE_HR_PER_PA  = 0.034   # ~1 HR per 29 PA
LEAGUE_H_PER_PA   = 0.270   # batting average proxy
LEAGUE_TB_PER_PA  = 0.400   # total bases per PA
LEAGUE_RBI_PER_PA = 0.110   # RBI per PA
LEAGUE_R_PER_PA   = 0.130   # runs scored per PA
LEAGUE_SB_PER_PA  = 0.020   # stolen bases per PA
LEAGUE_K9_SP      = 9.1     # average SP K/9

# Park HR factors from park_factors.csv (loaded on demand)
_PARK_HR_FACTORS: dict[str, float] = {}

# Tier cutoffs — game picks
LOCK_THRESH   = 0.68
STRONG_THRESH = 0.62
LEAN_THRESH   = 0.55

# HR-specific tier cutoffs — calibrated to Poisson reality
# Even elite sluggers max out at ~22% per-game probability,
# so we rank relative to a league-average baseline (~11%).
HR_LOCK_THRESH   = 0.20   # ~1.8x league avg — elite spot
HR_STRONG_THRESH = 0.15   # ~1.35x league avg — solid edge
HR_LEAN_THRESH   = 0.12   # ~1.1x league avg — slight lean

# Total Bases Over 1.5 — typical hitter ~45-55%; elite spots reach 65%
TB_LOCK_THRESH   = 0.62
TB_STRONG_THRESH = 0.55
TB_LEAN_THRESH   = 0.48

# RBI Over 0.5 — league avg ~32%; strong spots ~45-52%
RBI_LOCK_THRESH   = 0.50
RBI_STRONG_THRESH = 0.42
RBI_LEAN_THRESH   = 0.35

# Runs Scored Over 0.5 — league avg ~38%; leadoff spots ~48%
R_LOCK_THRESH   = 0.50
R_STRONG_THRESH = 0.42
R_LEAN_THRESH   = 0.35

# Stolen Bases Over 0.5 — only speedsters crack 25%+
SB_LOCK_THRESH   = 0.28
SB_STRONG_THRESH = 0.20
SB_LEAN_THRESH   = 0.13


def _load_park_factors():
    global _PARK_HR_FACTORS
    if _PARK_HR_FACTORS:
        return
    path = os.path.join(DATA_DIR, "park_factors.csv")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            team  = row.get("team", "").strip()
            try:
                hr_f  = float(row.get("hr_factor", 1.0) or 1.0)
            except ValueError:
                hr_f  = 1.0
            _PARK_HR_FACTORS[team] = hr_f


def _tier(conf: float) -> str:
    if conf >= LOCK_THRESH:   return "LOCK"
    if conf >= STRONG_THRESH: return "STRONG"
    if conf >= LEAN_THRESH:   return "LEAN"
    return "SKIP"


def _hr_tier(conf: float) -> str:
    """HR-calibrated tier using lower absolute thresholds."""
    if conf >= HR_LOCK_THRESH:   return "LOCK"
    if conf >= HR_STRONG_THRESH: return "STRONG"
    if conf >= HR_LEAN_THRESH:   return "LEAN"
    return "SKIP"




# ─────────────────────────────────────────────────────────────────────────────
# TRAILING HIT RATE BLEND
# ─────────────────────────────────────────────────────────────────────────────

def _apply_trailing_hit_rate(player_name: str, prop_type: str, line,
                              base_conf: float, days: int = 30) -> tuple[float, float | None]:
    """
    Blend model base confidence with player's trailing hit rate.
    Returns (adjusted_conf, trailing_hit_rate_or_None).

    Only applied when >= 5 graded results exist for this player/prop/line.
    Blend: 0.70 * base_conf + 0.30 * trailing_hit_rate
    Bonus +2% if trailing >= 0.65; penalty -3% if trailing <= 0.40.
    Cap: [0.40, 0.92].
    """
    try:
        from db.picks_store import get_player_trailing_hit_rate
        thr = get_player_trailing_hit_rate(player_name, prop_type, line, days=days)
    except Exception:
        return base_conf, None

    if thr is None:
        return base_conf, None

    blended = 0.70 * base_conf + 0.30 * thr
    if thr >= 0.65:
        blended += 0.02
    elif thr <= 0.40:
        blended -= 0.03

    blended = max(0.40, min(0.92, blended))

    if abs(blended - base_conf) >= 0.005:
        log.info(
            f"Trailing hit rate for {player_name} {prop_type} {line}: "
            f"{thr:.1%} (last {days}d) | conf {base_conf:.1%} -> {blended:.1%}"
        )

    return round(blended, 4), thr


# ─────────────────────────────────────────────────────────────────────────────
# HOME RUN PROPS
# ─────────────────────────────────────────────────────────────────────────────

def _poisson_p_at_least_one(lam: float) -> float:
    """P(X >= 1) for Poisson with mean lam."""
    if lam <= 0:
        return 0.0
    return 1.0 - math.exp(-lam)


def score_hr_prop(player: dict, pitcher_opp: dict, home_team: str,
                  is_home: bool, weather: dict = None) -> dict | None:
    """
    Score a home run prop for one player.
    Uses a Poisson model:
        lambda = PA_expected × adj_hr_per_pa

    Adjustments applied:
        1. Pitcher HR/9 vs league average (pitcher factor)
        2. Park HR factor
        3. Wind component (WIND_OUT +12%, WIND_IN -10%)
        4. Home/away split if available
    """
    _load_park_factors()

    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None   # insufficient sample

    # Base rate — prefer platoon-adjusted if we know pitcher hand
    # For now use season rate; platoon can be added when pitcher hand is stored
    base_rate = player.get("hr_per_pa", LEAGUE_HR_PER_PA)

    # Home/away adjustment
    if is_home:
        ha_rate = player.get("home_hr_per_pa", base_rate)
    else:
        ha_rate = player.get("away_hr_per_pa", base_rate)
    # Blend 60/40 season vs split
    hr_rate = 0.60 * base_rate + 0.40 * ha_rate

    # Pitcher factor: pitcher's HR/9 vs league avg HR/9 (≈1.20)
    pitcher_hr9    = pitcher_opp.get("hr_per_9", 1.20)
    pitcher_factor = pitcher_hr9 / 1.20  # >1 = gives up more HRs
    pitcher_factor = max(0.5, min(2.0, pitcher_factor))

    # Park factor
    park_factor = _PARK_HR_FACTORS.get(home_team, 1.0)

    # Weather
    wind_factor = 1.0
    wind_note   = ""
    if weather:
        flag = weather.get("weather_flag", "NORMAL")
        if flag == "WIND_OUT":
            wind_factor = 1.12
            wind_note   = "Wind blowing out"
        elif flag == "WIND_IN":
            wind_factor = 0.90
            wind_note   = "Wind blowing in"
        elif flag == "COLD":
            wind_factor = 0.95
            wind_note   = "Cold temps suppress HRs"

    # Expected PA per game (lineup position matters — top 3 get ~4.2 PA, 7-9 get ~3.5 PA)
    batting_order = player.get("batting_order", 5)
    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = hr_rate * pitcher_factor * park_factor * wind_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_one(lam)

    # Build reasoning
    parts = []
    if pitcher_factor >= 1.15:
        parts.append(f"pitcher allows {pitcher_hr9:.2f} HR/9 (above avg)")
    elif pitcher_factor <= 0.85:
        parts.append(f"pitcher suppresses HRs ({pitcher_hr9:.2f} HR/9)")
    if park_factor >= 1.10:
        parts.append(f"hitter-friendly park (HR factor {park_factor:.2f})")
    elif park_factor <= 0.90:
        parts.append(f"pitcher park (HR factor {park_factor:.2f})")
    if wind_note:
        parts.append(wind_note)
    parts.append(f"{pa} PA sample this season ({base_rate:.3f} HR/PA)")

    reasoning = " | ".join(parts) if parts else f"{pa} PA, {base_rate:.3f} HR/PA"

    tier = _hr_tier(prob)   # HR-calibrated thresholds, not game-pick thresholds
    if tier == "SKIP":
        return None

    return {
        "prop_type":     "HR",
        "line":          0.5,
        "player_name":   pname,
        "batting_order": batting_order,
        "proj":          round(lam, 3),
        "confidence":    round(prob, 4),
        "tier":          tier,
        "reasoning":     reasoning,
        # Explicit fields for HR Watch display
        "hr_park_factor":  round(park_factor, 3),
        "hr_pitcher_hr9":  round(pitcher_hr9, 2),
        "hr_wind_note":    wind_note,
        "hr_base_rate":    round(base_rate, 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HITS PROPS
# ─────────────────────────────────────────────────────────────────────────────

def score_hits_prop(player: dict, pitcher_opp: dict,
                    is_home: bool) -> dict | None:
    """
    Score a 0.5 hits prop for one hitter.
    Poisson model: lambda = PA_expected × adj_h_per_pa
    """
    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None

    base_rate = player.get("h_per_pa", LEAGUE_H_PER_PA)

    # Home/away split blend — floor at base_rate/2 to prevent early-season
    # 0-split data from collapsing HITS probability (e.g. 0 hits in 12 home PAs)
    if is_home:
        ha_rate = player.get("home_h_per_pa", base_rate)
    else:
        ha_rate = player.get("away_h_per_pa", base_rate)
    ha_rate = ha_rate if ha_rate > 0.100 else base_rate  # ignore near-zero splits
    h_rate = 0.60 * base_rate + 0.40 * ha_rate

    # Pitcher factor: pitcher's H/9 vs league avg H/9 (≈8.5)
    pitcher_h9     = pitcher_opp.get("h_per_9", 8.5)
    pitcher_factor = pitcher_h9 / 8.5
    pitcher_factor = max(0.6, min(1.6, pitcher_factor))

    # Expected PA
    batting_order = player.get("batting_order", 5)
    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = h_rate * pitcher_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_one(lam)

    # Reasoning
    parts = []
    if base_rate >= 0.290:
        parts.append(f"high contact hitter ({base_rate:.3f} H/PA)")
    elif base_rate <= 0.230:
        parts.append(f"low contact hitter ({base_rate:.3f} H/PA)")
    if pitcher_factor >= 1.12:
        parts.append(f"pitcher allows lots of hits ({pitcher_h9:.1f} H/9)")
    elif pitcher_factor <= 0.88:
        parts.append(f"stingy pitcher ({pitcher_h9:.1f} H/9)")
    pitcher_avg = pitcher_opp.get("opp_avg", 0)
    if pitcher_avg:
        parts.append(f"opp BA .{int(pitcher_avg*1000):03d}")
    if not parts:
        parts.append(f"{pa} PA, {base_rate:.3f} H/PA")

    reasoning = " | ".join(parts)
    tier      = _tier(prob)
    if tier == "SKIP":
        return None

    return {
        "prop_type":   "HITS",
        "line":        0.5,
        "player_name": pname,
        "batting_order": batting_order,
        "proj":        round(lam, 3),
        "confidence":  round(prob, 4),
        "tier":        tier,
        "reasoning":   reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# TOTAL BASES PROPS  (Over 1.5)
# ─────────────────────────────────────────────────────────────────────────────

def _poisson_p_at_least_two(lam: float) -> float:
    """P(X >= 2) for Poisson with mean lam."""
    if lam <= 0:
        return 0.0
    return 1.0 - math.exp(-lam) - lam * math.exp(-lam)


def score_tb_prop(player: dict, pitcher_opp: dict, is_home: bool) -> dict | None:
    """
    Score a Total Bases Over 1.5 prop.
    Poisson model: lambda = PA_expected × adj_tb_per_pa
    P(TB >= 2) = 1 - e^-λ - λe^-λ
    """
    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None

    base_rate = player.get("tb_per_pa", LEAGUE_TB_PER_PA)

    # Pitcher factor: opponent SLG vs league avg SLG (~.410)
    opp_slg       = pitcher_opp.get("opp_slg", 0.410)
    pitcher_factor = opp_slg / 0.410
    pitcher_factor = max(0.7, min(1.5, pitcher_factor))

    batting_order = player.get("batting_order", 5)
    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = base_rate * pitcher_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_two(lam)

    # Build reasoning
    parts = []
    if base_rate >= 0.460:
        parts.append(f"power hitter ({base_rate:.3f} TB/PA)")
    elif base_rate <= 0.340:
        parts.append(f"low power ({base_rate:.3f} TB/PA)")
    if opp_slg:
        if pitcher_factor >= 1.12:
            parts.append(f"pitcher gives up extra-base hits (opp SLG .{int(opp_slg*1000):03d})")
        elif pitcher_factor <= 0.88:
            parts.append(f"pitcher limits damage (opp SLG .{int(opp_slg*1000):03d})")
    if not parts:
        parts.append(f"{pa} PA, {base_rate:.3f} TB/PA, proj {lam:.2f} TB")

    reasoning = " | ".join(parts)

    if base_rate >= TB_LOCK_THRESH:      tier = "LOCK"
    elif base_rate >= TB_STRONG_THRESH:  tier = "STRONG"  # wrong — use prob
    else: tier = None

    if prob >= TB_LOCK_THRESH:   tier = "LOCK"
    elif prob >= TB_STRONG_THRESH: tier = "STRONG"
    elif prob >= TB_LEAN_THRESH:   tier = "LEAN"
    else:
        return None

    return {
        "prop_type":     "TB",
        "line":          1.5,
        "player_name":   pname,
        "batting_order": batting_order,
        "proj":          round(lam, 3),
        "confidence":    round(prob, 4),
        "tier":          tier,
        "reasoning":     reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RBI PROPS  (Over 0.5)
# ─────────────────────────────────────────────────────────────────────────────

def score_rbi_prop(player: dict, pitcher_opp: dict, is_home: bool) -> dict | None:
    """
    Score an RBI Over 0.5 prop.
    Poisson model: lambda = PA_expected × adj_rbi_per_pa
    Adjusted for batting order (cleanup hitters get more RBI chances).
    """
    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None

    base_rate = player.get("rbi_per_pa", LEAGUE_RBI_PER_PA)

    # Batting order adjustment: 3-5 hitters have more RBI opps (runners on base)
    batting_order = player.get("batting_order", 5)
    order_factor  = 1.0
    if batting_order in (3, 4, 5):
        order_factor = 1.15
    elif batting_order in (1, 2):
        order_factor = 0.85
    elif batting_order in (8, 9):
        order_factor = 0.90

    # Pitcher contact factor: higher opp AVG = more runners = more RBI chances
    opp_avg       = pitcher_opp.get("opp_avg", 0.255)
    pitcher_factor = opp_avg / 0.255 if opp_avg > 0 else 1.0
    pitcher_factor = max(0.75, min(1.35, pitcher_factor))

    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = base_rate * order_factor * pitcher_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_one(lam)

    parts = []
    if base_rate >= 0.140:
        parts.append(f"strong RBI producer ({base_rate:.3f} RBI/PA)")
    if batting_order in (3, 4, 5):
        parts.append("cleanup spot, more runners on")
    if pitcher_factor >= 1.10:
        parts.append(f"pitcher lets runners reach (opp AVG .{int(opp_avg*1000):03d})")
    if not parts:
        parts.append(f"{pa} PA, {base_rate:.3f} RBI/PA, proj {lam:.2f}")

    reasoning = " | ".join(parts)

    if prob >= RBI_LOCK_THRESH:   tier = "LOCK"
    elif prob >= RBI_STRONG_THRESH: tier = "STRONG"
    elif prob >= RBI_LEAN_THRESH:   tier = "LEAN"
    else:
        return None

    return {
        "prop_type":     "RBI",
        "line":          0.5,
        "player_name":   pname,
        "batting_order": batting_order,
        "proj":          round(lam, 3),
        "confidence":    round(prob, 4),
        "tier":          tier,
        "reasoning":     reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RUNS SCORED PROPS  (Over 0.5)
# ─────────────────────────────────────────────────────────────────────────────

def score_runs_prop(player: dict, pitcher_opp: dict, is_home: bool) -> dict | None:
    """
    Score a Runs Scored Over 0.5 prop.
    Poisson model: lambda = PA_expected × adj_r_per_pa
    Leadoff hitters score more runs; pitcher WHIP/OBP affects how many runners score.
    """
    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None

    base_rate = player.get("r_per_pa", LEAGUE_R_PER_PA)

    # Batting order: leadoff & 2-hole score runs at higher rates
    batting_order = player.get("batting_order", 5)
    order_factor  = 1.0
    if batting_order == 1:
        order_factor = 1.20
    elif batting_order == 2:
        order_factor = 1.10
    elif batting_order in (7, 8, 9):
        order_factor = 0.88

    # Pitcher OBP allowed — higher = more runners, more runs
    opp_obp       = pitcher_opp.get("opp_obp", 0.320)
    pitcher_factor = opp_obp / 0.320 if opp_obp > 0 else 1.0
    pitcher_factor = max(0.75, min(1.35, pitcher_factor))

    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = base_rate * order_factor * pitcher_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_one(lam)

    parts = []
    if batting_order == 1:
        parts.append("leadoff spot — most runs scored chances")
    elif batting_order == 2:
        parts.append("2-hole — solid run-scoring position")
    if base_rate >= 0.160:
        parts.append(f"high run scorer ({base_rate:.3f} R/PA)")
    if pitcher_factor >= 1.10:
        parts.append(f"pitcher walks/hits batters (OBP .{int(opp_obp*1000):03d})")
    if not parts:
        parts.append(f"{pa} PA, {base_rate:.3f} R/PA, proj {lam:.2f}")

    reasoning = " | ".join(parts)

    if prob >= R_LOCK_THRESH:   tier = "LOCK"
    elif prob >= R_STRONG_THRESH: tier = "STRONG"
    elif prob >= R_LEAN_THRESH:   tier = "LEAN"
    else:
        return None

    return {
        "prop_type":     "R",
        "line":          0.5,
        "player_name":   pname,
        "batting_order": batting_order,
        "proj":          round(lam, 3),
        "confidence":    round(prob, 4),
        "tier":          tier,
        "reasoning":     reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STOLEN BASE PROPS  (Over 0.5)
# ─────────────────────────────────────────────────────────────────────────────

def score_sb_prop(player: dict, pitcher_opp: dict, is_home: bool) -> dict | None:
    """
    Score a Stolen Bases Over 0.5 prop.
    Poisson model: lambda = PA_expected × adj_sb_per_pa
    Only speedsters (sb_per_pa > 0.04) typically qualify.
    """
    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None

    base_rate = player.get("sb_per_pa", LEAGUE_SB_PER_PA)

    # Skip players who clearly don't run — at minimum ~0.04 SB/PA to bother
    if base_rate < 0.030:
        return None

    # Pitcher WHIP factor — lower WHIP = fewer base runners = fewer SB attempts
    whip          = pitcher_opp.get("whip", 1.30)
    pitcher_factor = whip / 1.30 if whip > 0 else 1.0
    pitcher_factor = max(0.75, min(1.40, pitcher_factor))

    batting_order = player.get("batting_order", 5)
    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = base_rate * pitcher_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_one(lam)

    parts = []
    if base_rate >= 0.080:
        parts.append(f"elite base stealer ({base_rate:.3f} SB/PA)")
    elif base_rate >= 0.050:
        parts.append(f"active on bases ({base_rate:.3f} SB/PA)")
    else:
        parts.append(f"{base_rate:.3f} SB/PA this season")
    if pitcher_factor >= 1.12:
        parts.append(f"pitcher doesn't hold runners well (WHIP {whip:.2f})")
    if not parts:
        parts.append(f"{pa} PA, {base_rate:.3f} SB/PA, proj {lam:.2f}")

    reasoning = " | ".join(parts)

    if prob >= SB_LOCK_THRESH:   tier = "LOCK"
    elif prob >= SB_STRONG_THRESH: tier = "STRONG"
    elif prob >= SB_LEAN_THRESH:   tier = "LEAN"
    else:
        return None

    return {
        "prop_type":     "SB",
        "line":          0.5,
        "player_name":   pname,
        "batting_order": batting_order,
        "proj":          round(lam, 3),
        "confidence":    round(prob, 4),
        "tier":          tier,
        "reasoning":     reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PITCHER STRIKEOUT PROPS
# ─────────────────────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Approximate standard normal CDF."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


# Flip to True only AFTER side-aware prop grading exists (side column +
# db/picks_store.py hit-rate queries keyed on picked side, not 'OVER').
ALLOW_UNDER_K = True   # enabled 2026-07-22: side-aware grading landed (pick_side column + side-aware hit queries)


def score_k_prop(pitcher_name: str, pitcher_stats: dict,
                 opp_team_k_rate: float,
                 innings_expected: float = 5.5,
                 line: float = 5.5,
                 weather: dict = None,
                 over_price: int = None,
                 under_price: int = None) -> dict | None:
    """
    Score a pitcher strikeout over/under prop.

    Model:
        proj_k = (pitcher_k9 / 9) × innings_expected × team_k_rate_adj

    Uses normal distribution around projection to compute P(K > line).
    """
    k9 = float(pitcher_stats.get("k9",
              pitcher_stats.get("k_per_9",
              pitcher_stats.get("strikeoutsPer9Inn", 0))) or 0)
    if k9 < 1.0:
        return None   # no data

    # Opener / short-outing guard. A Pinnacle K line at/below 2.5 means the book
    # expects a ~1-2 inning outing (opener or bulk reliever), but this model
    # projects a FULL start (innings_expected=5.5). That mismatch produced fake
    # locks like "Braydon Fisher Over 0.5, proj 4.83 (94%)". Real starters sit at
    # 3.5+; below that the starter projection is invalid, so skip.
    if line <= 2.5:
        return None

    # Opponent team K rate adjustment vs league average (≈22%)
    league_k_rate  = 0.220
    opp_k_factor   = opp_team_k_rate / league_k_rate if opp_team_k_rate > 0 else 1.0
    opp_k_factor   = max(0.7, min(1.4, opp_k_factor))

    # Weather: COLD reduces K rate slightly
    weather_factor = 1.0
    if weather and weather.get("weather_flag") == "COLD":
        weather_factor = 0.97

    proj_k_per_9 = k9 * opp_k_factor * weather_factor
    proj_k       = (proj_k_per_9 / 9.0) * innings_expected

    # Std dev for K props — roughly sqrt(proj_k) × 1.1
    std_dev = max(1.2, math.sqrt(proj_k) * 1.1)

    # P(K > line) using normal approximation, continuity correction
    z    = (line + 0.5 - proj_k) / std_dev
    prob = 1.0 - _norm_cdf(z)   # P(K > line)

    # Score BOTH directions against the REAL line (0.5 lines => no push, so
    # P(under) = 1 - P(over)). Bet the side the projection favors; that is the
    # honest read now that `line` is Pinnacle's actual number, not 0.8x proj.
    p_over  = prob
    p_under = 1.0 - p_over
    if proj_k >= line:
        side, conf, price = "OVER", p_over, over_price
    else:
        side, conf, price = "UNDER", p_under, under_price

    # OVER-ONLY GATE (temporary). The line is now Pinnacle's real number and both
    # directions ARE computed, but player_prop_history has no `side` column and
    # the hit-rate queries in db/picks_store.py hardcode result='OVER'=hit. So an
    # UNDER pick would grade wrong. Ship real-line OVERS now (they grade correctly);
    # flip ALLOW_UNDER once side-aware grading lands (schema + 3 SQL rewrites).
    if side == "UNDER" and not ALLOW_UNDER_K:
        return None

    tier = _tier(conf)
    if tier == "SKIP":
        return None

    # Expected value vs the real American price, when we have it.
    ev = None
    if price is not None:
        try:
            payout = (price / 100.0) if price > 0 else (100.0 / abs(price))
            ev = round(conf * payout - (1 - conf), 4)   # per 1u risked
        except (TypeError, ZeroDivisionError):
            ev = None

    parts = []
    if k9 >= 10.5:
        parts.append(f"elite swing-miss stuff ({k9:.1f} K/9)")
    elif k9 >= 8.5:
        parts.append(f"above-avg strikeout rate ({k9:.1f} K/9)")
    else:
        parts.append(f"{k9:.1f} K/9 this season")
    if opp_k_factor >= 1.10:
        parts.append(f"opponent strikes out a lot ({opp_team_k_rate:.1%})")
    elif opp_k_factor <= 0.90:
        parts.append(f"opponent makes contact ({opp_team_k_rate:.1%} K rate)")
    parts.append(f"proj {proj_k:.1f} Ks vs Pinnacle line {line} -> {side}")
    if ev is not None:
        parts.append(f"EV {ev:+.2f}u at {price:+d}")

    return {
        "prop_type":   "K",
        "line":        line,
        "pick_side":   side,            # OVER / UNDER (bet direction)
        "label":       f"{side} {line}",
        "price":       price,
        "over_price":  over_price,
        "under_price": under_price,
        "ev":          ev,
        "player_name": pitcher_name,
        "proj":        round(proj_k, 2),
        "confidence":  round(conf, 4),
        "tier":        tier,
        "reasoning":   " | ".join(parts),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCORER — combines everything for a date
# ─────────────────────────────────────────────────────────────────────────────

def _knorm(n: str) -> str:
    """Normalize a pitcher name for matching Pinnacle <-> schedule."""
    return " ".join((n or "").strip().split()).lower()


# ─────────────────────────────────────────────────────────────────────────────
# REAL BOOK LINES FOR BATTER PROPS  (added 2026-08-11)
# ─────────────────────────────────────────────────────────────────────────────
# Until now every batter prop was scored against a line the MODEL invented:
# "line": 0.5 for HR/HITS/RBI/R/SB and 1.5 for TB, hardcoded, with no price at
# all. So a 68% hit rate on Over 0.5 Hits was recorded as a win when real books
# price that around -250 to -350, where 68% LOSES money. The record measured
# beating a made-up number.
#
# Pinnacle's free guest feed carries real two-way markets. Verified live on
# 2026-08-11 via /admin/pinnacle-props-scan, all 100% parseable:
#     Bases 266 (0.5-1.5), Home Runs 120 (0.5), Strikeouts 29 (2.5-7.5),
#     Hits Allowed 27 (3.5-6.5), Pitching Outs 24 (14.5-18.5)
# There is NO batter "Hits" market. Do not synthesize one.
#
# A prop is only BETTABLE when it has a real line AND both prices. Everything
# else stays a research projection. Missing means missing.


def _poisson_p_over(lam: float, line: float) -> float:
    """P(X > line) for Poisson(lam) on a half-integer line.

    Over 0.5 means at least 1. Over 1.5 means at least 2. Over 2.5 means at
    least 3. Works for any .5 line, which is all the book offers.
    """
    if lam <= 0:
        return 0.0
    k = int(math.floor(line)) + 1          # 0.5 -> 1, 1.5 -> 2, 2.5 -> 3
    cum = 0.0                               # P(X <= k-1)
    term = math.exp(-lam)
    for i in range(0, k):
        if i > 0:
            term *= lam / i
        cum += term
    return max(0.0, min(1.0, 1.0 - cum))


def _american_to_decimal(a):
    if a in (None, 0, ""):
        return None
    a = float(a)
    if abs(a) < 100:        # impossible American price = corrupt data
        return None
    return 1 + (a / 100.0 if a > 0 else 100.0 / abs(a))


def _prop_ev(prob: float, american) -> float | None:
    d = _american_to_decimal(american)
    if d is None or prob is None:
        return None
    return prob * (d - 1.0) - (1.0 - prob)


def _load_pinnacle_props(date: str) -> dict:
    """Load persisted real prop lines: {prop_key: {normalized_name: {...}}}."""
    out = {}
    try:
        from scrapers.mlb_pinnacle_scraper import load_prop_lines
        raw = load_prop_lines(date) or {}
        for key, players in raw.items():
            out[key] = {_knorm(n): d for n, d in players.items()}
    except Exception as _e:
        log.warning(f"Pinnacle prop lines load failed (non-fatal): {_e}")
    log.info("Pinnacle prop lines loaded: %s", {k: len(v) for k, v in out.items()})
    return out


# Model prop_type -> Pinnacle market key. Only these two batter markets exist
# with real lines. HITS, RBI, R and SB have NO real market and stay projections.
# HR and TB come from Pinnacle (free, automatic, every slate).
# HITS, RBI and R only exist if someone pulled them from the Odds API via
# /admin/props-pull, which costs a credit per market per game. When absent the
# prop keeps its invented line and stays a research projection.
PROP_TYPE_TO_BOOK = {"HR": "HR", "TB": "TB", "HITS": "HITS", "RBI": "RBI", "R": "R"}

# Never lay more than this on a prop. Props are the least reliable output in the
# model, and heavy juice turns a small estimation error into a large loss.
PROP_PRICE_FLOOR = -250

# Largest tolerated disagreement with the de-vigged market before we assume the
# MODEL is wrong rather than the market. Pinnacle prices props tightly.
MAX_PROP_MARKET_GAP = 0.12


def reprice_prop_with_book(prop: dict, book: dict) -> dict:
    """
    Re-score a prop against the REAL book line and prices.

    The scorers compute a Poisson rate (`proj`) against their own invented line.
    Given the real line we recompute P(over) from that same rate, price BOTH
    directions, and take the side with positive EV. If neither side is +EV the
    prop stays visible but is marked unbettable — that is a real answer, not a
    failure.

    Mutates and returns the prop dict. Adds:
      book_line, over_price, under_price, pick_side, ev, bettable, model_line
    """
    lam = prop.get("proj")
    line = book.get("line")
    op, up = book.get("over_price"), book.get("under_price")
    if lam is None or line is None or op is None or up is None:
        return prop

    p_over = _poisson_p_over(float(lam), float(line))
    p_under = 1.0 - p_over
    ev_o, ev_u = _prop_ev(p_over, op), _prop_ev(p_under, up)
    if ev_o is None and ev_u is None:
        return prop

    if (ev_o or -9) >= (ev_u or -9):
        side, prob, price, ev = "OVER", p_over, op, ev_o
    else:
        side, prob, price, ev = "UNDER", p_under, up, ev_u

    # ── Guards. EV alone is not enough on props. ──────────────────────────────
    # 1. PRICE FLOOR. Laying -514 on an 86.5% model estimate shows +3.3% EV, but
    #    a 3-point model error there wipes it out and you lose 5 units to win 1.
    #    Props are the least reliable part of this model; do not lay heavy juice
    #    on it. (Real example: Corbin Carroll HR under at -514 on 2026-08-11.)
    # 2. MIRAGE GUARD. If the model disagrees with the market by more than
    #    MAX_PROP_MARKET_GAP, the model is far more likely to be wrong than the
    #    market. That HR case had the model at 13.5% against a market implied
    #    23% — a 9.5 point gap on a market Pinnacle prices tightly. Betting the
    #    other side of your own large disagreement is not edge, it is a bug bet.
    #    Same logic already protects the Best Bets surface.
    mkt_prob = None
    d_pick = _american_to_decimal(price)
    d_other = _american_to_decimal(up if side == "OVER" else op)
    if d_pick and d_other:
        ip, io_ = 1.0 / d_pick, 1.0 / d_other
        if ip + io_ > 0:
            mkt_prob = ip / (ip + io_)          # de-vigged market probability

    blocked = None
    if price is not None and float(price) < PROP_PRICE_FLOOR:
        blocked = f"price {price} below floor {PROP_PRICE_FLOOR}"
    elif mkt_prob is not None and abs(prob - mkt_prob) > MAX_PROP_MARKET_GAP:
        blocked = (f"model {prob*100:.0f}% vs market {mkt_prob*100:.0f}% "
                   f"({abs(prob-mkt_prob)*100:.0f} pt gap, likely model error)")

    prop["model_line"]  = prop.get("line")      # keep what it used to claim
    prop["line"]        = float(line)
    prop["book_line"]   = float(line)
    prop["over_price"]  = op
    prop["under_price"] = up
    prop["pick_side"]   = side
    prop["confidence"]  = round(prob, 4)
    prop["ev"]          = round(ev, 4) if ev is not None else None
    prop["price"]       = price
    prop["market_prob"] = round(mkt_prob, 4) if mkt_prob is not None else None
    prop["blocked"]     = blocked
    prop["bettable"]    = bool(ev is not None and ev > 0 and blocked is None)
    prop["reasoning"]   = (prop.get("reasoning", "") +
                           f" | REAL Pinnacle line {line} {side} "
                           f"{'+' if price > 0 else ''}{price}, EV "
                           f"{ev*100:+.1f}%" if ev is not None else prop.get("reasoning", ""))
    return prop


def _load_pinnacle_k(date: str) -> dict:
    """Load persisted Pinnacle K lines keyed by normalized pitcher name. {} if none."""
    out = {}
    try:
        from scrapers.mlb_pinnacle_scraper import load_strikeout_lines
        for _pname, _d in (load_strikeout_lines(date) or {}).items():
            out[_knorm(_pname)] = _d
    except Exception as _e:
        log.warning(f"Pinnacle K lines load failed (non-fatal): {_e}")
    log.info(f"Pinnacle K lines loaded: {len(out)} pitchers")
    return out


def score_all_props(target_date: str = None) -> list[dict]:
    """
    Load today's hitter stats + lineup data + pitcher stats,
    score all supported props, and return a ranked list.

    Each result dict:
      game, away_team, home_team, prop_type, line, player_name,
      batting_order, proj, confidence, tier, reasoning
    """
    today    = target_date or _today_et()
    raw_path = os.path.join(DATA_DIR, "raw", f"mlb_hitter_stats_{today}.json")

    if not os.path.exists(raw_path):
        log.warning(f"No hitter stats file for {today} — batter props skipped, K props still run")
        games = []
    else:
        with open(raw_path, encoding="utf-8") as f:
            data = json.load(f)
        games = data.get("hitters", [])

    # ── Load pitcher stats master — keyed by name ─────────────────────────────
    # We use this for both K props AND to build pitcher opponent stats
    # (HR/9, H/9) since the schedule master stores names, not IDs.
    pitcher_stats: dict[str, dict] = {}
    ps_path = os.path.join(DATA_DIR, "clean", "mlb_pitcher_stats_master.csv")
    if os.path.exists(ps_path):
        with open(ps_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pname = row.get("player_name", "").strip()
                season = row.get("season", "")
                if pname:
                    # Keep most recent season
                    if pname not in pitcher_stats or season > pitcher_stats[pname].get("season", ""):
                        pitcher_stats[pname] = row

    def pitcher_opp_from_name(sp_name: str) -> dict:
        """
        Build pitcher opponent stats dict from the pitcher stats master.
        Returns HR/9, H/9, K/9, BB/9, opp_avg, era, whip by name lookup.
        """
        if not sp_name or sp_name == "TBD":
            return {}
        row = pitcher_stats.get(sp_name, {})
        if not row:
            return {}
        def sf(v, d=0.0):
            try: return float(v) if v else d
            except: return d
        ip  = sf(row.get("ip", row.get("innings_pitched", 0)))
        hr  = sf(row.get("hr", row.get("home_runs", 0)))
        h   = sf(row.get("h",  row.get("hits", 0)))
        k   = sf(row.get("so", row.get("strikeouts", 0)))
        bb  = sf(row.get("bb", row.get("walks", 0)))
        era = sf(row.get("era", 4.20))
        whip= sf(row.get("whip", 1.30))
        k9  = sf(row.get("k9", row.get("k_per_9", 0)))
        # Compute per-9 rates from raw counts when available
        hr9 = round((hr / ip) * 9, 3) if ip > 5 else 1.20
        h9  = round((h  / ip) * 9, 3) if ip > 5 else 8.50
        bb9 = round((bb / ip) * 9, 3) if ip > 5 else 3.20
        if k9 == 0 and ip > 5:
            k9 = round((k / ip) * 9, 3)
        return {
            "era":     era,
            "whip":    whip,
            "hr_per_9": hr9,
            "h_per_9":  h9,
            "k_per_9":  k9,
            "bb_per_9": bb9,
        }

    # ── Team K rate — fraction of plate appearances ending in a strikeout ─────
    #
    # Two bugs fixed 2026-07-21:
    #   1. This read a column named "strikeout_rate". The actual column is
    #      "k_rate" (values ~0.20-0.26). The wrong name returned 0 and fell
    #      into a fallback whose denominator defaulted to 1, so kr became the
    #      RAW season strikeout total (~1420). That blew past the 1.4 clamp at
    #      the score_k_prop call site, maxing the multiplier for EVERY team and
    #      inflating every K projection ~40% (the "142000.0%" on the dashboard).
    #   2. It kept the FIRST row per team. The master is ordered 2023->2025, so
    #      it always used 2023 rates. Keep the LATEST season instead.
    #
    # A real k_rate sits in (0.10, 0.35); anything outside that is treated as
    # bad data and falls back to the ~0.22 league average.
    team_k_rate: dict[str, float] = {}
    team_k_season: dict[str, int] = {}
    for fname in ("mlb_team_hitting_master.csv", "mlb_team_stats_master.csv"):
        ts_path = os.path.join(DATA_DIR, "clean", fname)
        if os.path.exists(ts_path):
            with open(ts_path, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    tname = row.get("team_name", "").strip()
                    if not tname:
                        continue
                    try:
                        season = int(float(row.get("season", 0) or 0))
                    except (ValueError, TypeError):
                        season = 0
                    # only overwrite with a newer season
                    if tname in team_k_season and season <= team_k_season[tname]:
                        continue
                    try:
                        kr = float(row.get("k_rate", 0) or 0)
                        if not (0.10 < kr < 0.35):
                            # derive from counts if k_rate looks wrong
                            so = float(row.get("strikeouts", 0) or 0)
                            pa = float(row.get("plate_appearances", 0) or 0)
                            if pa <= 0:
                                # approximate PA from the components we do have
                                ab = float(row.get("at_bats", 0) or 0)
                                bb = float(row.get("walks", 0) or 0)
                                pa = ab + bb
                            kr = so / pa if pa > 0 else 0.220
                        if not (0.10 < kr < 0.35):
                            kr = 0.220
                        team_k_rate[tname]   = kr
                        team_k_season[tname] = season
                    except (ValueError, ZeroDivisionError):
                        team_k_rate.setdefault(tname, 0.220)
            break   # use first file found

    # ── Real pitcher K lines from Pinnacle (free, sharp). Keyed by normalized
    #    pitcher name. Absent pitcher => no market line => no K bet (do NOT
    #    invent one; that was the 0.8x-projection bug). ────────────────────────
    pinnacle_k = _load_pinnacle_k(today)
    pinnacle_props = _load_pinnacle_props(today)

    # ── Weather ───────────────────────────────────────────────────────────────
    weather_data: dict[int, dict] = {}
    w_path = os.path.join(DATA_DIR, "clean", "mlb_weather_master.csv")
    if os.path.exists(w_path):
        with open(w_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("game_date") == today:
                    try:
                        gid = int(row.get("game_id", 0))
                        weather_data[gid] = row
                    except (ValueError, TypeError):
                        pass

    # ── Schedule: pitcher names by game_id ────────────────────────────────────
    sched_path = os.path.join(DATA_DIR, "clean", "mlb_schedule_master.csv")
    game_pitchers: dict[int, dict] = {}
    if os.path.exists(sched_path):
        with open(sched_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("game_date") == today:
                    try:
                        gid = int(row.get("game_id", 0))
                        game_pitchers[gid] = {
                            "away_sp":    row.get("away_probable_pitcher", "TBD"),
                            "home_sp":    row.get("home_probable_pitcher", "TBD"),
                            "away_sp_id": row.get("away_probable_pitcher_id", ""),
                            "home_sp_id": row.get("home_probable_pitcher_id", ""),
                            "away_team":  row.get("away_team", ""),
                            "home_team":  row.get("home_team", ""),
                        }
                    except (ValueError, TypeError):
                        pass

    # ── Statcast quality-of-contact data (optional — non-fatal if missing) ──────
    statcast: dict[str, dict] = {}
    try:
        from scrapers.mlb_statcast_scraper import load_statcast
        statcast = load_statcast(min_pa=20)
        if statcast:
            log.info(f"Statcast loaded: {len(statcast)} batters")
    except Exception:
        pass  # Statcast not yet fetched — model runs without it

    def _statcast_adjust(player_name: str, base_conf: float,
                         prop_type: str) -> tuple[float, str]:
        """
        Apply a small Statcast-based confidence adjustment.
        Returns (adjusted_conf, note_string).
        Elite hard contact raises confidence by up to +0.03;
        weak contact lowers it by up to -0.02.
        """
        sc = statcast.get(player_name.lower())
        if not sc:
            return base_conf, ""

        adj  = 0.0
        note = ""

        barrel = sc.get("barrel_batted_rate")
        hh_pct = sc.get("hard_hit_percent")
        xba    = sc.get("xba")

        if prop_type in ("HR", "TB"):
            # Barrel rate matters most for power props
            if barrel is not None:
                if barrel >= 0.15:    # elite — top ~10%
                    adj  += 0.025
                    note  = f"elite barrel rate {barrel:.1%}"
                elif barrel >= 0.10:  # above average
                    adj  += 0.010
                    note  = f"solid barrel rate {barrel:.1%}"
                elif barrel <= 0.04:  # well below avg
                    adj  -= 0.015
                    note  = f"low barrel rate {barrel:.1%}"

        if prop_type in ("HITS", "RBI", "R"):
            # Hard hit % and xBA matter for contact props
            if hh_pct is not None:
                if hh_pct >= 50:      # top tier
                    adj  += 0.015
                    note  = f"{hh_pct:.0f}% hard contact"
                elif hh_pct <= 30:    # below avg contact quality
                    adj  -= 0.010
                    note  = f"soft contact ({hh_pct:.0f}% hard hit)"
            if xba is not None and not note:
                if xba >= 0.290:
                    adj  += 0.010
                    note  = f"xBA .{int(xba*1000):03d} — makes quality contact"
                elif xba <= 0.220:
                    adj  -= 0.010
                    note  = f"xBA .{int(xba*1000):03d} — struggles with quality contact"

        adj_conf = min(0.97, max(0.01, base_conf + adj))
        return adj_conf, note

    all_props = []

    for game in games:
        if not game.get("lineup_confirmed"):
            continue

        game_id   = game.get("game_id", 0)
        away_team = game.get("away_team", "")
        home_team = game.get("home_team", "")
        game_str  = f"{away_team} @ {home_team}"
        weather   = weather_data.get(game_id)

        # Pull pitcher names from schedule master (fallback to lineup game data)
        gp = game_pitchers.get(game_id, {})
        away_sp = gp.get("away_sp", game.get("away_sp", "TBD"))
        home_sp = gp.get("home_sp", game.get("home_sp", "TBD"))

        # Build pitcher opponent stats from pitcher stats master by name
        away_pitcher_opp = pitcher_opp_from_name(away_sp)
        home_pitcher_opp = pitcher_opp_from_name(home_sp)

        def _finalize_prop(prop: dict | None, side: str) -> dict | None:
            """Apply Statcast + trailing hit rate adjustments; return final prop or None."""
            if prop is None:
                return None
            pname = prop.get("player_name", "")
            ptype = prop.get("prop_type", "")
            line  = prop.get("line", 0)
            base  = prop.get("confidence", 0.0)
            adj_conf, sc_note = _statcast_adjust(pname, base, ptype)
            if sc_note:
                prop["reasoning"] = prop.get("reasoning", "") + f" | Statcast: {sc_note}"
                prop["confidence"] = round(adj_conf, 4)
                # Re-tier after adjustment
                if ptype == "HR":
                    prop["tier"] = _hr_tier(adj_conf)
                elif ptype == "TB":
                    if adj_conf >= TB_LOCK_THRESH:        prop["tier"] = "LOCK"
                    elif adj_conf >= TB_STRONG_THRESH:    prop["tier"] = "STRONG"
                    elif adj_conf >= TB_LEAN_THRESH:      prop["tier"] = "LEAN"
                    else: return None
                elif ptype == "SB":
                    if adj_conf >= SB_LOCK_THRESH:        prop["tier"] = "LOCK"
                    elif adj_conf >= SB_STRONG_THRESH:    prop["tier"] = "STRONG"
                    elif adj_conf >= SB_LEAN_THRESH:      prop["tier"] = "LEAN"
                    else: return None
                else:
                    prop["tier"] = _tier(adj_conf)
                if prop["tier"] == "SKIP":
                    return None
            # ── REAL BOOK LINE (2026-08-11) ───────────────────────────────────
            # If Pinnacle lists this player in this market, throw away the
            # model's invented line and re-score against the real one, pricing
            # both directions. Props without a real market keep their model line
            # and stay research projections. Missing means missing.
            _bk = PROP_TYPE_TO_BOOK.get(ptype)
            if _bk:
                _row = (pinnacle_props.get(_bk) or {}).get(_knorm(pname))
                if _row:
                    prop = reprice_prop_with_book(prop, _row)
                    line = prop.get("line", line)

            # Apply trailing hit rate blend (non-fatal — returns base_conf unchanged if no history)
            final_conf, thr = _apply_trailing_hit_rate(pname, ptype, line, prop["confidence"])
            prop["confidence"] = final_conf
            prop["trailing_hit_rate"] = thr
            if thr is not None:
                hits_n = round(thr * 30)  # approximate count out of 30d window
                prop["reasoning"] = prop.get("reasoning", "") + f" | Hit {thr:.0%} last 30d"
            return prop

        # ── Hitter props (away batters face home SP, home batters face away SP)
        for player in game.get("away_lineup", []):
            hr_prop   = _finalize_prop(score_hr_prop(player, home_pitcher_opp, home_team,
                                      is_home=False, weather=weather), "away")
            hits_prop = _finalize_prop(score_hits_prop(player, home_pitcher_opp, is_home=False), "away")
            tb_prop   = _finalize_prop(score_tb_prop(player, home_pitcher_opp, is_home=False), "away")
            rbi_prop  = _finalize_prop(score_rbi_prop(player, home_pitcher_opp, is_home=False), "away")
            r_prop    = _finalize_prop(score_runs_prop(player, home_pitcher_opp, is_home=False), "away")
            sb_prop   = _finalize_prop(score_sb_prop(player, home_pitcher_opp, is_home=False), "away")
            for prop in (hr_prop, hits_prop, tb_prop, rbi_prop, r_prop, sb_prop):
                if prop:
                    all_props.append({
                        "game":      game_str,
                        "game_id":   game_id,
                        "away_team": away_team,
                        "home_team": home_team,
                        "side":      "away",
                        "player_id": player.get("player_id"),
                        **prop,
                    })

        for player in game.get("home_lineup", []):
            hr_prop   = _finalize_prop(score_hr_prop(player, away_pitcher_opp, home_team,
                                      is_home=True, weather=weather), "home")
            hits_prop = _finalize_prop(score_hits_prop(player, away_pitcher_opp, is_home=True), "home")
            tb_prop   = _finalize_prop(score_tb_prop(player, away_pitcher_opp, is_home=True), "home")
            rbi_prop  = _finalize_prop(score_rbi_prop(player, away_pitcher_opp, is_home=True), "home")
            r_prop    = _finalize_prop(score_runs_prop(player, away_pitcher_opp, is_home=True), "home")
            sb_prop   = _finalize_prop(score_sb_prop(player, away_pitcher_opp, is_home=True), "home")
            for prop in (hr_prop, hits_prop, tb_prop, rbi_prop, r_prop, sb_prop):
                if prop:
                    all_props.append({
                        "game":      game_str,
                        "game_id":   game_id,
                        "away_team": away_team,
                        "home_team": home_team,
                        "side":      "home",
                        "player_id": player.get("player_id"),
                        **prop,
                    })

    # ── Pitcher K props ──────────────────────────────────────
    # Generated from schedule for ALL today's games — no hitter stats or lineup
    # confirmation needed. K props need only pitcher stats (always in R2 CSVs).
    for _gid, _gp in game_pitchers.items():
        _away_team = _gp.get("away_team", "")
        _home_team = _gp.get("home_team", "")
        _game_str  = f"{_away_team} @ {_home_team}"
        _weather   = weather_data.get(_gid)
        for _sp_name, _opp_team, _sp_id in (
                (_gp["away_sp"], _home_team, _gp.get("away_sp_id", "")),
                (_gp["home_sp"], _away_team, _gp.get("home_sp_id", ""))):
            if not _sp_name or _sp_name == "TBD":
                continue
            # Skip if already scored this pitcher in the confirmed game loop
            if any(p.get("player_name") == _sp_name and p.get("prop_type") == "K"
                   for p in all_props):
                continue
            _sp_row = pitcher_stats.get(_sp_name, {})
            if not _sp_row:
                continue
            _opp_kr = team_k_rate.get(_opp_team, 0.220)
            # REAL Pinnacle line + prices. No line => no market => no bet.
            _pk = pinnacle_k.get(_knorm(_sp_name))
            if not _pk:
                continue
            _k_prop = score_k_prop(
                pitcher_name=_sp_name,
                pitcher_stats=_sp_row,
                opp_team_k_rate=_opp_kr,
                innings_expected=5.5,
                line=float(_pk.get("line")),
                weather=_weather,
                over_price=_pk.get("over_price"),
                under_price=_pk.get("under_price"),
            )
            if _k_prop:
                all_props.append({
                    "game":      _game_str,
                    "game_id":   _gid,
                    "away_team": _away_team,
                    "home_team": _home_team,
                    "side":      "pitcher",
                    "player_id": _sp_id or _sp_row.get("player_id"),
                    **_k_prop,
                })

    # Sort by confidence descending
    all_props.sort(key=lambda x: x["confidence"], reverse=True)
    by_type = {}
    for p in all_props:
        by_type.setdefault(p["prop_type"], 0)
        by_type[p["prop_type"]] += 1
    type_summary = " | ".join(f"{k}:{v}" for k, v in sorted(by_type.items()))
    log.info(f"Props scored: {len(all_props)} total — {type_summary} | "
             f"{sum(1 for p in all_props if p['tier']=='LOCK')} LOCKs | "
             f"{sum(1 for p in all_props if p['tier']=='STRONG')} STRONGs")
    return all_props


def score_projected_props(projected_lineups: dict, target_date: str = None) -> list[dict]:
    """
    Score props for games where lineups haven't been officially confirmed yet,
    using the most recent confirmed batting orders per team as a projection.

    projected_lineups: dict keyed by full team name ->
        { "players": [full player stat dicts], "date": "YYYY-MM-DD" }

    Each returned prop is identical to score_all_props() output but includes:
        "projected": True

    Called during dashboard build when today's hitter stats file is absent or
    contains unconfirmed games.  Confirmed props always take priority — callers
    should only surface projected props for teams with no confirmed props today.
    """
    today = target_date or _today_et()
    pinnacle_k = _load_pinnacle_k(today)
    pinnacle_props = _load_pinnacle_props(today)

    if not projected_lineups:
        return []

    # ── Load supporting data (mirrors score_all_props setup) ─────────────────
    _load_park_factors()

    pitcher_stats: dict[str, dict] = {}
    ps_path = os.path.join(DATA_DIR, "clean", "mlb_pitcher_stats_master.csv")
    if os.path.exists(ps_path):
        with open(ps_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pname = row.get("player_name", "").strip()
                season = row.get("season", "")
                if pname:
                    if pname not in pitcher_stats or season > pitcher_stats[pname].get("season", ""):
                        pitcher_stats[pname] = row

    def pitcher_opp(sp_name: str) -> dict:
        if not sp_name or sp_name == "TBD":
            return {}
        row = pitcher_stats.get(sp_name, {})
        if not row:
            return {}
        def sf(v, d=0.0):
            try: return float(v) if v else d
            except: return d
        ip   = sf(row.get("ip", row.get("innings_pitched", 0)))
        hr   = sf(row.get("hr", row.get("home_runs", 0)))
        h    = sf(row.get("h",  row.get("hits", 0)))
        k    = sf(row.get("so", row.get("strikeouts", 0)))
        bb   = sf(row.get("bb", row.get("walks", 0)))
        era  = sf(row.get("era", 4.20))
        whip = sf(row.get("whip", 1.30))
        k9   = sf(row.get("k9", row.get("k_per_9", 0)))
        hr9  = round((hr / ip) * 9, 3) if ip > 5 else 1.20
        h9   = round((h  / ip) * 9, 3) if ip > 5 else 8.50
        bb9  = round((bb / ip) * 9, 3) if ip > 5 else 3.20
        if k9 == 0 and ip > 5:
            k9 = round((k / ip) * 9, 3)
        return {"era": era, "whip": whip, "hr_per_9": hr9,
                "h_per_9": h9, "k_per_9": k9, "bb_per_9": bb9}

    # Team K rate — SAME fixed logic as score_all_props (was a duplicate that
    # still read the wrong column "strikeout_rate" with a denominator defaulting
    # to 1, so kr became the raw season K total ~1241 -> the "124100%" on the
    # board and a maxed multiplier inflating every projected K OVER). Read k_rate,
    # validate to a real rate, and keep the LATEST season per team.
    team_k_rate: dict[str, float] = {}
    team_k_season: dict[str, int] = {}
    for fname in ("mlb_team_hitting_master.csv", "mlb_team_stats_master.csv"):
        ts_path = os.path.join(DATA_DIR, "clean", fname)
        if os.path.exists(ts_path):
            with open(ts_path, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    tname = row.get("team_name", "").strip()
                    if not tname:
                        continue
                    try:
                        season = int(float(row.get("season", 0) or 0))
                    except (ValueError, TypeError):
                        season = 0
                    if tname in team_k_season and season <= team_k_season[tname]:
                        continue
                    try:
                        kr = float(row.get("k_rate", 0) or 0)
                        if not (0.10 < kr < 0.35):
                            so = float(row.get("strikeouts", 0) or 0)
                            pa = float(row.get("plate_appearances", 0) or 0)
                            if pa <= 0:
                                ab = float(row.get("at_bats", 0) or 0)
                                bb = float(row.get("walks", 0) or 0)
                                pa = ab + bb
                            kr = so / pa if pa > 0 else 0.220
                        if not (0.10 < kr < 0.35):
                            kr = 0.220
                        team_k_rate[tname]   = kr
                        team_k_season[tname] = season
                    except (ValueError, ZeroDivisionError):
                        team_k_rate.setdefault(tname, 0.220)
            break

    weather_data: dict[int, dict] = {}
    w_path = os.path.join(DATA_DIR, "clean", "mlb_weather_master.csv")
    if os.path.exists(w_path):
        with open(w_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("game_date") == today:
                    try:
                        gid = int(row.get("game_id", 0))
                        weather_data[gid] = row
                    except (ValueError, TypeError):
                        pass

    # Schedule: game_id + pitchers + team names
    sched_path = os.path.join(DATA_DIR, "clean", "mlb_schedule_master.csv")
    today_games: list[dict] = []
    if os.path.exists(sched_path):
        with open(sched_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("game_date") == today:
                    try:
                        gid = int(row.get("game_id", 0))
                    except (ValueError, TypeError):
                        gid = 0
                    today_games.append({
                        "game_id":   gid,
                        "away_team": row.get("away_team", ""),
                        "home_team": row.get("home_team", ""),
                        "away_sp":   row.get("away_probable_pitcher", "TBD"),
                        "home_sp":   row.get("home_probable_pitcher", "TBD"),
                    })

    statcast: dict[str, dict] = {}
    try:
        from scrapers.mlb_statcast_scraper import load_statcast
        statcast = load_statcast(min_pa=20)
    except Exception:
        pass

    def _sc_adjust(player_name: str, base_conf: float, prop_type: str):
        sc = statcast.get(player_name.lower())
        if not sc:
            return base_conf, ""
        adj, note = 0.0, ""
        barrel = sc.get("barrel_batted_rate")
        hh_pct = sc.get("hard_hit_percent")
        xba    = sc.get("xba")
        if prop_type in ("HR", "TB"):
            if barrel is not None:
                if barrel >= 0.15:   adj += 0.025; note = f"elite barrel rate {barrel:.1%}"
                elif barrel >= 0.10: adj += 0.010; note = f"solid barrel rate {barrel:.1%}"
                elif barrel <= 0.04: adj -= 0.015; note = f"low barrel rate {barrel:.1%}"
        if prop_type in ("HITS", "RBI", "R"):
            if hh_pct is not None:
                if hh_pct >= 50:     adj += 0.015; note = f"{hh_pct:.0f}% hard contact"
                elif hh_pct <= 30:   adj -= 0.010; note = f"soft contact ({hh_pct:.0f}% hard hit)"
            if xba is not None and not note:
                if xba >= 0.290:     adj += 0.010; note = f"xBA .{int(xba*1000):03d}"
                elif xba <= 0.220:   adj -= 0.010; note = f"xBA .{int(xba*1000):03d}"
        return min(0.97, max(0.01, base_conf + adj)), note

    def _fin(prop, sc_note=""):
        if prop is None:
            return None
        if sc_note:
            prop["reasoning"] = prop.get("reasoning", "") + f" | Statcast: {sc_note}"
        # REAL BOOK LINE — same treatment as the confirmed-lineup path. This is
        # the path that actually drives the visible board most of the day, since
        # lineups are unconfirmed until a few hours before first pitch, so it
        # must reprice too. Missing this is how the K-rate bug stayed live for
        # weeks: score_projected_props had its own duplicate loader.
        _bk = PROP_TYPE_TO_BOOK.get(prop.get("prop_type", ""))
        if _bk:
            _row = (pinnacle_props.get(_bk) or {}).get(_knorm(prop.get("player_name", "")))
            if _row:
                prop = reprice_prop_with_book(prop, _row)
        prop["projected"] = True
        return prop

    all_props: list[dict] = []

    for g in today_games:
        away_team = g["away_team"]
        home_team = g["home_team"]
        game_id   = g["game_id"]
        game_str  = f"{away_team} @ {home_team}"

        proj_away = projected_lineups.get(away_team)
        proj_home = projected_lineups.get(home_team)
        if not proj_away and not proj_home:
            # No batter lineup data for either team, but K props still run below
            pass  # fall through to K props section

        weather  = weather_data.get(game_id)
        away_opp = pitcher_opp(g["away_sp"])   # away batters face home SP
        home_opp = pitcher_opp(g["home_sp"])   # home batters face away SP

        for side, lineup_data, pitcher_opp_data, is_home in [
            ("away", proj_away, home_opp, False),
            ("home", proj_home, away_opp, True),
        ]:
            if not lineup_data:
                continue
            team = away_team if side == "away" else home_team

            for player in lineup_data.get("players", []):
                pname = player.get("player_name", player.get("name", ""))
                # Score all hitter prop types
                hr_adj,   hr_note   = _sc_adjust(pname, 0, "HR")
                hits_adj, hits_note = _sc_adjust(pname, 0, "HITS")
                tb_adj,   tb_note   = _sc_adjust(pname, 0, "TB")
                rbi_adj,  rbi_note  = _sc_adjust(pname, 0, "RBI")
                r_adj,    r_note    = _sc_adjust(pname, 0, "R")
                sb_adj,   sb_note   = _sc_adjust(pname, 0, "SB")

                for prop, note in [
                    (score_hr_prop(player, pitcher_opp_data, team, is_home=is_home, weather=weather), hr_note),
                    (score_hits_prop(player, pitcher_opp_data, is_home=is_home), hits_note),
                    (score_tb_prop(player, pitcher_opp_data, is_home=is_home), tb_note),
                    (score_rbi_prop(player, pitcher_opp_data, is_home=is_home), rbi_note),
                    (score_runs_prop(player, pitcher_opp_data, is_home=is_home), r_note),
                    (score_sb_prop(player, pitcher_opp_data, is_home=is_home), sb_note),
                ]:
                    if prop:
                        if note:
                            prop["reasoning"] = prop.get("reasoning", "") + f" | Statcast: {note}"
                        prop["projected"] = True
                        all_props.append({
                            "game":      game_str,
                            "game_id":   game_id,
                            "away_team": away_team,
                            "home_team": home_team,
                            "side":      side,
                            "player_id": player.get("player_id"),
                            **prop,
                        })

        # ── Pitcher K props (projected) ──────────────────────────────────────
        for sp_name, opp_team in ((g.get("away_sp",""), home_team), (g.get("home_sp",""), away_team)):
            if not sp_name or sp_name == "TBD":
                continue
            sp_row = pitcher_stats.get(sp_name, {})
            if not sp_row:
                continue
            opp_kr = team_k_rate.get(opp_team, 0.220)
            # REAL Pinnacle line + prices. No line => no market => no bet.
            pk = pinnacle_k.get(_knorm(sp_name))
            if not pk:
                continue
            k_prop = score_k_prop(
                pitcher_name=sp_name,
                pitcher_stats=sp_row,
                opp_team_k_rate=opp_kr,
                innings_expected=5.5,
                line=float(pk.get("line")),
                weather=weather_data.get(game_id),
                over_price=pk.get("over_price"),
                under_price=pk.get("under_price"),
            )
            if k_prop:
                k_prop["projected"] = True
                all_props.append({
                    "game":      game_str,
                    "game_id":   game_id,
                    "away_team": away_team,
                    "home_team": home_team,
                    "side":      "pitcher",
                    "player_id": sp_row.get("player_id"),
                    **k_prop,
                })

    all_props.sort(key=lambda x: x["confidence"], reverse=True)
    by_type = {}
    for p in all_props:
        by_type.setdefault(p["prop_type"], 0)
        by_type[p["prop_type"]] += 1
    type_summary = " | ".join(f"{k}:{v}" for k, v in sorted(by_type.items()))
    log.info(f"Projected props scored: {len(all_props)} — {type_summary}")
    return all_props


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print(score_all_props())
