"""
mlb_weather_scraper.py
Pulls game-time weather forecasts for today's MLB schedule using Open-Meteo
(free, no API key required).

Key output per game:
  - wind_speed_mph       : sustained wind at game time
  - wind_dir_degrees     : meteorological direction (0=from N, 90=from E, etc.)
  - wind_component       : + = blowing OUT to CF (helps offense/over)
                           - = blowing IN from CF (suppresses offense/under)
  - temp_f               : temperature at game time
  - precip_prob          : precipitation probability (0-100)
  - weather_flag         : "WIND_OUT", "WIND_IN", "COLD", "PRECIP", "NORMAL"
  - roof                 : True if park has roof (wind irrelevant)
"""

import requests
import csv
import os
import math
import logging
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

WEATHER_API = "https://api.open-meteo.com/v1/forecast"
HEADERS     = {"User-Agent": "mlb-betting-pipeline/1.0"}

# ── Stadium data: lat, lon, timezone, CF bearing (compass °), has roof ────────
# cf_bearing: compass degrees from home plate toward CF
# Wind component = wind_speed * cos(wind_from - cf_bearing)
#   positive = wind blowing OUT to CF (hitter-friendly)
#   negative = wind blowing IN from CF (pitcher-friendly)
STADIUMS = {
    "Oriole Park at Camden Yards":          {"lat": 39.2840,  "lon": -76.6218,  "tz": "America/New_York",    "cf": 55,  "roof": False},
    "Fenway Park":                          {"lat": 42.3467,  "lon": -71.0972,  "tz": "America/New_York",    "cf": 35,  "roof": False},
    "Yankee Stadium":                       {"lat": 40.8296,  "lon": -73.9262,  "tz": "America/New_York",    "cf": 20,  "roof": False},
    "Citi Field":                           {"lat": 40.7571,  "lon": -73.8458,  "tz": "America/New_York",    "cf": 35,  "roof": False},
    "Citizens Bank Park":                   {"lat": 39.9057,  "lon": -75.1665,  "tz": "America/New_York",    "cf": 30,  "roof": False},
    "Nationals Park":                       {"lat": 38.8730,  "lon": -77.0074,  "tz": "America/New_York",    "cf": 350, "roof": False},
    "Truist Park":                          {"lat": 33.8908,  "lon": -84.4678,  "tz": "America/New_York",    "cf": 20,  "roof": False},
    "loanDepot park":                       {"lat": 25.7781,  "lon": -80.2197,  "tz": "America/New_York",    "cf": 350, "roof": True },
    "Tropicana Field":                      {"lat": 27.7683,  "lon": -82.6534,  "tz": "America/New_York",    "cf": 0,   "roof": True },
    "PNC Park":                             {"lat": 40.4469,  "lon": -80.0057,  "tz": "America/New_York",    "cf": 340, "roof": False},
    "Progressive Field":                    {"lat": 41.4962,  "lon": -81.6852,  "tz": "America/New_York",    "cf": 35,  "roof": False},
    "Comerica Park":                        {"lat": 42.3390,  "lon": -83.0485,  "tz": "America/Detroit",     "cf": 40,  "roof": False},
    "Guaranteed Rate Field":                {"lat": 41.8300,  "lon": -87.6338,  "tz": "America/Chicago",     "cf": 15,  "roof": False},
    "Wrigley Field":                        {"lat": 41.9484,  "lon": -87.6553,  "tz": "America/Chicago",     "cf": 15,  "roof": False},
    "American Family Field":                {"lat": 43.0280,  "lon": -87.9712,  "tz": "America/Chicago",     "cf": 350, "roof": True },
    "Busch Stadium":                        {"lat": 38.6226,  "lon": -90.1928,  "tz": "America/Chicago",     "cf": 10,  "roof": False},
    "Target Field":                         {"lat": 44.9817,  "lon": -93.2781,  "tz": "America/Chicago",     "cf": 355, "roof": False},
    "Kauffman Stadium":                     {"lat": 39.0514,  "lon": -94.4803,  "tz": "America/Chicago",     "cf": 5,   "roof": False},
    "Minute Maid Park":                     {"lat": 29.7572,  "lon": -95.3555,  "tz": "America/Chicago",     "cf": 20,  "roof": True },
    "Globe Life Field":                     {"lat": 32.7473,  "lon": -97.0822,  "tz": "America/Chicago",     "cf": 35,  "roof": True },
    "Coors Field":                          {"lat": 39.7559,  "lon": -104.9942, "tz": "America/Denver",      "cf": 350, "roof": False},
    "Chase Field":                          {"lat": 33.4453,  "lon": -112.0667, "tz": "America/Phoenix",     "cf": 0,   "roof": True },
    "T-Mobile Park":                        {"lat": 47.5914,  "lon": -122.3324, "tz": "America/Los_Angeles", "cf": 340, "roof": True },
    "Oracle Park":                          {"lat": 37.7786,  "lon": -122.3893, "tz": "America/Los_Angeles", "cf": 25,  "roof": False},
    "Oakland Coliseum":                     {"lat": 37.7516,  "lon": -122.2005, "tz": "America/Los_Angeles", "cf": 340, "roof": False},
    "Sutter Health Park":                   {"lat": 38.5802,  "lon": -121.5011, "tz": "America/Los_Angeles", "cf": 340, "roof": False},
    "Dodger Stadium":                       {"lat": 34.0739,  "lon": -118.2400, "tz": "America/Los_Angeles", "cf": 25,  "roof": False},
    "UNIQLO Field at Dodger Stadium":       {"lat": 34.0739,  "lon": -118.2400, "tz": "America/Los_Angeles", "cf": 25,  "roof": False},
    "Angel Stadium":                        {"lat": 33.8003,  "lon": -117.8827, "tz": "America/Los_Angeles", "cf": 35,  "roof": False},
    "Petco Park":                           {"lat": 32.7076,  "lon": -117.1570, "tz": "America/Los_Angeles", "cf": 30,  "roof": False},
    "Great American Ball Park":             {"lat": 39.0975,  "lon": -84.5061,  "tz": "America/New_York",    "cf": 0,   "roof": False},
    "Daikin Park":                          {"lat": 29.7572,  "lon": -95.3555,  "tz": "America/Chicago",     "cf": 20,  "roof": True },
    "Rate Field":                           {"lat": 41.8300,  "lon": -87.6338,  "tz": "America/Chicago",     "cf": 15,  "roof": False},
}

FIELDNAMES = [
    "game_id", "game_date", "game_time_utc", "venue",
    "temp_f", "wind_speed_mph", "wind_dir_degrees",
    "wind_component", "wind_label",
    "precip_prob", "roof", "weather_flag", "timestamp",
]

WIND_OUT_THRESHOLD  =  6.0   # mph blowing out — meaningful offensive boost
WIND_IN_THRESHOLD   = -6.0   # mph blowing in  — meaningful suppression
COLD_THRESHOLD      = 50     # °F — cold significantly affects ball flight
PRECIP_THRESHOLD    = 30     # % — flag potential weather delays


def wind_component(wind_from_deg: float, cf_bearing: float) -> float:
    """
    Calculate effective wind component along the home-plate → CF axis.
    Positive = blowing OUT to CF. Negative = blowing IN from CF.
    Uses meteorological convention: wind_from_deg is where wind originates.
    """
    angle = math.radians(wind_from_deg - cf_bearing)
    return round(-math.cos(angle), 4)   # negative cos: "from" → "to" flip


def get_stadium(venue: str) -> dict:
    if venue in STADIUMS:
        return STADIUMS[venue]
    v = venue.lower()
    for k, s in STADIUMS.items():
        if k.lower() in v or v in k.lower():
            return s
    return None


def fetch_weather_for_game(game: dict) -> dict:
    venue     = game.get("venue", "")
    game_time = game.get("game_time_utc", "")
    game_id   = game.get("game_id", "")
    game_date = game.get("game_date", "")

    stadium = get_stadium(venue)
    if not stadium:
        log.warning(f"No stadium data for: {venue}")
        return _default_row(game_id, game_date, game_time, venue, "UNKNOWN_PARK")

    if stadium["roof"]:
        log.info(f"Roof park — skipping weather: {venue}")
        return _default_row(game_id, game_date, game_time, venue, "ROOF", roof=True)

    lat, lon, tz = stadium["lat"], stadium["lon"], stadium["tz"]
    cf           = stadium["cf"]

    # Parse game hour in UTC
    target_hour = None
    if game_time:
        try:
            dt = datetime.strptime(game_time, "%Y-%m-%dT%H:%M:%SZ")
            target_hour = dt.strftime("%Y-%m-%dT%H:00")
        except ValueError:
            pass

    params = {
        "latitude":              lat,
        "longitude":             lon,
        "hourly":                "temperature_2m,wind_speed_10m,wind_direction_10m,precipitation_probability",
        "wind_speed_unit":       "mph",
        "temperature_unit":      "fahrenheit",
        "timezone":              "UTC",
        "forecast_days":         3,
    }

    try:
        resp = requests.get(WEATHER_API, params=params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Weather fetch failed for {venue}: {e}")
        return _default_row(game_id, game_date, game_time, venue, "FETCH_ERROR")

    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])
    temps  = hourly.get("temperature_2m", [])
    winds  = hourly.get("wind_speed_10m", [])
    dirs   = hourly.get("wind_direction_10m", [])
    precip = hourly.get("precipitation_probability", [])

    # Find matching hour
    idx = None
    if target_hour:
        for i, t in enumerate(times):
            if t.startswith(target_hour[:13]):
                idx = i
                break
    if idx is None and times:
        # Fall back to first available hour on game date
        for i, t in enumerate(times):
            if t.startswith(game_date):
                idx = i
                break

    if idx is None:
        return _default_row(game_id, game_date, game_time, venue, "NO_FORECAST")

    temp        = round(temps[idx], 1)  if idx < len(temps)  else 70.0
    wind_spd    = round(winds[idx], 1)  if idx < len(winds)  else 0.0
    wind_dir    = round(dirs[idx], 0)   if idx < len(dirs)   else 0.0
    precip_pct  = round(precip[idx], 0) if idx < len(precip) else 0.0

    wc          = wind_component(wind_dir, cf) * wind_spd
    wind_lbl    = ("OUT to CF" if wc > 1 else "IN from CF" if wc < -1 else "CROSSWIND")

    # Weather flag
    flag = "NORMAL"
    if precip_pct >= PRECIP_THRESHOLD:   flag = "PRECIP"
    elif temp <= COLD_THRESHOLD:          flag = "COLD"
    elif wc >= WIND_OUT_THRESHOLD:        flag = "WIND_OUT"
    elif wc <= WIND_IN_THRESHOLD:         flag = "WIND_IN"

    return {
        "game_id":         game_id,
        "game_date":       game_date,
        "game_time_utc":   game_time,
        "venue":           venue,
        "temp_f":          temp,
        "wind_speed_mph":  wind_spd,
        "wind_dir_degrees":wind_dir,
        "wind_component":  round(wc, 2),
        "wind_label":      wind_lbl,
        "precip_prob":     precip_pct,
        "roof":            False,
        "weather_flag":    flag,
        "timestamp":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _default_row(game_id, game_date, game_time, venue, flag, roof=False):
    return {
        "game_id": game_id, "game_date": game_date, "game_time_utc": game_time,
        "venue": venue, "temp_f": 70.0, "wind_speed_mph": 0.0,
        "wind_dir_degrees": 0.0, "wind_component": 0.0, "wind_label": "N/A",
        "precip_prob": 0.0, "roof": roof, "weather_flag": flag,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def run(games: list = None, target_date: str = None) -> list:
    """
    Fetch weather for all games on target_date.
    Pass games list directly, or reads from schedule master if not provided.
    """
    log.info("=" * 60)
    log.info("Weather Scraper started")
    log.info("=" * 60)

    if games is None:
        import csv as _csv
        sched_path = os.path.join(BASE_DIR, "data", "clean", "mlb_schedule_master.csv")
        with open(sched_path, encoding="utf-8") as f:
            all_games = list(_csv.DictReader(f))
        date = target_date or datetime.now().strftime("%Y-%m-%d")
        games = [g for g in all_games if g.get("game_date") == date]

    rows = []
    for g in games:
        row = fetch_weather_for_game(g)
        rows.append(row)
        log.info(f"{g.get('away_team','?')} @ {g.get('home_team','?')} | "
                 f"{row['temp_f']}°F | Wind {row['wind_speed_mph']} mph "
                 f"{row['wind_label']} | Flag: {row['weather_flag']}")
        time.sleep(0.3)

    # Write raw
    if rows:
        date_str  = rows[0]["game_date"] if rows else (target_date or datetime.now().strftime("%Y-%m-%d"))
        out_path  = os.path.join(RAW_DIR, f"mlb_weather_{date_str}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"Weather written: {out_path} ({len(rows)} games)")

    log.info("Weather Scraper complete")
    return rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()
