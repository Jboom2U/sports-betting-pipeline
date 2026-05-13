# Statalizers — Project Context for Claude

## ⚠️ CRITICAL: Log Timestamps Are UTC — Always Convert to ET
**Railway logs always show UTC time. NEVER read a log timestamp as ET.**
- Subtract 4 hours (EDT) or 5 hours (EST) to get ET
- Example: log shows `22:32` → actual time is `6:32pm ET`
- This mistake has been made repeatedly and causes incorrect diagnosis
- When reading any log line, always convert before reasoning about what was happening at that time

---

## What This Is
MLB betting dashboard at **statalizers.com**, deployed on **Railway.app**. Built by Justin Skelly (jskellly@gmail.com). Flask app that runs a full data pipeline every morning at 6am ET, scores today's MLB games across moneyline, run line, totals, and player props, and serves an HTML dashboard with picks tiered by confidence (LOCK / STRONG / LEAN).

The goal is to drive pick confidence as high as possible using every available data signal — stats, Statcast, odds, sharp action, Kalshi + Polymarket prediction markets, weather, lineups, park factors, platoon splits, umpire tendencies, pitcher stuff metrics, and bullpen fatigue.

---

## Deployment
- **Platform:** Railway.app (service name: Jboom2u Picks)
- **Domain:** statalizers.com (DNS through Cloudflare — nameservers: doug.ns.cloudflare.com, savanna.ns.cloudflare.com)
- **Deploy command:** `railway up`
- **Environment variables (set via Railway CLI):**
  - `ODDS_API_KEY` — The Odds API (free tier: 500 req/month, resets 1st of month)
  - `KALSHI_API_KEY` — Kalshi prediction market
  - `DATABASE_URL` — Set automatically by Railway PostgreSQL plugin
  - `STORAGE_ENDPOINT_URL` — Cloudflare R2: `https://8b850ffd1a28f3de49e4559366ba5204.r2.cloudflarestorage.com`
  - `STORAGE_ACCESS_KEY_ID` — R2 API token access key
  - `STORAGE_SECRET_ACCESS_KEY` — R2 API token secret
  - `STORAGE_BUCKET` — `statalizers-data`
- **Traffic monitoring:** Cloudflare dashboard → statalizers.com → Analytics & logs → HTTP Traffic

### Data Persistence — IMPLEMENTED
- **Railway PostgreSQL** — stores picks history, scored games, pipeline run log
- **Cloudflare R2** — bucket `statalizers-data` stores all CSV snapshots
- On startup: app downloads CSVs from R2 → checks DB if pipeline ran today → runs pipeline if needed
- After 6am pipeline: uploads all CSVs to R2, saves picks + scored games to PostgreSQL
- `railway up` now deploys code only — data survives deploys

---

## Repo Structure
```
sports-betting-pipeline/
├── app.py                        # Flask server — serves dashboard, schedules pipeline
├── run_pipeline.py               # Main pipeline entry point (all steps)
├── run_picks.py                  # Generate picks from scored games
├── run_picks_html.py             # Dashboard HTML generator
├── run_afternoon.py              # Mid-day refresh (lineups + odds) — also runs on Railway
├── run_analysis.py               # Grades yesterday's picks, pushes results to DB
├── run_historical.py             # Backfill historical data
├── serve_picks.py                # Local serving utility
│
├── scrapers/
│   ├── mlb_scraper.py            # Core MLB stats scraper (Baseball Reference)
│   ├── mlb_pitcher_scraper.py    # Pitcher stats + recent starts (Baseball Savant)
│   ├── mlb_team_scraper.py       # Team hitting/pitching stats
│   ├── mlb_hitter_scraper.py     # Individual hitter stats for lineup players
│   ├── mlb_bullpen_scraper.py    # Bullpen ERA, WHIP, usage stats
│   ├── mlb_statcast_scraper.py   # Statcast quality-of-contact (Baseball Savant)
│   ├── mlb_statcast_pitcher_scraper.py  # Pitcher stuff metrics — xwOBA, whiff%, velocity
│   ├── mlb_lineup_scraper.py     # Confirmed lineups
│   ├── mlb_odds_scraper.py       # Live odds + sharp action (Odds API) — quota warnings built in
│   ├── mlb_kalshi_scraper.py     # Kalshi prediction market probabilities
│   ├── mlb_polymarket_scraper.py # Polymarket prediction market probabilities (no auth)
│   ├── mlb_weather_scraper.py    # Game-time weather (wind, temp, precip)
│   ├── mlb_umpire_scraper.py     # HP ump assignments + career RPG tendency
│   ├── mlb_bullpen_fatigue_scraper.py  # Reliever workload last 3 days per team
│   └── mlb_historical_scraper.py # Historical game results for backtesting
│
├── normalize/
│   ├── mlb_normalize.py          # Normalizes raw scrape → clean master CSVs
│   ├── mlb_pitcher_normalize.py  # Pitcher-specific normalization
│   ├── mlb_bullpen_normalize.py  # Bullpen normalization
│   └── mlb_historical_normalize.py
│
├── model/
│   ├── mlb_model.py              # Core game scoring model
│   ├── mlb_picks.py              # Pick generation + parlay builder
│   └── mlb_props_model.py        # Player prop probability engine
│
├── db/
│   ├── connection.py             # PostgreSQL connection pool
│   ├── schema.py                 # Table creation (pipeline_runs, picks, scored_games)
│   ├── pipeline_log.py           # Mark pipeline started/complete/failed
│   ├── picks_store.py            # Save + grade picks, get accuracy summary
│   └── csv_sync.py               # Upload/download CSVs to/from Cloudflare R2
│
├── data/
│   ├── park_factors.csv          # Static park run/HR factors by venue
│   ├── raw/                      # Daily raw scrape outputs (restored from R2 on deploy)
│   └── clean/                    # Normalized master CSVs (restored from R2 on deploy)
│
└── logs/                         # Pipeline logs
```

---

## Pipeline Flow (run_pipeline.py)
Runs daily at 6am ET via app.py scheduler (Railway — no laptop needed). Steps in order:

1. **mlb_scraper** — scrapes yesterday's results + today's schedule from Baseball Reference
2. **mlb_normalize** — normalizes raw → appends to clean master CSVs
3. **mlb_odds_scraper** — pulls live odds + line movement via Odds API; detects sharp action
4. **mlb_weather_scraper** — fetches game-time weather for today's games
5. **mlb_umpire_scraper** — fetches HP ump assignments from MLB Stats API
6. **mlb_bullpen_fatigue_scraper** — reliever pitch counts last 3 days per team
7. **mlb_pitcher_scraper + normalize** — recent starts for today's probable pitchers
8. **mlb_statcast_scraper** — Statcast batter metrics from Baseball Savant
9. **mlb_statcast_pitcher_scraper** — pitcher stuff metrics (xwOBA, whiff%, velocity)
10. **mlb_polymarket_scraper** — Polymarket implied probabilities
11. **mlb_bullpen_scraper + normalize** — bullpen season stats
12. **mlb_lineup_scraper** — confirmed lineups
13. **mlb_hitter_scraper** — individual hitter stats for lineup players
14. **mlb_kalshi_scraper** — Kalshi implied probabilities
15. **run_analysis** — grades yesterday's picks, pushes grades to PostgreSQL
16. **mlb_model + save_picks** — scores games, saves picks + scored games to DB
17. **mark_pipeline_complete** — records run in DB so next deploy skips pipeline
18. **csv_upload_all** — uploads all CSVs to Cloudflare R2

**Afternoon refresh** (11:30am ET — runs on Railway, no laptop needed):
- Grades yesterday, refreshes odds, umpires, bullpen fatigue, Kalshi, Polymarket, lineups, hitter stats
- Rebuilds dashboard cache

---

## Core Model (model/mlb_model.py)

**Architecture:** Pythagorean win expectation (exponent 1.83)
- Expected runs = team RPG × pitcher suppression × park factor × weather adjustment
- Win probability from Pythagorean formula drives moneyline confidence
- Home field boost: +0.025

**Data signals used:**
- Pitcher season ERA/FIP/WHIP
- Pitcher home/away splits (weighted 30% splits / 70% season)
- Pitcher platoon splits (vs. Left / vs. Right batters)
- Pitcher recent form: last 3 starts blended 30% recent / 70% season
- **Pitcher Statcast stuff** — xwOBA against (25% weight) + whiff% (10% weight) → suppression multiplier capped ±15%
- Team offensive RPG, OPS
- Team pitching ERA
- Bullpen ERA + WHIP
- **Bullpen fatigue** — reliever pitch counts last 3 days; FRESH/NORMAL/TIRED/SPENT tiers adjust bp_era by -5%/0%/+12%/+20% before SP/BP blend
- Park factors (run factor + HR factor by venue)
- Weather: wind MPH toward/away CF (+0.04 runs per mph out), temp (−1.2% per °F below 65), precip probability
- **HP Umpire tendency** — career RPG vs 9.0 league avg; 40% blend weight applied to exp_total
- Live odds + line movement (sharp action detection)
- **Kalshi implied probability** — market signal vs model win probability
- **Polymarket implied probability** — second market signal; CONFIRM (+1.5% conf) / DIVERGE (-1.5% conf) vs Kalshi
- Statcast: barrel rate, exit velocity, hard hit %, xBA, xSLG, xwOBA (batter quality)
- Confirmed lineups (lineup-weighted offensive scoring)

**League-average baselines (2023-2025):**
- ERA: 4.20, FIP: 4.20, RPG: 4.50, OPS: 0.720, xwOBA: 0.315, whiff%: 25.0

---

## Pick Generation (model/mlb_picks.py)

**Confidence tiers:**
- LOCK: 68%+ — strongest model signal
- STRONG: 62-68%
- LEAN: 55-62%
- PASS: <55% — not shown

**Parlay rules:**
- Minimum 57% confidence per leg
- No two picks from the same game
- Ranked by combined probability

**Prop types supported (mlb_props_model.py):**
HR (0.5+), HITS (0.5+), TB (1.5+), RBI (0.5+), R (0.5+), SB (0.5+), K (SP strikeout total over/under)

---

## app.py Behavior
- Serves dashboard from cache; never blocks a request
- Cache TTL: 10 minutes (regenerates in background)
- Full pipeline: runs at 6am ET daily on Railway (background thread)
- Afternoon refresh: runs at 11:30am ET daily on Railway (background thread) — no Windows Task Scheduler needed
- Odds snapshot: every 2 hours between 8am-10pm ET
- Lineup refresh: checks lineup_confirmed status from JSON directly; retries every 30 min while any game unconfirmed
- Dashboard shows TOMORROW badge (amber pill) when picks are for next calendar day
- gzip compression via flask-compress (570KB HTML → ~80KB)

**Routes:**
- `/` — main dashboard
- `/status` — pipeline status (last run, DB connection)
- `/performance` — JSON: rolling W/L/ROI by tier and pick type (supports `?days=30`)
- `/performance-html` — dark-themed performance dashboard with 7/14/30/60/90d toggles

---

## Backtesting / Performance Tracking
- `run_analysis.py` grades yesterday's picks nightly using MLB Stats API for final scores
- `push_grades_to_db()` matches graded picks to DB rows by (pick_type, label) and calls `db/picks_store.grade_pick()`
- `db/picks_store.get_accuracy_summary(days)` computes rolling W/L/ROI from PostgreSQL
- Yesterday panel on dashboard shows prior day results once graded
- Monthly stats display planned once sufficient data accumulates

---

## Data Sources
| Source | What | Auth |
|--------|------|------|
| Baseball Reference | Game results, schedule, team/pitcher stats | None (scrape) |
| Baseball Savant | Statcast metrics — batters + pitchers | None (CSV endpoint) |
| MLB Stats API | Schedule, boxscores, umpire assignments, final scores | None |
| Odds API | Live odds, line movement, sharp action | ODDS_API_KEY (500 req/month free, resets 1st) |
| Kalshi | Prediction market implied probabilities | KALSHI_API_KEY |
| Polymarket | Second prediction market implied probabilities | None (public Gamma API) |
| Open-Meteo | Game-time weather | None |
| Rotowire / MLB.com | Confirmed lineups | None (scrape) |

---

## Cloudflare Setup
- Account: jskellly@gmail.com at dash.cloudflare.com
- DNS: statalizers.com CNAME → Railway URL (proxied, orange cloud)
- www CNAME → jboom2u-picks Railway URL (proxied)
- MX records: eforward1-5.registrar-servers.com (DNS only, for email)
- Traffic analytics: dash.cloudflare.com → statalizers.com → Analytics & logs → HTTP Traffic
- Nameservers at Namecheap: doug.ns.cloudflare.com + savanna.ns.cloudflare.com
- **R2 bucket:** `statalizers-data` (Eastern North America) — 9+ objects, grows with each pipeline run

---

## Roadmap

### Completed
- Railway PostgreSQL persistence (picks, scored games, pipeline log)
- Cloudflare R2 CSV persistence (data survives deploys)
- Backtesting loop — nightly grading + /performance dashboard
- Umpire HP signal — MLB Stats API + career RPG lookup table
- Pitcher Statcast stuff metrics — xwOBA/whiff% suppression multiplier
- Bullpen fatigue signal — 3-day reliever workload tiers
- Polymarket second prediction market signal — divergence vs Kalshi
- Afternoon refresh moved to Railway (11:30am ET background thread)
- Tomorrow badge on dashboard header
- Lineup refresh fix — checks actual lineup_confirmed status, retries every 30 min
- Odds API quota warnings at 150/75/25 remaining

### Next Up — Model Improvements (Priority 3)
- Tune Pythagorean weights using accumulated backtesting data (need ~4 weeks of graded picks first)
- ~~Add Kelly Criterion bet sizing recommendations~~ **DONE** — Half-Kelly at -110 displayed on every pick card (e.g. 55%→2.8%, 65%→13.3%)
- Monthly performance summary widget on main dashboard
- Consider ensemble approach (Random Forest alongside Pythagorean) for validation

---

## Key Coding Conventions
- All scrapers have a `run()` function as entry point, return a result summary
- Data flows: raw scrape → `data/raw/` → normalize → `data/clean/` master CSVs
- Model loads all clean CSVs once via `MLBModel.load()`, then scores on demand
- Non-fatal steps (odds, weather, Kalshi, Polymarket) use try/except + log.warning — pipeline continues if they fail
- All times in ET (America/New_York) via zoneinfo
- Flask never blocks on pipeline — everything runs in background threads
- **File truncation risk on Windows-mounted filesystem:** Edit tool can truncate large files. Fix pattern: use Python via bash — find stub string, write head + tail. Always verify with `tail -10` after edits to large files.

---

## Known Issues / Watch Points
- **Odds API free tier** — 500 req/month. Deploys trigger pipeline runs which burn quota. May 2026 exhausted due to multiple deploys. Resets May 1st. Quota warnings now logged at 150/75/25 re