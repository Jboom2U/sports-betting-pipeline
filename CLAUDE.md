# Statalizers — Project Context for Claude

## 📍 Which System Am I In? — Session Scope Map

This folder holds **three related but separate systems**. Confirm which one the session is
about before doing anything. Justin opens chats with a scope prefix; honor it.

| Prefix | System | Lives in | Deploys with |
|---|---|---|---|
| `PIPELINE` | MLB data pipeline + model | `scrapers/`, `normalize/`, `model/`, `run_*.py` | `railway up` |
| `DASHBOARD` | statalizers.com Flask app | `app.py`, `routes/`, `run_picks_html.py` | `railway up` |
| `CONSENSUS` | Multi-AI pick review worker | `consensus-worker/` | `npx wrangler deploy` |
| `PICKS-SITE` | picks.statalizers.com (capper picks, D1) | separate repo | Cloudflare |
| `DAILY` | Routine daily pick runs, no code changes | n/a | n/a |

**CONSENSUS is not the same as DASHBOARD.** The consensus worker is a Cloudflare Worker that
reads pipeline output from R2 and sends it to Gemini/OpenRouter/OpenAI for critical review.
It has its own D1 database (`consensus-db`), its own login, and its own cron (6:45am ET).
It deploys with wrangler, NOT `railway up`, so it costs zero Odds API quota.

**If the scope is ambiguous, ask before editing.** A change to `model/` can silently alter
what the consensus worker reviews the next morning.

### Session history note
Cowork keeps roughly the last 50 sessions. Older ones age out and cannot be recovered.
Anything worth keeping belongs in this file, not in a chat transcript.

---

## ⚠️ CRITICAL: Never Run Git Commands From the Sandbox
**The Cowork sandbox cannot delete lock files on the Windows-mounted repo.** Running `git add` or `git commit` from bash leaves `.git/index.lock` and `.git/HEAD.lock` stranded, breaking the next commit.
- **Claude: make file edits only from the sandbox. Never run git add/commit/push from bash.**
- Tell the user to run all git operations from their own PowerShell terminal.
- Correct pattern: edit files in sandbox → "run `git commit` and `git push` from your terminal"

---

## ⚠️ CRITICAL: Log Timestamps Are UTC — Always Convert to ET
**Railway logs always show UTC time. NEVER read a log timestamp as ET.**
- Subtract 4 hours (EDT) or 5 hours (EST) to get ET
- Example: log shows `22:32` → actual time is `6:32pm ET`

---

## ⚠️ CRITICAL: Pre-Deploy Checklist — Run Before Every railway up

**Always run `python scripts/predeploy_check.py` before giving the user a commit.**

The script does 4 things:
1. Syntax / null bytes / truncation check on all 18 critical files
2. Field-name bug checks (BOM strip, Monte Carlo conf fallback)
3. SQL column validation — cross-checks queries against schema.py column names
4. **Live route simulation against committed code** — does `git stash`, tests /, /performance-html, /schedule-status, /status, then `git stash pop`. This catches NameErrors and route crashes that syntax checks miss.

**Why the stash matters:** Without stashing, the sim tests your local patched files, not what Railway will actually run. Always stash first.

**File truncation risk on Windows-mounted filesystem:** Use Python-via-bash splice pattern for any file over ~200 lines. Always verify with `python3 scripts/predeploy_check.py` after edits.

---

## ⚠️ CRITICAL: SQL Queries — Cross-Check Column Names Against schema.py
Before writing any SQL query, verify the column names exist in `db/schema.py`.
Key table column names:
- `scored_games`: uses `score_date` (NOT `game_date`), `game_id`, `ml_signal`, `sharp_side`
- `picks`: uses `pick_date`, `game_id`, `game`, `pick_type`, `label`, `team`, `conf`, `tier`, `actual_result`, `market_signal`, `away_final`, `home_final`
- Join picks ↔ scored_games on: `sg.score_date = p.pick_date AND sg.game_id = p.game_id`

---

## What This Is
MLB betting dashboard at **statalizers.com**, deployed on **Railway.app**. Built by Justin Skelly (jskellly@gmail.com). Flask app that runs a full data pipeline every morning at 6am ET, scores today's MLB games across moneyline, run line, totals, and player props, and serves an HTML dashboard with picks tiered by confidence (LOCK / STRONG / LEAN / TOSSUP).

---

## Deployment
- **Platform:** Railway.app (service name: Jboom2u Picks)
- **Domain:** statalizers.com (DNS through Cloudflare)
- **Git repo:** `C:\Users\Jskel\GitHub\sports-betting-pipeline` — single source of truth
- **Deploy command:** `railway up` (from PowerShell in repo dir, NOT from sandbox bash)
- **Odds API usage:** Resets 1st of month. ~1 pull/day via adaptive refresh (~30/month). Track carefully.
- **Environment variables:** `ODDS_API_KEY`, `KALSHI_API_KEY`, `DATABASE_URL`, `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY_ID`, `STORAGE_SECRET_ACCESS_KEY`, `STORAGE_BUCKET`, `ADMIN_PASSWORD`, `ADMIN_SECRET`

### PowerShell commit pattern:
```powershell
cd C:\Users\Jskel\GitHub\sports-betting-pipeline
git add <files>
git commit -m "..."
git push
railway up
```

### Data Persistence
- **Railway PostgreSQL** — picks, scored_games, pipeline_runs, player_prop_history
- **Cloudflare R2** — bucket `statalizers-data` — all CSVs + picks HTML
- On startup: downloads CSVs from R2 → checks DB if pipeline ran today → runs if needed
- `railway up` deploys code only — data survives deploys

---

## Repo Structure
```
sports-betting-pipeline/
├── app.py                        # Flask server
├── run_pipeline.py               # Full pipeline entry point
├── run_picks.py                  # Generate picks from scored games
├── run_picks_html.py             # Dashboard HTML generator
├── run_afternoon.py              # Adaptive afternoon refresh
├── run_analysis.py               # Grades yesterday's picks
├── run_historical.py             # Backfill historical data
│
├── routes/
│   ├── admin.py                  # /admin routes (password protected)
│   └── analytics.py             # /analytics NL query interface
│
├── scrapers/                     # One scraper per data source
├── normalize/                    # Raw → clean CSV normalization
├── model/
│   ├── mlb_model.py              # Core Pythagorean scoring model
│   ├── mlb_picks.py              # Pick generation + parlay builder
│   ├── mlb_props_model.py        # Player prop probability engine
│   └── mlb_analysis_sections.py # HR Watch + Monte Carlo tab builders
│
├── db/
│   ├── connection.py             # PostgreSQL connection pool
│   ├── schema.py                 # Table creation + migrations
│   ├── pipeline_log.py           # Mark pipeline started/complete/failed
│   ├── picks_store.py            # Save/grade picks, accuracy summaries
│   ├── model_config.py           # Load/save model weights from DB
│   └── csv_sync.py               # Upload/download CSVs to/from R2
│
├── scripts/
│   └── predeploy_check.py        # Run before every railway up
│
└── data/
    ├── park_factors.csv
    ├── raw/                      # Daily raw scrape outputs
    └── clean/                    # Normalized master CSVs
```

---

## Pipeline Flow (run_pipeline.py)
Runs daily at 6am ET via app.py scheduler. Key steps:
1. mlb_scraper → yesterday's results + today's schedule
2. mlb_normalize → raw → clean CSVs
3. mlb_odds_scraper → live odds + sharp action (Odds API)
4. mlb_weather_scraper, mlb_umpire_scraper, mlb_bullpen_fatigue_scraper
5. mlb_pitcher_scraper + statcast scrapers (BOM strip required on Savant CSVs)
6. mlb_polymarket_scraper (FIXED 2026-07-21: builds per-game slugs mlb-<away>-<home>-<date> from schedule; /markets ignores tag_slug so old catalog-walk found 0 games. The '422 at offset 10100 is expected' note described the BUG, not normal behavior.)
7. mlb_bullpen_scraper, mlb_lineup_scraper, mlb_hitter_scraper
8. mlb_kalshi_scraper (FIXED 2026-07-20: parses the ticker KXMLBGAME-... not the ambiguous title. Verified 17 game markets parsed on 2026-07-21.)
9. run_analysis → grade yesterday's picks → push to DB
10. mlb_model + save_picks → score games → save to DB
11. mark_pipeline_complete + csv_upload_all

**Adaptive afternoon refresh** — fires 2h before first pitch (computed from schedule CSV). Scheduled time stored in `_schedule_state` global dict in app.py and exposed at `/schedule-status` JSON endpoint.

---

## Routes
- `/` — main dashboard (served from cache, 10 min TTL)
- `/admin` — admin hub (password protected) — links to all internal routes
- `/admin/login` — login page; password set via `ADMIN_PASSWORD` env var
- `/admin/logout` — clears session
- `/admin/model-config` — model control panel (adjust signal weights, preview picks, save config to DB)
- `/analytics` — analytics dashboard + natural language DB query interface
- `/status` — pipeline health page
- `/schedule-status` — JSON: next pipeline, next refresh, first pitch times
- `/performance` — JSON W/L/ROI by tier
- `/performance-html` — dark-themed performance dashboard (7d/14d/30d/60d/Season/All time dropdown)
  - Includes: tier breakdown, market signal (CONFIRM/DIVERGE/NEUTRAL), monthly summary, Sharp Action vs Model head-to-head
- `/force-statcast` — manually re-pull Statcast data + rebuild dashboard
- `/force-refresh` — manually trigger afternoon refresh
- `/force-pipeline` — manually trigger full pipeline
- `/unstick` — clear stuck pipeline state

---

## Core Model (model/mlb_model.py)
**Architecture:** Pythagorean win expectation (exponent 1.83)

**Data signals:** pitcher ERA/FIP/WHIP, home/away splits (30/70), platoon splits, recent form (3 starts, 30/70), Statcast stuff (xwOBA 25% + whiff% 10%), team RPG/OPS, bullpen ERA/WHIP, bullpen fatigue (FRESH/NORMAL/TIRED/SPENT → -5%/0%/+12%/+20% bp_era), park factors, weather, HP umpire RPG tendency, Kalshi implied prob, Polymarket implied prob (CONFIRM +1.5% / DIVERGE -1.5%), Statcast barrel/EV/xwOBA (batters), confirmed lineups.

**Global state:** `_schedule_state` dict stores `next_pipeline_et`, `next_refresh_et`, `first_pitch_et` — populated by scheduler and `_schedule_adaptive_refresh()`.

---

## Pick Generation (model/mlb_picks.py)
**Confidence tiers:**
- LOCK: 75%+
- STRONG: 68-75%
- LEAN: 60-68%
- TOSSUP: 48-60% (shown, no Kelly)
- PASS: <48% (not shown)

**RL minimum:** 60% edge required
**TBD suppression:** TOTAL suppressed when either SP TBD; RL suppressed when either SP TBD; ML downgraded one tier when both TBD

**Parlay rules:** min 57% per leg, no two picks from same game
**Thematic parlays:** tagged by thesis (Pitching Mismatch, Home Dog Value, Sharp Action, Bullpen Edge, Market Confirm, Hot Team)
**Pick narrative:** `_build_narrative()` generates 2-3 sentence plain-English explanation per card

---

## Performance Dashboard (/performance-html)
- Rolling W/L/ROI by confidence tier and pick type — dropdown filter: 7d/14d/30d/60d/Season/All time
- Market signal breakdown (CONFIRM / DIVERGE / NEUTRAL vs Kalshi/Polymarket)
- Monthly W/L/ROI summary table
- Sharp Action vs Model — every game where STEAM contradicted model ML pick, result, head-to-head win rate
- Query joins `picks` and `scored_games` on `score_date + game_id`

---

## Dashboard Features (run_picks_html.py)
- Schedule status bar — shows next 6am pipeline, lineup refresh time, first pitch — auto-refreshes every 60s via `/schedule-status`
- HR Watch tab — top HR candidates by barrel rate + park factor + pitcher xwOBA
- Monte Carlo tab — 1000 sim hit-rate bar per ML pick with EV tag
- Daily Summary tab — completed (Final) games only, no in-progress cards
- TOMORROW badge (amber pill) when picks are next-day
- Auto-switches to tomorrow's slate when all today's games started
- gzip compression via flask-compress
- Startup: seeds cache from R2 HTML immediately (site live before pipeline reruns)

---

## Known Issues / Watch Points

### Kalshi matching broken
Fetches 30 KXMLB markets but 0 match today's games. Team/game name format mismatch between Kalshi and schedule CSV. Needs debug session to inspect raw market titles.

### Batter Statcast loads 1 row on restart
`force-statcast` correctly writes 532 batters, but on next startup only 1 loads. Likely a BOM issue in the load path (not just the scraper). Not yet fixed.

### Polymarket 422 at offset 10100
Expected behavior — Polymarket hard caps at 10,000 markets. The 422 is caught and logged as WARNING (non-fatal).

### Odds API quota
500 req/month free tier. Adaptive refresh = ~1 pull/day (~30/month). Every wasted `railway up` that triggers an odds call burns quota.

### predeploy_check.py stash behavior
`git stash --include-untracked` is used so the simulation tests committed code only. If git stash fails (nothing to stash), the sim falls through and tests local files — still catches most issues but not the deployed vs local gap.

---

## Completed Features (chronological)
- Railway PostgreSQL + R2 CSV persistence
- Backtesting loop (nightly grading + /performance-html)
- Umpire HP signal, pitcher Statcast stuff, bullpen fatigue, Polymarket, adaptive refresh
- Probable pitcher upsert in afternoon refresh (catches scratches)
- Tomorrow badge + date toggle + auto-slate switch
- Odds API quota warnings at 150/75/25
- Half-Kelly bet sizing on every pick card
- market_signal column + backfill + market signal breakdown in /performance-html
- Monthly performance summary
- R2 HTML cache seeding on startup
- Hitter stats JSON uploaded to R2 after afternoon scraper
- TBD starter suppression
- Run line minimum 60% edge threshold
- Data consistency fix (ON CONFLICT DO UPDATE, grade preserved)
- Pick card narrative (`_build_narrative()`)
- Thematic parlays (`build_thematic_parlays()`, `_tag_pick_thesis()`)
- Kalshi scraper expanded (paginated search, 7 tickers, events fallback)
- JS IIFE bug fix in run_picks_html.py
- HR Watch tab + Monte Carlo tab (model/mlb_analysis_sections.py)
- /force-statcast route
- BOM strip on Savant pitcher CSV (was silently dropping 656/657 pitchers)
- HITS prop split floor (RBI/Hit internal consistency fix)
- Polymarket 422 silent stop
- predeploy_check.py with stash-based route simulation + SQL column validation
- Schedule status bar on dashboard (/schedule-status endpoint + JS countdown)
- Sharp Action vs Model retrospective in /performance-html
- All INTERVAL '%s days' SQL queries in picks_store.py replaced with Python date cutoff (was silently returning empty)
- push_grades_to_db() game+type fallback (prevents grade push failure when afternoon refresh updates pick label)
- Yesterday panel DB fallback (builds metrics from graded picks when JSON file is missing)
- Daily Summary tab UI redesigned — completed games only, no in-progress cards
- Performance page filter: dropdown replacing pill links, 7d/14d/30d/60d/Season/All time
- Market signal breakdown + monthly summary sections in /performance-html
- player_prop_accuracy min_picks lowered from 5 to 3
- Admin hub at /admin with password login (ADMIN_PASSWORD env var)
- Model control panel at /admin/model-config — tune signal weights, preview pick impact, save config to DB
- Analytics dashboard at /analytics — NL query interface over PostgreSQL
- db/model_config.py — load/save model weights from DB with preset support
- Probable pitcher refresh in afternoon run — fixes stale K prop starters when rotation changes post-6am
- TOSSUP picks now graded in DB (push_grades_to_db uses full graded list) — sharp table shows results for all tiers
- TOSSUP picks included in graded_display — yesterday panel shows all graded games (15+ picks per day)

## ⚠️ DATA BOUNDARY: 2026-07-21 — pre-fix vs post-fix picks

**Every pick generated before 2026-07-21 was produced by a partially blind model.**
Umpire blank, bullpen fatigue never firing, platoon splits dead, Kalshi dead, and
eight master CSVs vanishing from `data/clean/` on every container restart.

The 1,459 graded picks in the calibration findings below are ALL pre-fix. They
measure how the model performed while starved, not how it performs now.

**Do not pool pre- and post-2026-07-21 picks in one calibration run.** Doing so
averages a crippled model with a repaired one and will produce a conclusion that
is wrong about both. When there are enough post-fix graded picks (target ~300, so
roughly 3-4 weeks), run calibration filtered to `pick_date >= '2026-07-21'` and
compare against the pre-fix table below.

Specifically unresolved by the pre-fix data: whether TOTAL picks have edge. Their
pre-fix win rate is flat noise (44.8%-55.4%, no trend across bands), but bullpen
fatigue and umpire RPG both feed run expectancy and neither was firing. Totals may
genuinely improve. Do not permanently suppress totals based on pre-fix data alone.

What the pre-fix data DOES establish, because it held even while the model was
blind: **ML at 75%+ won 67.5% (n=40) and 63.8% (n=47).** That edge existed under
degraded inputs, so it is the safest thing to lean on while post-fix data
accumulates.

---

## 📊 CALIBRATION FINDINGS — 2026-07-20 (read before tuning anything)
**⚠️ ALL PRE-FIX. See the data boundary section above before acting on these.**

First real calibration run, on 1,459 graded picks covering ~May 15 to Jul 19,
after backfilling the picks table from R2. See `/admin/calibration`.

**Overall: 714-745, 48.9%. Break-even at -110 is 52.38%. The model is 3.4 points
underwater over two months.**

### The core problem: confidence does not discriminate

| band | n | predicted | actual |
|---|---|---|---|
| 50-55% | 286 | 52.5% | 44.8% |
| 55-60% | 298 | 57.6% | 45.6% |
| 60-65% | 279 | 62.5% | 49.5% |
| 65-70% | 214 | 67.3% | 50.5% |
| 70-75% | 266 | 72.7% | 49.6% |

Predicted rises 20 points; actual rises ~5 and is not monotonic. All five bands
clear n=200 (CI ≈ ±6) and are statistically indistinguishable from each other.
**Between 50% and 75% confidence the model's number carries almost no
information.** That covers 1,343 of 1,459 graded picks.

Real edge exists only at the top: 75-80% wins 67.5% (n=40), 80%+ wins 63.8%
(n=47). Small samples but clearly separated from everything below.

### Three specific findings

1. **Run line is a systematic loser.** 222 graded. Every band deeply negative:
   37.9% at 50-55 (n=103), 38.2% at 55-60, 36.8% at 65-70. The 60% RL threshold
   in `model/mlb_picks.py` is not protecting anything. Consider suppressing RL.
2. **Totals cap confirmed.** `total_conf_base = min(0.74, ...)` at
   `model/mlb_model.py:972` bunches distinct games onto one number. TOTAL at
   70-75 is n=204, actual 49.0% vs 72.9% predicted (gap -23.9, CI ±6.9).
3. **Tiers are inverted.** Approx win rates: STRONG 53.1%, LOCK 48.8%,
   TOSSUP 47.7%, LEAN 45.5%. LOCK at 65-70 wins 35.8% (n=53). STRONG is the only
   tier above break-even; LOCK is the worst-performing high tier.

### Tier/conf mismatch (unresolved)

`tier` and `conf` are assigned from different numbers. CLAUDE.md documents
LOCK as 75%+, but LOCK contains picks at 65-70% conf; STRONG (doc: 68-75) holds
picks at 60-65. Probably pre- vs post-adjustment confidence. Any tier-based
analysis is untrustworthy until this is traced.

### Open data-integrity question (resolve before acting on the above)

`/admin/calibration` market signal table splits by source: NEUTRAL = 737
live-saved picks at 51.8%; NONE = 722 backfilled picks at 46.0%. Same date
range, same model, 5.8 points apart — larger than chance comfortably explains.
Either the live grading path (`push_grades_to_db`) and the JSON grading path
disagree about outcomes, or the deploy-day sample is skewed. **Resolve this
first; it affects confidence in every number above.**

### What NOT to do

Do not tune signal weights in `/admin/model-config`. Weight tuning assumes the
model ranks correctly and needs rebalancing. The data says the confidence output
is near-uninformative across its main range — a structural problem in how
signals combine into a probability, not a weighting problem.

---

## 🎯 SPEC: "High Confidence" tab (requested 2026-07-21, not yet built)

Justin wants a tab surfacing only picks in the band with demonstrated edge, while
keeping every other pick visible for optional riskier plays. Nothing is hidden or
suppressed — this is prioritization, not filtering.

**Inclusion rule (pre-fix data, revisit after the 2026-07-21 boundary):**
`pick_type == "ML" AND conf >= 0.75`. That band won 67.5% (n=40) and 63.8% (n=47)
even while the model was starved, which is why it is the safe default. Do NOT
extend it to TOTAL or RL on pre-fix evidence; both were flat-to-negative, but they
were also measured blind. Re-derive the threshold from post-fix calibration once
~300 post-2026-07-21 graded picks exist.

**Implementation notes:**
- Nav buttons live at `run_picks_html.py:1656` (`section-nav-btn`, `data-panel`).
  Panels are `<div class="section-panel" id="panel-...">`.
- Blocker: pick cards are built as one large inline template literal inside
  `renderPicks()` (starts ~line 2055, grid `#picksGrid`). There is no reusable
  card function. **Step 1 is extracting that markup into `buildPickCard(p, idx)`**
  so both `#picksGrid` and the new `#highConfGrid` can call it. Do this as its own
  commit and verify the existing Game Picks tab still renders before adding the tab.
- run_picks_html.py is 221 KB — Edit truncates it. Use the Python-via-bash splice
  pattern and `python3 -m py_compile` after every change.
- Show the band's historical record on the panel (e.g. "ML 75%+: 87 picks, 65.5%
  since May 15") so the tab carries its own justification.

---

## 🔴 POLYMARKET ROOT CAUSE FOUND 2026-07-21 (fix not yet written)

`fetch_mlb_markets()` in `scrapers/mlb_polymarket_scraper.py:115` queries
`GET /markets?tag_slug=mlb`. **The `/markets` endpoint silently ignores
`tag_slug`.** Verified live 2026-07-21: that exact call returns
"New Rihanna Album before GTA VI?", "Will Jesus Christ return before GTA VI?",
"Trump out as President before GTA VI?" — zero baseball.

`GET /events?tag_slug=mlb` DOES filter correctly (returns "MLB World Series
Champion 2026", "New MLB CBA by Dec. 1?").

Consequence: the scraper paginates Polymarket's entire ~10k market catalog every
run, matches nothing, and returns early at the `if not games:` guard on line 514
WITHOUT writing `data/clean/mlb_polymarket_master.csv`. `load_polymarket_for_date()`
then finds no file and returns `{}` with no log line — a fully silent failure.
This is why `/admin/signal-audit` shows poly_away_prob / poly_home_prob /
poly_market_gap MISSING and poly_market_signal NO_DATA.

**The documented "422 at offset 10100 is expected — stop silently" note was
describing this bug as normal behavior.** It is not normal; it is the scraper
walking the whole catalog.

**Fix:** rewrite `fetch_mlb_markets()` to hit `/events?tag_slug=mlb&closed=false`
and extract per-game markets from each event's nested `markets` array. Per-game
event slugs look like `mlb-<away>-<home>-<date>`. Then re-verify
`extract_game_probabilities()` against the real shape before trusting it.

**Knock-on:** Kalshi's `combined_away_prob`/`combined_home_prob` and the entire
CONFIRM/DIVERGE market signal need BOTH Kalshi and Polymarket probabilities
(`get_market_divergence(poly_away_prob, kalshi_away_prob)` at
`mlb_polymarket_scraper.py:476`). So Polymarket must be fixed before the Kalshi
ticker fix from 2026-07-20 can produce any market signal at all.

---

## 🌅 BUILD FIRST TOMORROW — 2026 team hitting data (confirm before games)

**Must be done and verified before tomorrow's slate.** The team K-rate fix
(2026-07-21) now reads `k_rate` correctly and uses the latest season per team,
but `mlb_team_hitting_master.csv` only holds 2023-2025 — there is NO 2026 row —
so K props currently score 2026 games on 2025 opponent K-rates. Better than the
2023 rates it used before the fix, but still a season stale.

Fix: the team hitting scraper must pull 2026. Find it (likely
`mlb_team_scraper.py` or `mlb_hitter_scraper.py`'s team path), confirm its season
list / `SEASON` includes 2026, run it, and verify `mlb_team_hitting_master.csv`
gains 30 rows of 2026 data. Then `/admin/refresh-signals`-equivalent for team
stats, and confirm K prop opponent factors vary by team on the board.

Verify working before first pitch so all prop grading starts on clean data.

### Also queued (props, lower urgency — NOT garbage, just unrefined)
- **batting_order defaults to 5 for every hitter.** The hitter scraper does not
  write `batting_order`, so RBI/runs props (`mlb_props_model.py:229,437,505`)
  treat everyone as a #5 bat and the lineup-slot adjustment never differentiates.
  Wire order from the confirmed lineup JSON. Touches the lineup-build path, so do
  it deliberately, not before games. Props still compute correctly meanwhile.
- HR/hits/TB/RBI/runs/SB props otherwise verified sound 2026-07-21: matching
  column names, sane fallbacks, current-season rates (SEASON=datetime.now().year).

---

## 🎲 HR WATCH + MONTE CARLO (flagged 2026-07-21, diagnosed not fixed)

**Monte Carlo "Market Implied" column is a REAL bug — dead since built.**
`build_monte_carlo()` in `model/mlb_analysis_sections.py:175` reads
`p.get("ml_away_odds")` / `p.get("ml_home_odds")` off the PICK dict. But
`generate_picks()` (`model/mlb_picks.py:92-100`) never writes odds onto the pick
dict — it writes type/label/team/conf/tier/game. Odds live on the scored-game
dict, carried as `p["game_data"]`. So pick_odds is always None -> market implied
"N/A" -> EV tag "No Market" on every row. The hit-rate bars work (they just
simulate model_p), but the model-vs-market EV comparison — the whole point of the
tab — has never functioned.
Fix: read `p.get("game_data",{}).get("ml_away_odds")`, or copy ml_away_odds/
ml_home_odds/away_team onto the pick dict in generate_picks. Verify prep_picks
doesn't strip game_data before the analysis sections run.

**HR Watch is probably NOT broken — it's gated on confirmed lineups.**
`build_hr_watch()` filters `all_props` for prop_type=="HR" AND not projected AND
confidence>=0.08. HR props are only generated for confirmed lineups
(`score_all_props` skips unconfirmed games). "No HR candidates — lineups not yet
confirmed" is the correct message pre-lineup. TEST: re-check after lineups lock
near game time. If candidates appear -> working as designed. If STILL empty after
confirmation -> real bug, check whether HR props clear the 0.08 threshold at all
(and note the K-rate-style team-data path is separate; HR uses hr_per_pa which
the hitter scraper writes correctly).

---

## Active Work Queue
1. **Resolve the 51.8% vs 46.0% split** between live-saved and backfilled picks
   (see Calibration Findings). Gates all model work.
2. **Trace the tier/conf mismatch** — tier labels do not match their documented
   confidence ranges.
3. **Run line** — decide whether to suppress RL entirely (39-44% over 222 picks).
4. **Totals cap** — replace `min(0.74, ...)` at `mlb_model.py:972` with something
   that keeps discriminating past ~1.7 runs of edge.
5. **K prop inflation (props, lower priority)** — `mlb_props_model.py:775` reads
   `strikeout_rate`; the CSV column is `k_rate`. Falls through to a fallback whose
   denominator defaults to 1, so `kr` = raw season strikeout total (~1420). That
   blows past the 1.4 clamp at line 641, so EVERY team maxes the multiplier and
   every K projection is inflated ~40%. Also: `mlb_team_hitting_master.csv` only
   holds 2023-2025, and line 772 keeps the first row per team, so even fixed it
   would score 2026 games on 2023 rates.
6. **Batter Statcast 1-row load bug** — 532-row CSV produces 1 row on model load.
7. **Game picks independent of lineups** — ML/RL/total at 6am, props wait for
   lineup confirmation.
8. **Projected props** — publish early for everyday regulars (PROJECTED).

## Housekeeping / latent issues
- `market_signal` is used by `save_picks` but is **not in `db/schema.py`** — no
  column definition, no migration. It exists in production only. A DB rebuilt via
  `create_all()` would make every pick write fail silently.
- `routes/routes/analytics.py` is a stray duplicate of `routes/analytics.py`.
- `kalshi_private.pem` was untracked on 2026-07-20 but remains in git history —
  **rotate the Kalshi key.**
- Kalshi markets span multiple dates; `extract_game_probabilities` does not filter
  by date, so a future game can collide with today's. Ticker carries the date.
- CLAUDE.md route list and env vars drifted ~27 commits behind before 2026-07-20;
  `/force-refresh` documented below does not exist (actual route is `/refresh`).

## ✅ SIGNAL AUDIT: 5/79 dead as of 2026-07-21 4:40pm ET (was 25)

The 5 remaining are all CONSTANT-but-not-broken: `away_bp_found`/`home_bp_found`
(data found = healthy) and the rest-days trio `away_rest`/`home_rest`/`rest_ml_adj`
(0 = "played yesterday", the normal state in daily-play baseball, not a failure).
Functionally there are no dead inputs left.

**Verified live on the 4:40pm adaptive refresh (first clean run after all fixes):**
- Polymarket: **15/15** game markets fetched, zero abbreviation misses. The 6am
  run (pre-deploy) logged `parsed 0`; the 4:40 run logged `parsed 15` — proves the
  slug-builder rewrite is what fixed it.
- Kalshi: `Fetched 70 markets` → `Parsed 17 unique game markets`.
- **`poly_market_signal` = DIVERGE/NEUTRAL for the first time ever.** CONFIRM/DIVERGE
  needs BOTH feeds; it was NO_DATA since inception. Now producing real output.
- Umpire (11 distinct), bullpen fatigue, lineups, platoon, combined_away/home_prob
  all OK.
- total_signal fix (run-unit thresholds) deployed; will show DRIFT/STEAM on the
  next refresh where a total moved 0.5+ runs.

Log-viewer note: Railway's filtered log window doesn't reliably tail to "now".
`filter=Polymarket` topped out at Jul 20; `filter=game markets` surfaced today's
20:40 UTC lines. Use a content filter that matches the exact log string.

---

## 🔬 SIGNAL AUDIT — /admin/signal-audit (built 2026-07-20)

Scores today's slate and reports, per model input, whether it actually VARIES
across games. CONSTANT means every game got the same value, so the signal cannot
differentiate picks no matter its weight. This is the tool that found the dead
subsystems below — use it before ever touching model weights.

**Tonight's progression: 25 → 31 → 20 → 18 of 79 dead.**
(The rise to 31 was deploys wiping `data/raw/` before the R2 sync fix landed.)

### Now live for the first time
- **Bullpen fatigue** — tiers now TIRED/SPENT/NORMAL, `bp_pitches_1d` 24-203.
  Documented as a -5% to +20% swing on bullpen ERA; it had never once fired.
- **Umpire** — 15 distinct names, `ump_factor` -0.212 to 0.008, `ump_rpg` 8.47-9.02.

### Still dead (18)
- **Platoon splits (4) — UNRESOLVED, highest-value remaining.** The master CSV is
  now populated (374 kB in R2, 3,492 rows) and the "vs Left" vs "vs. Left" label
  mismatch is fixed via `_canon_split()`, yet all four fields still return None.
  Both sides run through `normalize_player()` (`" ".join(...).title()`) so names
  *should* match, and `get_pitcher()` on the same name works. Next step: dump
  `self.pitcher_platoon` keys next to `away_sp_name` for one game and compare.
- **Polymarket (4)** — all probs None, signal NO_DATA. Separate from Kalshi.
- **Kalshi combined (2)** — needs both Kalshi and Polymarket; blocked on the above.
- **Lineups (3)** — recoverable any time via `/force-lineups` (free, no Odds API).
- **Rest days (2)** — `away_rest`/`home_rest` always 0, forcing `rest_ml_adj` to 0.
- **total_signal / total_adj** — `total_adj` is a flat -0.01 on every game, so
  totals get zero differentiation from adjustments. Combined with the 0.74 cap at
  `mlb_model.py:972`, this is why TOTAL at 70-75 wins 49% on n=204.
- `bp_found`, `lineup_confirmed` — CONSTANT-but-healthy, ignore.

---

## Fixed 2026-07-20
- **DB connection pool leak** (root cause of ~2 months of silent data loss):
  `run_picks_html.py` called `get_conn()` then `conn.close()`, never returning the
  slot to the pool. maxconn=5 drained ~50 min after each deploy, so DB writes only
  succeeded on deploy days. Now uses the `db_conn()` context manager. maxconn 5→10.
  Missing-DATABASE_URL log lifted DEBUG→WARNING (Railway logs at INFO, so it was
  invisible).
- **`/admin/grade-backfill` rewritten** — was a hardcoded 5-date list; now
  enumerates `picks/mlb_analysis_*.json` from R2 and inserts+grades, committing
  per file. Idempotent on (pick_date, game, pick_type, label).
- **`/admin/calibration` added** — predicted vs actual by conf band, pick type,
  and tier, with 95% CIs and thin-sample flags.
- **Kalshi parsing fixed** — titles use ambiguous city names ("Chicago WS",
  "Los Angeles D"); TEAM_ALIASES is keyed on nicknames the titles never contain;
  and the old regex captured "Toronto Winner". Now parses the ticker
  (`KXMLBGAME-26JUL221335PITNYY-NYY`) via `KALSHI_ABBR`, and inverts yes_prob when
  the contract covers the home side. Note: market signal stays NEUTRAL for all
  historical picks — CONFIRM/DIVERGE needs both Kalshi and Polymarket probs, and
  Kalshi has been returning 0 matches, so that dimension was dead the whole time.
- **UTC vs ET date resolution (major).** Railway runs Python in UTC, so a bare
  `datetime.now()` rolls to tomorrow at 8pm ET. From 8pm ET to midnight the model
  looked for *tomorrow's* dated files and silently lost umpires, lineups and
  bullpen fatigue every single evening. Caught live: an audit at 7:5x showed
  `lineup_confirmed=True`, at 8:0x it showed False. Fixed with `_today_et()` in
  `mlb_model.py`, `run_picks_html.py`, `mlb_props_model.py` (6+2+2 call sites).
  `run_pipeline.py` still has naive calls but runs at 6am ET, same UTC date.
- **raw/ JSONs never synced to R2.** `SYNC_PATTERNS` omitted `mlb_umpires_*`,
  `mlb_bullpen_fatigue_*` and `mlb_lineups_*`, so Railway's ephemeral FS destroyed
  them on every restart. Each deploy made the model progressively blinder.
- **CSV sync race.** Railway's healthcheck hits `/` the instant Flask binds,
  triggering a dashboard regen while `download_all()` was still writing. The model
  loaded a half-empty `data/clean/` and scored on defaults, logging only
  "File not found". Now gated on a `_csv_ready` Event.
- **`json` imported inside a conditional block** in `MLBModel.load()` — the umpire
  block used it but only the lineup block imported it, so fixing the ET date
  surfaced an UnboundLocalError. Hoisted to module scope.
- **`/admin/refresh-signals` added** — regenerates umpire, bullpen-fatigue and
  pitcher/platoon data and uploads to R2. Zero Odds API usage (verified: no
  reference to ODDS_API_KEY in any of the three scrapers).
- **Gamelog transaction abort** — an empty `game_date` raised a date syntax error
  that poisoned the transaction, so every subsequent row failed with "current
  transaction is aborted" and logged hundreds of duplicate lines. Now skips empty
  dates, wraps each row in a SAVEPOINT, and caps the log noise.

## Key Coding Conventions
- All scrapers have a `run()` function as entry point
- Data flows: raw scrape → `data/raw/` → normalize → `data/clean/` master CSVs
- Model loads all clean CSVs once via `MLBModel.load()`, then scores on demand
- Non-fatal steps (odds, weather, Kalshi, Polymarket) use try/except + log.warning
- All times in ET (America/New_York) via zoneinfo
- Flask never blocks on pipeline — everything runs in background threads
- Large file edits: use Python-via-bash splice pattern (Edit tool truncates files >~200 lines on Windows FS)
- SQL queries: always verify column names against db/schema.py before writing
- Never call the Odds API during development or testing
- Never delete existing data — additive changes only
