# Statalizers — Project Context for Claude


> **History lives in `CLAUDE-ARCHIVE.md`** (same folder). Every dated "Fixed on /
> Added on / Changed on" entry was moved there verbatim on 2026-08-19 so this file
> stays loadable in one pass by any model, including the cheaper ones used for
> handoffs. Nothing was deleted or reworded. When debugging a specific subsystem,
> read its section in the archive on purpose rather than carrying all of it into
> every session.

## 2026-08-18 (LATEST): FACTOR WATERFALL ON EVERY CARD

**`exp_runs` now records its own decomposition.** New `record=` parameter; the
list is collected during scoring and exported as `away_factors` / `home_factors`
on the scored game, so the card shows the numbers the model ACTUALLY used rather
than a re-derivation that can drift.

`exp_runs` is a MULTIPLICATIVE chain, so each row is the change in projected runs
that step caused (value_after minus value_before). Deltas are order dependent,
which is inherent to decomposing a product, but they sum EXACTLY to the model's
own projection. That equality is asserted in testing and is the property that
makes it a waterfall rather than a decoration.

Rows: League baseline (4.50) -> Team offense -> Recent form -> Confirmed lineup
-> Opposing starter -> Opposing bullpen -> Pitcher Statcast -> Park -> Home field
-> Weather. **A grey zero bar means that signal did nothing for this game**, which
is the entire point: platoon splits sat dead for weeks while being "in the model"
and nothing on the board would have shown it.

Surfaced as a third collapsible, "Why this number", NOT a card flip. The cards
already carry two `<details>` dropdowns and a click-to-flip container fights them,
since a click inside a dropdown would flip the card.

**The Analysis dropdown was blind, and that is why it repeated.** Justin: "the
analysis seems to be the same on every card." `buildAnalysis` read exactly seven
fields, all of them price and confidence (`conf`, `market_prob`, `pick_price`,
`type`, `value_*`). Nothing about the game reached it. Since most cards sit in the
52-62% band at -110 to -160, the same branch fired repeatedly, correctly: from its
inputs those picks WERE identical.

Fixed by wrapping it. The original is now `buildAnalysisCore`; `buildAnalysis`
prepends `topDrivers(p)`, which names the two largest factor contributions for the
side being backed, pulled from the same breakdown the waterfall draws. Verified
that two picks with identical conf and identical price produce different text.

---

## 2026-08-18 (LATER): SNAPSHOTS NOW RECORD WHO WROTE THEM

Justin: "I often really never know when lines have been pulled and which ones."

**He was not missing a display, he was missing a COLUMN.** `SNAPSHOT_FIELDNAMES`
had 30 columns and none of them was `source`. Both scrapers always knew their own
source and returned it in their result dict (`{"source": "pinnacle"}`), but it was
never written to a row, so which feed produced a given price was unanswerable from
stored data.

- `source` is APPENDED to `SNAPSHOT_FIELDNAMES`, never inserted, so
  `write_snapshot_rows` treats it as additive and MIGRATES every existing row
  rather than dropping them. Verified end to end with an old header file.
- **It is stamped in `write_snapshot_rows`, not in the scrapers.** Both callers
  already pass `source`, so doing it centrally means a third feed cannot ship
  having forgotten it.

**`pull_log.py` derives the day's pulls from the snapshots that actually landed,
never from a run log.** A log records that a pull was ATTEMPTED. This reports what
was WRITTEN, so a pull that fired and produced nothing shows as zero games instead
of a green tick. That distinction is why Polymarket looked healthy for weeks while
matching zero games.

Two things to know when reading it:

1. **`snapshot_time` is UTC.** Both scrapers write
   `datetime.now(timezone.utc)`, so a 6am ET pull is stored as 10:00. Everything
   converts to ET before display. Same rule as the Railway logs.
2. **Source is inferred for pre 08-18 rows and LABELLED as inferred.** The
   inference runs per SNAPSHOT, not per row. Doing it per row split one Pinnacle
   pull into a phantom second "1 game Odds API" pull because a single row happened
   to carry a total. One pull stamps one timestamp, so the timestamp is the pull.

**Odds API quota is now persisted from the API's own `x-requests-used` and
`x-requests-remaining` headers.** They were being read, logged to a line nobody
goes looking for, and discarded. The counters are authoritative; counting pulls
locally drifts the moment a request fails after being counted or a per event props
pull costs more than one credit. Stored at `data/clean/mlb_oddsapi_quota.json` and
named explicitly in `SYNC_PATTERNS`, because CLEAN_DIR previously synced `*.csv`
only and Railway's filesystem is ephemeral.

Surfaces: full panel with quota at the top of `/admin`, and a compact "Lines
pulled" entry in the dashboard schedule bar fed by `/schedule-status`, which the
page already polls every 60 seconds. The bar previously showed only what was
coming NEXT and nothing about what had already landed.

---

## 2026-08-18: LONG TERM INTENT, THIS MAY BECOME A PAID SERVICE

Justin's stated goal: once the model is effective he wants to sell access. Not
being built now, but it changes what is cheap to decide today versus expensive to
retrofit. Tracked as its own group on the /admin checklist.

**Read the data licences before building more on top of them.** This is the one
that can invalidate work already done. Free and personal use tiers commonly forbid
commercial redistribution of odds. That applies to the Odds API and to anything
scraped from a book, and it bears directly on the Selenium scraper plan discussed
the same day: scraping for personal use and reselling the output are materially
different questions.

**A published record has to be immutable.** Picks are re-scored all day, so what
someone saw at noon is not necessarily what gets graded. `tier_locked` and
`was_best_bet` latch, which is the right instinct, but nothing writes a
timestamped pre game snapshot that is never edited. A track record that can move
after the fact is not one worth selling.

**Model version stamping stops being optional.** A record spanning several
undeclared model versions is fine as a note to self and is a claim once money is
involved.

**Access is currently one shared site password.** Per user accounts touch the
schema, every auth check and the session model.

---

## 2026-08-18: THE CHECKLIST IS ON /admin, AND IT VERIFIES ITSELF

`/admin` was rebuilt. Read this before adding a route or a checklist item.

**The open work list now lives at the top of /admin** and its status comes from
reading the repo on every page load, not from a tick box. `model/checklist.py`
holds ~39 items; 24 carry a PROBE that greps the actual source or counts rows in
the actual CSV. The remaining 15 are judgement calls and render as NO CHECK.

**A stored status can never green a probed item.** `db/checklist_state` exists
only for the un-probeable ones and renders as "asserted" with a date. The
asymmetry is the point. A hand set flag is how this file described tier
thresholds of 75/68/60/48 for weeks while the code used 68/62/52/48, and that
phantom was chased more than once.

**Justin can add items from the page.** They go to `db/checklist_notes` and
survive the session, which chat does not (Cowork keeps ~50 sessions, older ones
age out unrecoverably). A new item shows as "waiting on a review". If the DB is
down the POST reports `added=0` and says the item was NOT saved, rather than
silently dropping it.

**Two things the probes caught immediately, both of which this file had wrong:**

1. **Platoon splits: `normalize_platoon_splits()` DOES exist** in
   `normalize/mlb_pitcher_normalize.py:65`. But `run_pipeline.py:170` imports only
   `normalize_recent_starts` from that module, so **the platoon master is never
   refreshed by the 6am run**. It fills only on a manual `/admin/refresh-signals`.
   The local copy is header only, 0 data rows. This was chased three times as a
   name matching bug. It is a pipeline wiring gap. Fix that before touching
   `get_platoon()`, because wiring the model to a file nothing refreshes fixes
   nothing.
2. **The first version of the injuries probe graded itself done.** It counted the
   word "injuries" across `model/*.py` and matched 8 occurrences inside
   `checklist.py` itself. `_py_files()` now excludes this module. **A checklist
   that can see itself will always report itself complete.**

**Adding a route to /admin:** append a tuple to `ROUTES` in `admin_hub.py`. Do
not write card markup by hand. The old page was ~130 lines of inline HTML in
app.py and five cards had been appended using `<b>` and `<span>` instead of
`<div class="card-title">` and `<div class="card-desc">`. Both are inline
elements, so they rendered with no line break: "Prop match diagnosticWhy a real
prop line is not reaching the board." Every card now goes through one `_card()`
function, so that class of mistake is structurally impossible.

**Quota spending routes are now their own section** with the cost on the card.
Previously `/admin/props-pull` (spends quota) sat under Diagnostics and
`/admin/force-oddsapi` (3 credits) sat under Actions, both looking identical to
the free ones, on a 500 request monthly cap.

**Adding a checklist item:** append to `ITEMS` in `model/checklist.py` and write
a probe if one is at all possible. A probe must never raise; `run_probe()`
catches everything and degrades to `unknown`, because a checklist that takes the
admin page down is worse than no checklist. Evidence strings should be facts
("7 scrapers call requests.get with no timeout"), not summaries ("not done").

---

## 2026-08-17: LINE SHOPPING PER BOOK, INCLUDING HARD ROCK

**The Odds API is now queried by BOOKMAKER NAME, not by region.** From the docs:
"Bookmakers can be from any region. Every group of 10 bookmakers is the
equivalent of 1 region." So `SHOP_BOOKS` (<=10, asserted at import) costs exactly
what `regions=us` cost before. **Do not exceed 10 — the 11th doubles the quota.**

This is how Hard Rock got added. It was never missing from the API; it sits in
region `us2`, and the scraper only ever asked for `us`.

Two entries in the old `CONSENSUS_BOOKS` were dead keys matching nothing:
`caesars` (the real key is `williamhill_us`, paid tier only) and `pointsbet`
(US PointsBet delisted; only `pointsbetau` survives). The "8 book consensus" was
really 5.

**Per-book prices are now kept, not averaged away.** New `books_json` column in
`odds_schema.py` holds a compact dict per book. The consensus is still computed
for the model, but the consensus is not bettable anywhere — it is an average of
numbers different books offered. Every card now shows the BEST price, which book
has it, and how far Hard Rock is off it, in a collapsed `<details>` block
(`bookShopHtml`). Only the Odds API pull populates this; Pinnacle leaves it empty.

This matters because the shopping spread is frequently the whole edge: RL 60-70%
needs -154 to clear the 8% cushion, and the same run line was -152 at DraftKings
and -165 at Hard Rock on a test slate. One is a bet, the other is not.

---

## 2026-08-17 (LATER): REAL PRICES OVERTURNED THE MORNING'S CONCLUSION

`/admin/real-roi` on 167 graded picks carrying a stored price. **Read this before
trusting any -110 backtest, including `/admin/strategy-backtest`.**

| type | W-L | REAL ROI |
|---|---|---|
| ML | 38-44 | **-13.5%** |
| RL +1.5 | 20-9 | **+14.9%** |
| RL -1.5 | 8-9 | +3.7% |
| TOTAL | (bands) | **+5.6% and +19.8%** |

**Everything believed about these three types was backwards, and the reason is
always the price, never the pick.** ML picks average -131 to -163, so a 54% band
against a 56.8% break-even loses. Totals sit near -100, so a 62% band prints.
The -110 assumption flattered ML and buried totals.

Three conclusions reversed in one day:
- **ML 68%+ threshold, shipped this morning, is WRONG.** It came from a
  walk-forward test at flat -110 (+6.1%). At real prices ML is negative in five
  of six bands. A confidence threshold cannot see a price problem.
- **Totals are NOT structurally unbettable.** The a priori exclusion was costing
  money. TOTAL 55-60% returned +19.8%.
- **RL +1.5 is the one genuinely profitable type**, +14.9%, which the 08-11
  review had roughly right for the wrong reason.

**Best Bets is now PRICE-FIRST and type-agnostic** (`bestBetEval` +
`bandFor` in run_picks_html.py). Same rule for ML, RL and TOTAL: take that
band's OBSERVED win rate as the probability, require positive EV against the
REAL price with an 8% cushion, admit nothing and exclude nothing in advance.
Bands with fewer than `MIN_BAND_N` (30) graded picks are refused outright,
because a rate off n=15 is noise wearing a probability's clothes.

Backtested against the real-price table, this rule agrees with the observed
outcome in **7 of 10 bands**. All three misses are bands whose real-price sample
is 6, 15 or 21 picks. Two misses are conservative (skipped a winner). The one
aggressive miss is **ML 70-75%: the rule says bet, real ROI was -17.8% on n=6.**
Watch that band specifically on `/admin/real-roi` as the sample grows.

**Standing rule: a strategy validated at an assumed price is not validated.**
Re-check every threshold on `/admin/real-roi` before believing it.

---

## 2026-08-17: WHY BEST BETS WAS ALWAYS EMPTY, AND THE PRICE PROBLEM UNDER IT

**Best Bets scored moneylines off the Platt-calibrated probability. That was the
wrong instrument.** The curve is monotonic by construction; the real ML record is
not (55-60% stated wins 35.0%, 70-75% wins 65.6%, 75-80% wins 48.4%, 80%+ wins
73.7%). A smooth rising line cannot fit a zig-zag, so it splits the difference and
is wrong everywhere. Its practical effect, stated on `/admin/calibration-fit`: the
first stated confidence whose calibrated value clears break-even is **69%**, and
the model rarely produces an ML above 65%. So the shortlist was empty by
construction, essentially every day.

FIX: score off the **walk-forward validated threshold** instead. ML >= 68% went
30-24 (55.6%) on the held-out half. We use 55.6%, NOT the 60.2% in-sample rate,
because in-sample is the number the threshold was chosen on. RL bands refit on
n=296 (was n=94): the usable window is **60-70%**, not 57-65%. 55-60% is a coin
flip (51.6%) and was wrongly included; 65-70% is strong (64.8%) and was wrongly
excluded. Constants: `ML_MIN_CONF`, `ML_OOS_RATE`, `ML_BAND_RATES`,
`RL_BAND_RATES` in run_picks_html.py.

**THE BIGGER PROBLEM, and the one to fix next.** The RL edge was measured at a
flat -110. That assumption is false. The side the model picks is dog +1.5, which
is the EXPENSIVE side of the spread: Rockies +1.5 was **-182** on 08-16, where
break-even is 64.5%. The band wins 65.4%. **The whole +28.8% walk-forward ROI
collapses to +1.3% at the real price.** The model also frequently picks the
FAVORITE at +1.5, which Pinnacle's free feed does not quote at all (it carries
only dog +1.5 / favorite -1.5), so those picks have no price and drop out.

So the run line is not a validated edge at the prices actually available. It is a
validated edge at a price we were assuming. **`/admin/real-roi` (new) recomputes
every band from the `odds` column stored since 08-11, and splits RL by handicap
because +1.5 and -1.5 have opposite price profiles.** Read that page, not the
-110 backtest, before trusting any ROI number.

The shop-for numbers that follow from this, at +8% EV (not break-even):
**RL 60-70% only at -154 or better** (band rate 65.4%; break-even is -189).
**ML 68%+ only at -106 or better** (out-of-sample rate 55.6%; break-even is -125).
Both are on every card via the price check box.

**Totals: do not chase.** 08-16 went 8-2 at real prices for +50% ROI, which is
noise. Season is 104-98 (51.5%) and no threshold in the sweep is profitable.

---

## 2026-08-14: PRICES ARE FIXED AT THE SOURCE. Read before touching any odds code.

Three price defects, all the same root error: **arithmetic applied to a
representation that does not support it.** Every one inflated EV, so every one
pushed bad picks UP the Best Bets ranking. A corrupt price does not produce a
random result, it produces an attractive one.

**1. Run line prices were averaged across handicaps.** `parse_game` collected
every book's run line price for a team into one flat list and averaged it,
without recording which LINE each book quoted. Books split on which side lays
-1.5, so the list mixed plus money (a team at -1.5, +148) with minus money (that
same team at +1.5, -168). Averaging across the sign boundary lands near -100
every time. That is why two unrelated games both published **-109** on 08-13.
FIX: group by handicap (`rl_home_by_line`), average only within a group, publish
all four `rl_{side}_{m15,p15}_price` columns. `|price| < 100` rejected at intake.

**2. Moneyline prices were averaged in odds space.** American odds are not
linear and jump discontinuously across ±100. `-300, -200, +120` averaged to -127
(55.9%) when the true consensus is 62.4% = -166. Always errs toward making the
price look better. FIX: `_avg_american()` averages in implied-probability space.
Vig is deliberately left in; de-vigging happens in `model/value.py`.

**3. Totals mixed lines.** Over prices at 8.5 and 9.0 went into one list. FIX:
`totals_by_line`, main line = the one most books posted, priced only from those
books. `total_line_min/max` still carry the shopping range.

**4. TWO SCHEMAS, ONE FILE (the column shift, third occurrence).**
`mlb_odds_scraper` and `mlb_pinnacle_scraper` each declared their own
`SNAPSHOT_FIELDNAMES` and both append to `mlb_odds_master.csv`. The lists had
drifted by 4 columns, and DictWriter writes POSITIONALLY, so Odds API rows landed
under Pinnacle's header with `total_line` reading `rl_home_m15_price`.
FIX: **`scrapers/odds_schema.py` now owns the schema and the writer. Add a column
there and nowhere else.** `write_snapshot_rows()` checks the on-disk header and
rewrites rather than appending blindly; misaligned rows are DROPPED, never
migrated, because their values are positionally wrong and unrecoverable.

**5. `_price_for` in `mlb_picks.py` still read the legacy fields**, so the EV
gate priced a "+1.5" pick with that team's "-1.5" number. Now matches
`value.value_for_pick`: keyed by the actual handicap, **no fallback**.

**The rule these all point at: a missing value is safe, a wrong value is not.**
Missing is visible and drops the pick. Wrong looks like an answer, gets computed
on, and rises to the top of a ranked list precisely because it is corrupt.
`_price_for`, `_rl_price` and `value_for_pick` all return None rather than guess.

**Universal price check on every card** (`priceCheckHtml` / `pcCheck` /
`pcProbFor` in run_picks_html.py). The site cannot know what Hard Rock is
charging, and books differ by 10-20 cents, which is often the whole edge. Type
your price, get BET IT / TOO THIN / PASS plus a quarter-Kelly stake. Probability
is per bet type and never borrowed: RL in 57-65% uses that band's observed
record, RL outside it uses the 55.0% overall rate flagged as weak, ML and TOTAL
use their calibrated value. Grey until a price is typed so it does not read as a
recommendation.

---

## 2026-08-11: FULL MODEL REVIEW. Read this before touching pick logic.

Measured on 661 graded picks since the 2026-07-21 boundary, via the new
`/admin/calibration-fit` and `/admin/strategy-backtest` routes.

**RUN LINE is the only validated edge.** Walk-forward (threshold chosen on the
first half of the period, applied to the second half it never saw): **51-21,
70.8%, p=0.0011**. It is a broad plateau across 55-65% confidence, not one lucky
cut, which is what makes it credible. Usable band **57-65%**. It collapses at 68%
(15-14) because that is the `RL_DOG_COVER_CAP` region. This is the one bet type
already gated on positive EV against a real price, and that is not a coincidence.

**MONEYLINE did NOT hold up.** Walk-forward 48-42, 53.3%, **p=0.47, noise**. And
ML picks skew to favorites priced -130 to -160 where break-even is 56.5-61.5%, so
it loses at real prices. The in-sample sweep looked good (62.9% at a 70% cut) but
that threshold was chosen after seeing the data. Do not tune ML on the sweep.

**TOTALS cannot clear break-even, structurally.** `total_conf_base` is capped at
0.68 (`mlb_model.py`), which calibrates to 48.8% against a 52.38% break-even. No
total the model can currently produce is +EV. The fitted slope is also nearly
flat (A=0.179): 30 points of stated confidence map to under 7 points of real
probability. Record 72-83. **Do not "fix" this by raising the cap**. That just
relabels the same non-information. The run projection needs rebuilding.

**Per-type calibration is live** in `model/mlb_picks.py CAL_COEFFS`:
ML A=0.621123 B=-0.368625, TOTAL A=0.178915 B=-0.183981. **RL is deliberately
absent**: its fit made out-of-sample Brier WORSE (0.2327 → 0.2573) because there
is little miscalibration left to correct (stated 58.9% vs actual 55.0%). Only
adopt a type whose out-of-sample Brier improves. Refit monthly at
`/admin/calibration-fit`; override per type with `CAL_A_ML` / `CAL_B_ML` env vars.

### Signals that are collected and then thrown away
- **Platoon splits are dead.** Computed at `mlb_model.py:1064-1067`, written to
  the output dict at 1359-1362 for DISPLAY ONLY. They never touch `exp_runs`,
  `ml_conf`, or any adjustment. 3,492 scraped rows feeding a text label.
- **Batter Statcast never reaches the game model.** `mlb_statcast_master.csv` is
  loaded by `mlb_props_model.py` and `player_data.py` only. Every ML/RL/TOTAL is
  scored with zero batter quality input beyond team season RPG and OPS.
- **Only 2 of 7 pitcher Statcast fields are used** (xwOBA, whiff%). Velocity,
  barrel%, exit velo and hard-hit% are scraped and ignored.
- **Injuries are never used anywhere.** Zero references in `model/`.
- `recent_form` is raw last-10 runs with no opponent or park adjustment, at 35%
  of the offensive weight. Park factor is also partly double counted: season RPG
  already contains that team's home games and is then multiplied by park again.

### Full write-up
`C:\Users\Jskel\Vault\02 - Statalizers\Model Review 2026-08-11.md` (3 parts) and
`Betting Card 2026-08-11.md`.

---

## 🤝 AI ASSIST STRATEGY (decided 2026-08-01) — adding Gemini as a second brain

Justin is capped on Cowork/Claude usage and wants (a) coding hands when blocked
here and (b) an independent second opinion to keep the model honest. Decision:
use his existing **Gemini Pro** sub instead of buying Cursor ($20 saved).

- **Coding hands = Gemini CLI** on the local Windows repo. Guardrail file
  `GEMINI.md` (repo root) points it at CLAUDE.md + restates the hard rules
  (no git/railway from agent, diff-only edits on the 221KB file, post-fix data
  boundary, no Odds API in dev, verify SQL vs schema.py, run predeploy_check).
  Gemini CLI auto-loads GEMINI.md on startup. Safety backstop: Justin runs ALL
  git/railway himself and reviews `git diff` before committing, so a bad Gemini
  edit never reaches Railway.
- **Red-team second opinion = Gemini 2.5 Pro.** Different model family = errors
  uncorrelated with Claude's. Audits the MODEL/CODE (calibration, probability
  math), a layer above the Daily Consensus worker which reviews the PICKS.
  Gemini named the failure modes to hunt: over-smoothed calibration curves, vig
  removal across skewed ML distributions, feature/target leakage in backtests,
  logit transforms breaking near boundaries, Brier gains that cost EV at extremes.
- **BUILT 2026-08-01: Claude-vs-Gemini DEBATE (not just parallel reads).**
  `GEMINI_API_KEY` is set in Railway. New `gemini_client.py` = thin REST client
  (`call_gemini`, `gemini_available`; model via `GEMINI_MODEL` env, default
  `gemini-2.5-flash`, key sent as `x-goog-api-key` header not URL; degrades to a
  bracketed notice if key/call fails). Two surfaces:
  1. **`/admin/analysis` page:** `analysis_report.build_debate(data_text,
     narrative)` runs a 1-round debate on the SAME data pack — Gemini red-teams
     Claude's nightly read (`_GEMINI_SYSTEM`), then Claude answers back
     (`_DEBATE_SYSTEM` via generic `_call_claude`). Rendered as "🔵 Claude's read"
     → "🟠 Gemini challenges" → "🔵 Claude responds". Only on page view (not
     download/email) to save calls.
  2. **Statalizer Bot (`/ask`):** `ask_model.answer_question` now returns
     `{answer, gemini, used_pack}`. Claude answers first (`_call_claude_bot`),
     then Gemini gives its OWN read + debates the bot (`_GEMINI_BOT_SYSTEM`).
     `/ask/answer` returns both; the page shows a green Claude block + an orange
     Gemini block (hidden if key missing).
  Where they agree → trust; a real split → the day's flag. Verified graceful
  without a key in the sandbox; will call live on Railway where the key is set.
  NOTE: do NOT call the Gemini API from the dev sandbox — no key here by design.
  - **GEMINI 2.5 THINKING GOTCHA (fixed 2026-08-01):** Gemini 2.5 spends the
    `maxOutputTokens` budget on internal "thinking" FIRST, so a small budget (was
    800) returned a truncated answer cut mid-sentence ("...interesting totals and").
    Fix: `gemini_client.call_gemini` now sets `generationConfig.thinkingConfig.
    thinkingBudget = 0` when the model name contains "flash" (flash allows 0; pro
    does not, so it's left thinking + relies on the raised token budget), and the
    bot call was bumped 800→1400 tokens. Full answers now.
- **Best Bets disclaimer (#2, NOT a bug):** behavior is correct (re-ranks off live
  40-min Pinnacle pulls as EV changes). Added `.bb-note` explaining it tracks the
  closing market and locks at first pitch. Did NOT change the logic.
- **Loss analysis tool (#8):** `/admin/loss-analysis?days=N` — reverse-engineers
  graded losses by bet type / tier / confidence band / market signal + sharp
  divergence, worst high-conf beats, plain-English leak read. Post-fix floored at
  2026-07-25 (never pools pre-fix). Linked in /admin Analysis section.

**Still open from the 9-item list:** #1 root fix (pending props-diag output), #3
HRR/batter-prop real lines, #4 player props in thematic parlays. #9 (ERAs this-year)
CONFIRMED already correct: `get_pitcher` uses `sorted(keys)[-1]` = single latest season.

---

## 📋 QUEUED: Gemini pre-deploy proofread (requested 2026-08-11, not built)

Justin wants a second model reviewing changes BEFORE `railway up`, not after.
The hook is `scripts/predeploy_check.py`, which already runs before every deploy.

**Why:** on 2026-08-11 four self-inflicted bugs were caught only because they
happened to be re-read: a `_safe()` helper that did not exist (would have thrown
on schema creation at startup), a calibration fitted on a biased ML_TOP5 sample
and applied far too harshly, an ML-fitted Platt curve applied to run lines where
it is meaningless, and a claimed "latent bug" that was actually the reviewer's
own newly-inserted code. A model from a different family fails differently and is
more likely to catch what one model would repeat.

**Design sketch:**
- New step in `predeploy_check.py`, AFTER the syntax and SQL checks pass.
- Input: `git diff` of staged files, plus the relevant CLAUDE.md sections.
  (Justin runs it from PowerShell, so git is available there. This must NOT be
  run from the Cowork sandbox, see the git rule above.)
- Reuse `gemini_client.call_gemini()`. Needs `GEMINI_API_KEY` locally, not just
  on Railway. Degrade to a warning and PASS if the key is absent, so the check
  never blocks a deploy on a missing key.
- Prompt should target the failure modes this repo actually has, not generic
  code review: undefined helpers, column names that do not exist in schema.py,
  fields read from the wrong dict, prices looked up by the wrong key, statistics
  fitted on a filtered sample then applied to the whole population, thresholds
  chosen after seeing the data.
- Output ADVISORY only. Print findings, never fail the build on them. A model
  blocking a deploy on a false positive is worse than the bug it prevents.
- Separate concern from the existing `/admin/analysis` and `/ask` debates, which
  review the day's PICKS. This reviews the CODE.

**Also queued:** a one-off Gemini red-team of the 2026-08-11 statistical
conclusions themselves (the RL 57-65% band, the walk-forward split, the per-type
Platt acceptance test, and whether p=0.0011 is adequately discounted for the fact
that the threshold was chosen after seeing the bands).

---

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

## ⚠️ CRITICAL: Never Run ANY Git Command From the Sandbox

**The Cowork sandbox cannot delete lock files on the Windows-mounted repo.** A
stranded `.git/index.lock` breaks the next commit until the user removes it by
hand.

**This includes commands that look read-only.** Tightened 2026-08-11 after
`git status --porcelain` stranded a lock. The previous wording said "git
add/commit", which reads as write-only and is how the mistake got rationalized.

`git status`, `git diff` and `git ls-files` all refresh git's cached index of
file stat data (size, mtime, inode) when it looks stale. To do that git creates
`.git/index.lock`, writes a new index, renames it into place, then unlinks the
lock. The sandbox can create that lock but has no permission to unlink it, so
the cleanup fails and an empty lock file is left behind. It usually gets away
with it; it fails exactly when many files are dirty, which is the end of a big
session when it hurts most.

- **Claude: make file edits only from the sandbox. Run NO git command from bash,
  not even status/diff/log/ls-files.**
- For verification use the file tools, plus `ls`, `wc -l`, `python3 -m py_compile`
  and reading the file directly. That covers everything git status was being
  used for.
- Tell the user to run all git operations from their own PowerShell terminal.
- Correct pattern: edit files in sandbox → "run `git commit` and `git push` from
  your terminal"
- **Recovery if it happens anyway:** `Remove-Item .git\index.lock -Force` from
  PowerShell. Nothing is lost; the lock is empty and only blocks the next write.

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
**Confidence tiers** — VERIFIED against the code 2026-08-17, all 13 picks on
that day's board matched. The values below are `LOCK_THRESH` etc. in
model/mlb_picks.py; anything else written elsewhere is stale.
- LOCK: 68%+
- STRONG: 62-68%
- LEAN: 52-62%
- TOSSUP: 48-52% (shown, no Kelly)
- PASS: <48% (not shown)

This file previously documented 75 / 68 / 60 / 48, which matched nothing. That
error was then "confirmed" as a tier/conf mismatch BUG in the calibration notes
and chased more than once. There was never a bug: the code has always been
self-consistent and the document was wrong.

**A TIER IS NOT A BETTING RECOMMENDATION.** It reports how confident the model
is, nothing more. Best Bets reports whether the PRICE is wrong, which is a
different question and frequently disagrees. On 2026-08-17 the 81.6% LOCK
(Phillies at -240) was the WORST bet on the board at -31.4% EV, while a 54.5%
LEAN (Cardinals at -116) was the only qualifying bet at +9.7%. That is not a
contradiction, it is the price doing the work.

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

## ✅ K PROP REBUILT on real Pinnacle lines 2026-07-22 (over-only; UNDER pending)

**SHIPPED:** K props now score against Pinnacle's REAL strikeout line + prices,
not the old fictional 0.8x-projection line. Free (Pinnacle guest API), sharp book.
- `scrapers/mlb_pinnacle_scraper.py`: `fetch_strikeout_lines()` parses the
  `units=="Strikeouts"` specials (pitcher from `special.description`, line+prices
  from `/markets/straight`); `save_strikeout_lines(date)` ->
  `raw/mlb_pinnacle_k_lines_<date>.json`; `load_strikeout_lines(date)`.
  VERIFIED live 2026-07-22 via `/admin/pinnacle-k-test`: 8 pitchers, real lines
  (Ober 4.5, Singer 5.5, Cecconi 3.5...) with both prices.
- `model/mlb_props_model.py`: loads `pinnacle_k` by normalized pitcher name; both
  K call sites use the real line + over/under prices; NO line => NO bet (skip, do
  not invent). `score_k_prop` computes BOTH directions + EV vs the real price.
- Pulled in the afternoon refresh AND the Refresh Lineups button (free). File
  synced to R2 (`mlb_pinnacle_k_lines_*.json` in SYNC_PATTERNS).
- Verify routes: `/admin/pinnacle-test` (feed live?), `/admin/pinnacle-k-test`
  (parser + prices).

### ✅ UNDER direction ENABLED 2026-07-22 (side-aware grading landed)
`ALLOW_UNDER_K = True`. Full side-aware prop grading is in:
- `player_prop_history.pick_side` column (OVER/UNDER, default 'OVER') — added in
  both the CREATE TABLE and an idempotent `ADD COLUMN IF NOT EXISTS` migration in
  `schema.py create_all()`. Existing rows default 'OVER' (all were over-picks).
- `save_prop_pick(pick_side=...)` stores it; caller `run_pipeline.py` passes
  `pp.get("pick_side","OVER")`. Batter props have no pick_side => default OVER.
- `grade_prop_pick` still stores the OUTCOME direction in `result`
  (OVER/UNDER/PUSH) — correct, unchanged. WIN = (result == pick_side).
- All hit-rate queries in `db/picks_store.py` and the yesterday-props panel in
  `run_picks_html.py` now count `result = COALESCE(pick_side,'OVER')` as a hit.
- NAMING: the prop dict's `side` means away/home/pitcher (participant); the bet
  direction is `pick_side` (OVER/UNDER). Do NOT conflate them — score_k_prop
  returns `pick_side`, the wrapper keeps `side`="pitcher".
- Fixed along the way: the yesterday-props panel counted result=='WIN' which
  NEVER matched (result is OVER/UNDER/PUSH) — was always 0-0; now side-aware.

### Coverage note
Only pitchers Pinnacle lists (~8/slate) get a K prop now, vs ~13 fictional ones
before. Correct tradeoff. The PROJECTIONS VIEW (every starter's model number +
real line where it exists + edge both ways, unfiltered, missing flagged) is the
follow-up that surfaces the rest and turns "gems Justin finds manually" into
model output. Ties to the player-stats/RotoBot track.

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
