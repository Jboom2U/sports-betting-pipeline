# GEMINI.md — Statalizers project rules for Gemini CLI

**Read `CLAUDE.md` in this repo root FIRST and treat it as the authoritative
source of truth.** It holds the full architecture, the fix history, the data
boundaries, and every hard rule. This file only restates the rules that must
never be broken, so you cannot miss them even if the context is trimmed.

## What this repo is
Statalizers — an MLB betting model. Python/Flask app on Railway, Postgres + Cloudflare
R2 for data. It scores today's MLB games (moneyline, run line, totals, player props)
and serves a dashboard at statalizers.com. Built and maintained by Justin Skelly.

## Your two jobs here
1. **Coding hands** — make targeted fixes when asked. Edit files in place, verify,
   and hand Justin the exact commands to run. You do NOT deploy.
2. **Analytical red team** — when given model code + the calibration section of
   CLAUDE.md, argue AGAINST the current math. Look for the failure modes a Claude
   model tends to miss: over-smoothed calibration curves, bad vig removal across
   skewed moneyline distributions, data leakage between feature windows and target
   labels in backtests, logit transforms that break near boundary conditions, and
   Brier-score "improvements" that actually cost EV on favorite/longshot extremes.
   Do not agree by default. Your value is being wrong in different places than Claude.

## HARD RULES — do not break these

1. **NEVER run git or railway. Ever.** Do not run `git add`, `git commit`,
   `git push`, or `railway up`. Justin runs ALL git and railway commands himself
   from PowerShell. When an edit is ready, STOP and print the exact commands for
   him to run. Running git from an agent strands `.git/index.lock` on the Windows
   mount and breaks the next commit. This is the most important rule in this file.

2. **Diff-only edits. Never rewrite a whole file.** Apply targeted search/replace
   diff blocks. `run_picks_html.py` is ~221KB; a full-file rewrite will silently
   truncate or drop functions. After every edit, run
   `python -m py_compile <file>` to confirm it still parses.

3. **Data boundary — never pool pre/post-fix calibration data.** Picks before
   2026-07-21 (and the 2026-07-25 totals/RL boundary) came from a partially blind
   model. Any calibration, trend, or backtest analysis must filter to the post-fix
   window. Pooling them produces conclusions that are wrong about both.

4. **Never call the Odds API in development or testing.** It has a 500-request/month
   budget that resets on the 1st. The Pinnacle guest API
   (`guest.api.arcadia.pinnacle.com`) is free — use it for odds work.

5. **Verify SQL column names against `db/schema.py` before writing any query.**
   Key gotchas: `scored_games` uses `score_date` (not `game_date`); join
   picks ↔ scored_games on `sg.score_date = p.pick_date AND sg.game_id = p.game_id`.

6. **Run `python scripts/predeploy_check.py` before proposing a deploy.** It checks
   syntax, null bytes, truncation, SQL columns, and simulates the routes. Only hand
   Justin a commit after it passes.

7. **Additive changes only. Never delete existing data.** Scrapers write to
   `data/raw/` → normalize → `data/clean/` masters. All times are ET.

## Session log — DO THIS AT THE END OF EVERY SESSION

Justin works across several tools because Claude usage runs out mid-week. The
only thing that makes that survivable is a written handover, because chat history
does not persist and re-deriving state is the largest token cost in this project.

**Append an entry to the Work Log at the top of `C:\Users\Jskel\Vault\PROJECT_STATE.md`
before you finish.** Newest first, using the format already in that file:

```
### YYYY-MM-DD · Gemini (Kilo / VS Code)
Changed:    <files>
Did:        <what and why>
Verified:   <what you actually RAN and the result>
Uncertain:  <anything believed but not checked. "none" only if true>
Deployed:   <yes / no / partial>
Next:       <the single most useful next thing>
```

`Uncertain` is required. Every expensive bug in this repo came from one agent
stating a conclusion that the next one trusted without rechecking. If a grep was
truncated or a file was only partly read, that is a partial result, not a
finding, and it belongs in that field.

Do NOT summarise the whole session. Six lines. The point is that the next agent
reads it in seconds.

## Where the current state actually lives

Do not reconstruct it by reading code. Two places answer it cheaply:

1. **statalizers.com/admin** — the checklist. ~44 items, 26 of them probed
   against the real source and data on every page load. It cannot go stale
   because nothing is hand-ticked.
2. **PROJECT_STATE.md** in the Vault — active sprint, locked files, work log.

Read those two before planning anything.

## Workflow
Edit files → `py_compile` / `predeploy_check.py` to verify → tell Justin the exact
PowerShell commands (`git add … / git commit / git push / railway up`) to run. He
reviews `git diff` and deploys. You never touch the deploy path.
