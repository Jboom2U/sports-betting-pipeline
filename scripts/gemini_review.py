"""
scripts/gemini_review.py — second-model review of staged changes, pre-deploy.

WHY THIS EXISTS
On 2026-08-11/12 four bugs shipped that a second reader would have caught in
seconds. Every one was syntactically valid, so py_compile passed and the route
simulation missed them:

  1. `_safe(cur, ...)` called in db/schema.py — the helper did not exist.
     Would have thrown during schema creation at startup.
  2. `_last_first_pitch()` called from the lineup scheduler while defined INSIDE
     _start_frequent_odds. NameError swallowed by a try/except, silently falling
     back to a fixed cutoff.
  3. `already_pulled()` used in the props-pull route but never imported.
     500 on every request.
  4. An admin auth guard redirecting to ITSELF instead of /admin/login.
     Infinite redirect loop.

Plus two data-shape failures in the same window:
  5. Four columns added to SNAPSHOT_FIELDNAMES while save_snapshot appended
     under the OLD header, shifting every downstream column.
  6. A new file under data/raw/ that was never added to SYNC_PATTERNS, so
     Railway's ephemeral filesystem destroyed it (after Odds API credits had
     been spent fetching it).

A model from a different family fails differently, so it is more likely to spot
what one model would repeat.

DESIGN RULES
- ADVISORY ONLY. This never fails the build. A false positive blocking a deploy
  is worse than the bug it prevents, and a check that cries wolf gets ignored.
- Degrades to a notice when GEMINI_API_KEY is absent. Never blocks on config.
- Runs LOCALLY (PowerShell), where git is available. Never from the Cowork
  sandbox — see the git rule in CLAUDE.md.
- Reviews the DIFF, not the whole repo, so it stays fast and focused.

SETUP
  Add GEMINI_API_KEY to .env in the repo root. It is already set on Railway,
  but predeploy_check.py runs on your machine.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

MAX_DIFF_CHARS = 60000     # keep the request bounded; a huge diff gets truncated


def _load_key() -> str:
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if key:
        return key
    env = BASE_DIR / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _staged_diff() -> str:
    """Diff of what is about to be committed, falling back to unstaged.

    ENCODING (fixed 2026-08-12): text=True decodes with the LOCALE encoding,
    which on Windows is cp1252. This repo's diffs contain accented player names
    and typographic dashes, so cp1252 raised UnicodeDecodeError. Worse, that
    happened inside subprocess's reader THREAD, so the try/except here never saw
    it and the diff silently came back empty ("no changes to review").

    Force UTF-8 with errors="replace" so a stray byte degrades one character
    instead of killing the whole review.
    """
    # EXCLUDE noise. Data files, generated HTML and vendored code churn on every
    # pipeline run and would crowd out the source changes that are actually being
    # deployed. On 2026-08-12 the first real run burned the whole 60k budget on
    # consensus-worker/src/index.js and never reached the Python being shipped.
    exclude = [
        ":(exclude)data/**",
        ":(exclude)picks/**",
        ":(exclude)logs/**",
        ":(exclude)node_modules/**",
        ":(exclude)*.csv",
        ":(exclude)*.html",
        ":(exclude)*.json",
        ":(exclude)*.docx",
        ":(exclude)package-lock.json",
    ]
    for args in (["git", "diff", "--cached", "-U8", "--"] + exclude,
                 ["git", "diff", "-U8", "--"] + exclude):
        try:
            # RAW BYTES, decoded here. Do NOT pass text=True or encoding=: those
            # decode inside subprocess's reader THREAD, where a failure cannot be
            # caught by this try/except and silently yields an empty diff.
            out = subprocess.run(args, cwd=str(BASE_DIR), capture_output=True,
                                 timeout=30)
            if out.returncode == 0 and out.stdout:
                text = out.stdout.decode("utf-8", errors="replace")
                if text.strip():
                    return text
        except Exception:
            continue
    return ""


_PROMPT = """You are reviewing a diff for a Python MLB betting pipeline before it
deploys to production. Be terse and concrete. Report ONLY defects you can point
at a specific line for. If you find nothing, say exactly: NO ISSUES FOUND.

Do NOT comment on style, naming, formatting, type hints, docstrings, test
coverage, or general "consider refactoring" advice. Those waste the reader's
time. Hunt for the failure modes this repository actually produces:

REFERENCE ERRORS (most common, all shipped here recently)
- A function or name that is called but never defined or imported in that scope.
- A helper defined INSIDE another function but called from a different one.
- A Flask auth guard that redirects to its OWN route instead of the login page,
  which creates an infinite redirect loop.
- A dict key read that does not match the key written elsewhere.

DATA SHAPE ERRORS
- A column added to a fieldname list whose writer appends under an existing
  header (this shifts every later column for readers).
- A SQL column that does not exist in db/schema.py.
- A new file written under data/raw/ or data/clean/ that is NOT added to
  SYNC_PATTERNS in db/csv_sync.py. Railway's filesystem is ephemeral, so an
  unsynced file is destroyed on the next restart.
- A value read from the wrong dict (for example the pick dict when the data
  lives on pick['game_data']).
- A price or line looked up by a key that does not correspond to the bet
  actually being placed.

STATISTICAL ERRORS
- A model or coefficient fitted on a FILTERED sample then applied to the whole
  population.
- A threshold chosen after looking at the outcomes, then reported with a
  p-value as if it were chosen in advance.
- A calibration or transform applied to a quantity it was not fitted on.
- A strategy validated at an ASSUMED price (e.g. a flat -110) and then described
  as validated. The prices actually paid are in the `odds` column.

COMPARING THINGS THAT ARE NOT COMPARABLE — the single most common defect here.
It has shipped five separate times: run-line prices averaged across different
handicaps, totals pooled across 8.5 and 9.0, two CSV schemas written to one
file, per-book "best price" compared across different total lines, and average
American odds printed on the ROI page. Hunt it specifically:
- Two prices compared, averaged, or ranked without first checking they describe
  the SAME wager: same handicap, same total line, same side, same market.
- Arithmetic (mean, sum, sort) applied to AMERICAN ODDS. They are non-linear and
  discontinuous across +/-100; averaging -300 with +120 is meaningless. Convert
  to decimal or implied probability first.
- A "best" or "worst" selected across a set whose members are not interchangeable.

WRITTEN BUT NOT PERSISTED
- Railway's filesystem is EPHEMERAL. Anything written under data/ that is not
  uploaded to R2 is destroyed on the next restart. Flag any new file written
  without a corresponding upload, or missing from SYNC_PATTERNS in db/csv_sync.py.
- An expensive/paid API result written to local disk and not synced.

DESTRUCTIVE MIGRATION
- Code that DROPS or overwrites existing rows/records on a schema or format
  change. Ask whether the change is purely additive, in which case the old data
  can and should be carried forward. Data loss must be the last resort, not the
  default.

CLAIMS THAT CONTRADICT THE CODE
- A docstring, log line, UI label, or route name asserting behaviour the function
  does not implement (for example a route named "force X" that only reaches X in
  a fallback branch that never executes).

For each issue give: file, the line or symbol, what breaks, and the one-line
fix. Order by severity. Maximum six issues.

Here is the diff:

"""


def review(quiet: bool = False) -> int:
    """Print findings. Returns the NUMBER OF ISSUES found (0 = clean/skipped).

    The return value never fails a build — predeploy_check uses it only to put a
    loud line in the final summary. Findings printed mid-run were being scrolled
    past and missed, which made the whole check pointless.
    """
    key = _load_key()
    if not key:
        if not quiet:
            print("  [skip] GEMINI_API_KEY not set locally — add it to .env to "
                  "enable the second-model review.")
        return 0

    diff = _staged_diff()
    if not diff.strip():
        if not quiet:
            print("  [skip] no staged or unstaged changes to review.")
        return 0

    truncated = ""
    if len(diff) > MAX_DIFF_CHARS:
        diff = diff[:MAX_DIFF_CHARS]
        truncated = ("\n\n*** DIFF TRUNCATED at %d chars. The review covers only "
                     "the FIRST portion. Files later in the diff were NOT reviewed. "
                     "Stage a smaller change set for full coverage. ***"
                     % MAX_DIFF_CHARS)

    sys.path.insert(0, str(BASE_DIR))
    try:
        from gemini_client import call_gemini
    except Exception as e:
        print(f"  [skip] gemini_client unavailable: {e}")
        return 0

    try:
        os.environ.setdefault("GEMINI_API_KEY", key)
        out = call_gemini(
            "You are a meticulous senior engineer reviewing a production diff.",
            _PROMPT + diff,
            max_tokens=1600,
        )
    except Exception as e:
        print(f"  [skip] Gemini call failed (non-fatal): {e}")
        return 0

    text = (out or "").strip()
    if not text:
        print("  [skip] Gemini returned nothing.")
        return 0

    # Persist every review so findings can be re-read after the terminal scrolls,
    # and pasted somewhere for discussion.
    try:
        log_dir = BASE_DIR / "logs"
        log_dir.mkdir(exist_ok=True)
        stamp = __import__("datetime").datetime.now().strftime("%Y-%m-%d_%H%M%S")
        out_path = log_dir / f"gemini_review_{stamp}.txt"
        out_path.write_text(text, encoding="utf-8")
    except Exception:
        out_path = None
    # call_gemini never raises; it returns a bracketed notice on failure.
    # Treat that as a skip rather than printing it as if it were a finding.
    if text.startswith("[") and text.endswith("]") and len(text) < 400:
        print("  [skip] " + text.strip("[]"))
        return 0

    print()
    print("-" * 60)
    print("  SECOND-MODEL REVIEW (Gemini) — ADVISORY ONLY, never blocks")
    print("-" * 60)
    for line in text.splitlines():
        print("  " + line)
    if truncated:
        print("  " + truncated.strip())
    print("-" * 60)
    clean = "NO ISSUES FOUND" in text.upper()
    if not clean:
        print("  Read the above before deploying. It is advisory, not a gate:")
        print("  a second model is often wrong. Verify each claim yourself.")
    if out_path:
        print(f"  Saved to {out_path}")
    print()
    if clean:
        return 0
    # Rough count: numbered or bulleted findings. Only used for the summary line.
    import re as _re
    n = len(_re.findall(r"^\s*(?:\d+[.)]|[-*])\s+\S", text, _re.M))
    return max(1, n)


def safe_review() -> int:
    """review() wrapped so nothing here can ever break a deploy check.

    Returns the issue count for the summary line; 0 on any failure.
    """
    try:
        return review()
    except Exception as e:
        print(f"  [skip] second-model review errored (non-fatal): "
              f"{type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(safe_review())
