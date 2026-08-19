"""
pull_log.py: what odds data actually landed today, and when.

WHY THIS EXISTS (created 2026-08-18)

Justin: "I often really never know when lines have been pulled and which ones."

He was not missing a display. He was missing a COLUMN. Both scrapers always knew
their own source and returned it in their result dict, but `SNAPSHOT_FIELDNAMES`
had 30 columns and none of them recorded it, so the stored data could not answer
the question at all. `source` was added on 2026-08-18 and is stamped centrally in
`odds_schema.write_snapshot_rows`.

THE RULE THIS FOLLOWS: DERIVE FROM THE ARTIFACT, NEVER FROM A LOG OF INTENTIONS.

A run log records that a pull was attempted. This reads the snapshots that were
actually written, so a pull that fired and returned nothing shows as zero games
rather than as a green tick. That distinction is the entire point: the failure
mode in this repo has always been a step that reported success while writing
nothing (Polymarket walked the whole catalogue and returned early for weeks,
props saved 0 rows and read as "no picks today").

TIMES ARE STORED IN UTC. Both scrapers write
`datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")`, so a 6am ET pull is
recorded as 10:00. Everything here converts to ET before it is shown, because
reading a stored timestamp as local time is a mistake this project has already
made in Railway logs.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET        = ZoneInfo("America/New_York")
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
MASTER    = os.path.join(CLEAN_DIR, "mlb_odds_master.csv")
QUOTA_F   = os.path.join(CLEAN_DIR, "mlb_oddsapi_quota.json")

MONTHLY_CAP = 500


# ------------------------------------------------------------------ time

def _parse_utc(ts: str):
    """Snapshot timestamp to an aware UTC datetime. None when unparseable."""
    if not ts:
        return None
    t = ts.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(ts.strip().rstrip("Z"), fmt)
                break
            except ValueError:
                continue
        else:
            return None
    # Naive values predate the trailing Z but were written by the same
    # datetime.now(timezone.utc) call, so they are UTC too.
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _et(ts: str):
    dt = _parse_utc(ts)
    return dt.astimezone(ET) if dt else None


# ---------------------------------------------------------------- source

def _infer_source(row: dict) -> tuple:
    """(source, inferred) for rows written before the source column existed.

    Only the Odds API pull populates books_json and the dk_* columns, and only
    the Odds API carries a full game total, since Pinnacle's free feed has no
    clean one. Labelled as inferred wherever it is used, because a guess
    presented as a record is how this project got its worst bugs.
    """
    src = (row.get("source") or "").strip()
    if src:
        return src, False
    if (row.get("books_json") or "").strip() or (row.get("dk_ml_away") or "").strip():
        return "Odds API", True
    if (row.get("total_line") or "").strip():
        return "Odds API", True
    return "Pinnacle", True


def _has(row: dict, *cols) -> bool:
    return any((row.get(c) or "").strip() not in ("", "0") for c in cols)


# ----------------------------------------------------------------- pulls

def pulls_for(date_et: str | None = None, master_path: str | None = None) -> list:
    """Every odds pull that landed on the given ET date, oldest first.

    Grouped by (snapshot_time, source): one run stamps every row it writes with
    a single timestamp computed once, so the group IS the pull.
    """
    path = master_path or MASTER
    if not os.path.exists(path):
        return []
    if date_et is None:
        date_et = datetime.now(ET).strftime("%Y-%m-%d")

    # Group by TIMESTAMP ALONE, then decide the source once for the whole group.
    #
    # Inferring per row split a single Pinnacle pull into a phantom second pull
    # of "1 game" from the Odds API, because one row happened to carry a total.
    # A pull stamps every row it writes with one timestamp computed once, so the
    # timestamp is the pull and the source is a property of the pull, not of a
    # row within it.
    groups: dict = {}
    try:
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
            for row in csv.DictReader(fh):
                when = _et(row.get("snapshot_time", ""))
                if not when or when.strftime("%Y-%m-%d") != date_et:
                    continue
                g = groups.setdefault(row.get("snapshot_time", ""), {
                    "when": when, "source": "", "inferred": False, "_rows": [],
                    "games": 0, "ml": 0, "rl": 0, "total": 0, "books": 0,
                })
                g["_rows"].append(row)
                g["games"] += 1
                if _has(row, "ml_away", "ml_home"):
                    g["ml"] += 1
                if _has(row, "rl_home_m15_price", "rl_home_p15_price",
                        "rl_away_m15_price", "rl_away_p15_price"):
                    g["rl"] += 1
                if _has(row, "total_line"):
                    g["total"] += 1
                if _has(row, "books_json"):
                    g["books"] += 1
    except Exception:
        return []

    for g in groups.values():
        stamped = {(r.get("source") or "").strip() for r in g["_rows"]} - {""}
        if stamped:
            g["source"], g["inferred"] = sorted(stamped)[0], False
        else:
            # No stamped source anywhere in the pull: pre 2026-08-18 data. Decide
            # from the whole group, since evidence on ANY row identifies the pull.
            g["source"] = ("Odds API"
                           if any(_infer_source(r)[0] == "Odds API" for r in g["_rows"])
                           else "Pinnacle")
            g["inferred"] = True
        g.pop("_rows", None)

    out = []
    for g in sorted(groups.values(), key=lambda x: x["when"]):
        out.append({
            **g,
            "time_et": g["when"].strftime("%-I:%M %p").lower()
                       if os.name != "nt" else g["when"].strftime("%I:%M %p").lstrip("0").lower(),
            "gaps": _gaps(g),
            "games_label": f"{g['games']} game" + ("" if g["games"] == 1 else "s"),
        })
    return out


def _gaps(g: dict) -> list:
    """What this pull did NOT bring back. Stated plainly rather than implied.

    Pinnacle having no game total is structural, not a failure, and is labelled
    that way so it does not read as a broken pull every single morning.
    """
    out = []
    n = g["games"] or 1
    if g["ml"] < n:
        out.append(f"{n - g['ml']} games with no moneyline")
    if g["rl"] < n:
        out.append(f"{n - g['rl']} with no per handicap run line")
    if g["total"] == 0:
        out.append("no game total"
                   + (" (expected: Pinnacle's free feed has none)"
                      if g["source"].lower().startswith("pinn") else ""))
    elif g["total"] < n:
        out.append(f"{n - g['total']} with no total")
    if g["books"] == 0 and not g["source"].lower().startswith("pinn"):
        out.append("no per book prices")
    return out


# ----------------------------------------------------------------- quota

def record_quota(used, remaining) -> None:
    """Persist the Odds API's own counters. Called after a paid pull.

    These headers are authoritative. Counting pulls locally is an estimate and
    would drift the moment a request failed after being counted, or a per event
    props pull cost more than one credit.
    """
    try:
        os.makedirs(CLEAN_DIR, exist_ok=True)
        with open(QUOTA_F, "w", encoding="utf-8") as fh:
            json.dump({"checked_at": datetime.now(timezone.utc).isoformat(),
                       "used": used, "remaining": remaining}, fh)
    except Exception:
        pass


def quota() -> dict:
    """Last known Odds API quota, straight from the API's own headers."""
    blank = {"known": False, "used": None, "remaining": None,
             "checked_et": None, "pct": 0, "cap": MONTHLY_CAP}
    try:
        with open(QUOTA_F, encoding="utf-8") as fh:
            d = json.load(fh)
    except Exception:
        return blank
    try:
        rem = int(d.get("remaining"))
        used = int(d.get("used"))
    except (TypeError, ValueError):
        return blank
    when = _et(d.get("checked_at", ""))
    return {"known": True, "used": used, "remaining": rem,
            "checked_et": when.strftime("%b %d, %I:%M %p").replace(" 0", " ") if when else None,
            "pct": max(0, min(100, round(100.0 * used / MONTHLY_CAP))),
            "cap": MONTHLY_CAP}


def summary(date_et: str | None = None) -> dict:
    p = pulls_for(date_et)
    return {
        "pulls": p,
        "count": len(p),
        "last": p[-1] if p else None,
        "sources": sorted({x["source"] for x in p}),
        "any_inferred": any(x["inferred"] for x in p),
        "quota": quota(),
    }
