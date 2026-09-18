"""
One-shot splice: add /admin/r2-picks, a read-only R2 inspector that can also
backfill a lost slate.

WHY THIS EXISTS
---------------
On 2026-09-17 and 2026-09-18, save_picks was silently failing (a literal percent
sign in the SQL). Picks for 09-17 exist nowhere:

    Grading source: CSV (0 picks), DB had 0.
    No picks found for 2026-09-17 (CSV or DB)

The one remaining candidate is picks/mlb_picks_2026-09-17.html in R2. The
dashboard HTML carries the full DATA_PICKS array inline, with conf, tier, label,
team, game_id AND odds, which is strictly richer than the CSV would have been.

There is currently no way to ask what is in R2 without deploying code, which is
itself the thing that keeps breaking the site. So this adds one route that
answers the question.

WHAT THE ROUTE DOES
-------------------
    /admin/r2-picks?date=2026-09-17
        READ ONLY. Lists which keys exist for that date, their size and
        last-modified, and if the HTML is present, parses DATA_PICKS out of it
        and shows exactly which rows a backfill WOULD insert. Writes nothing.

    /admin/r2-picks?date=2026-09-17&apply=1
        Inserts those rows with ON CONFLICT DO NOTHING, so an existing row is
        never touched. Reports inserted vs skipped.

DESIGN RULES IT FOLLOWS
-----------------------
  * Read-only by default. `apply=1` is deliberate and explicit.
  * ON CONFLICT DO NOTHING, never DO UPDATE. A backfill must not be able to
    overwrite a live-saved row. This is the rule the first-pitch snapshot
    already relies on.
  * Never invents a price. If DATA_PICKS carries no odds for a pick, odds stays
    NULL. A missing value is safe, a wrong value is not.
  * actual_result is left at 'PENDING' so the normal grader picks these up on
    its next run, rather than this route guessing at outcomes.
  * Everything is wrapped so a failure reports rather than 500s.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\add_r2_picks_recovery.py
    python3 scripts\\predeploy_check.py

DEPLOY IN THE MORNING, after the 6am run and before first pitch. A restart
after first pitch wipes the dashboard cache and the site cannot rebuild,
which is what took statalizers.com down on 09-17 and again on 09-18.
"""

import io
import os
import shutil
import sys
import datetime
import py_compile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
APP = os.path.join(REPO, "app.py")
HUB = os.path.join(REPO, "admin_hub.py")

MARKER = "/admin/r2-picks"

ANCHOR_APP = '@app.route("/admin/export/picks.csv")\n'

ROUTE = '''@app.route("/admin/r2-picks")
def r2_picks_recovery():
    """Inspect, and optionally restore, a slate of picks from R2.

    Built 2026-09-18 after save_picks failed silently for two days and the
    2026-09-17 slate was found to exist in neither the DB nor the local CSV.
    The dashboard HTML in R2 embeds the whole DATA_PICKS array, which is the
    only surviving copy, and it carries odds where the CSV never did.

    READ ONLY unless apply=1. Inserts use ON CONFLICT DO NOTHING so a
    reconstructed row can never overwrite one that was saved live.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/r2-picks")

    import html as _h, json as _json, re as _re, tempfile as _tmp

    date  = (request.args.get("date") or "").strip()
    apply_ = request.args.get("apply") == "1"
    out = ["<pre style='background:#0d1117;color:#c9d1d9;padding:20px;"
           "font:13px/1.5 ui-monospace,monospace'>"]
    def say(s=""):
        out.append(_h.escape(str(s)))

    if not _re.match(r"^\\d{4}-\\d{2}-\\d{2}$", date):
        say("Usage: /admin/r2-picks?date=YYYY-MM-DD        (read only)")
        say("       /admin/r2-picks?date=YYYY-MM-DD&apply=1 (insert missing rows)")
        say("")
        say("Reports what exists in R2 for that date and what a backfill would do.")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")

    say(f"R2 picks recovery for {date}")
    say("=" * 60)
    say("")

    try:
        from db.csv_sync import _get_client, _bucket
        client, bucket = _get_client(), _bucket()
    except Exception as e:
        say(f"storage client unavailable: {e}")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")
    if client is None:
        say("storage client unavailable (STORAGE_* env vars not set)")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")

    keys = [f"picks/mlb_picks_{date}.html",
            f"picks/mlb_picks_{date}.csv",
            f"picks/mlb_props_{date}.csv"]
    present = {}
    say("WHAT EXISTS IN R2")
    for k in keys:
        try:
            meta = client.head_object(Bucket=bucket, Key=k)
            present[k] = meta
            say(f"  FOUND    {k}  {meta.get('ContentLength', 0):,} bytes  "
                f"{meta.get('LastModified')}")
        except Exception:
            say(f"  MISSING  {k}")
    say("")

    html_key = keys[0]
    if html_key not in present:
        say("The dashboard HTML is not in storage, so there is no surviving copy")
        say("of this slate. Nothing can be recovered. Record the date as a hole")
        say("rather than reconstructing it from memory or a screenshot.")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")

    try:
        with _tmp.NamedTemporaryFile(suffix=".html", delete=False) as tf:
            tf_path = tf.name
        client.download_file(bucket, html_key, tf_path)
        with open(tf_path, encoding="utf-8", errors="replace") as f:
            page = f.read()
        os.unlink(tf_path)
    except Exception as e:
        say(f"download failed: {e}")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")

    # Pull DATA_PICKS out of the page by bracket matching, not regex: the array
    # contains braces and quotes inside reasoning strings.
    def _extract(name):
        m = _re.search(r"const\\s+" + name + r"\\s*=\\s*", page)
        if not m:
            return None
        i = m.end()
        if i >= len(page) or page[i] != "[":
            return None
        depth, j, instr, q = 0, i, False, ""
        while j < len(page):
            c = page[j]
            if instr:
                if c == "\\\\":
                    j += 2
                    continue
                if c == q:
                    instr = False
            elif c in "\\"'":
                instr, q = True, c
            elif c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        try:
            return _json.loads(page[i:j + 1])
        except Exception:
            return None

    picks = _extract("DATA_TODAY_PICKS") or _extract("DATA_PICKS")
    if not picks:
        say("DATA_PICKS could not be parsed out of the stored HTML.")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")

    say(f"PARSED {len(picks)} PICKS FROM THE STORED BOARD")
    say("")
    say(f"  {'tier':8} {'type':6} {'label':34} {'conf':>6} {'odds':>7}")
    say("  " + "-" * 66)
    rows, no_price = [], 0
    for p in picks:
        conf = p.get("conf")
        try:
            conf = float(conf)
        except Exception:
            continue
        if conf > 1.5:
            conf = conf / 100.0
        odds = p.get("pick_price", p.get("odds"))
        try:
            odds = float(odds)
            if abs(odds) < 100:
                odds = None
        except Exception:
            odds = None
        if odds is None:
            no_price += 1
        rows.append({
            "game_id": str(p.get("game_id", "")),
            "game":    p.get("game", ""),
            "type":    (p.get("type") or p.get("pick_type") or "").upper(),
            "label":   p.get("label", ""),
            "team":    p.get("team", ""),
            "conf":    round(conf, 4),
            "tier":    p.get("tier", ""),
            "odds":    odds,
        })
        say(f"  {rows[-1]['tier']:8} {rows[-1]['type']:6} {rows[-1]['label'][:34]:34} "
            f"{conf*100:5.1f}% {('' if odds is None else int(odds)):>7}")
    say("")
    say(f"  {len(rows)} rows parsed, {no_price} with no usable price")
    say("")

    if not apply_:
        say("READ ONLY. Nothing was written.")
        say(f"To insert these, add &apply=1 to the URL.")
        say("")
        say("What apply=1 will do:")
        say("  - INSERT ... ON CONFLICT DO NOTHING, so any row already in the")
        say("    table is left exactly as it is")
        say("  - actual_result stays PENDING so the normal grader scores them")
        say("  - odds stays NULL where the board had no usable price. No price")
        say("    is ever invented")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")

    say("APPLYING")
    ins = skipped = 0
    try:
        from db.connection import db_conn as _dbc
        with _dbc() as conn:
            if conn is None:
                say("  database unavailable, nothing written")
                return Response("\\n".join(out) + "</pre>", mimetype="text/html")
            cur = conn.cursor()
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO picks
                        (pick_date, game_id, game, pick_type, label, team,
                         conf, tier, reasoning, odds, actual_result)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    ON CONFLICT (pick_date, game_id, pick_type) DO NOTHING
                    """,
                    (date, r["game_id"], r["game"], r["type"], r["label"],
                     r["team"], r["conf"], r["tier"],
                     "restored from stored board " + date, r["odds"]),
                )
                if cur.rowcount:
                    ins += 1
                else:
                    skipped += 1
    except Exception as e:
        say(f"  write failed: {e}")
        return Response("\\n".join(out) + "</pre>", mimetype="text/html")

    say(f"  inserted {ins}, skipped {skipped} (already present)")
    say("")
    say("Now run the grader for this date so results attach:")
    say(f"  /admin/regrade?date={date}")
    return Response("\\n".join(out) + "</pre>", mimetype="text/html")


'''


ANCHOR_HUB = '''        ("/admin/export/picks.csv", "Export graded picks",
'''

HUB_CARD = '''        ("/admin/r2-picks", "R2 pick recovery",
         "What survives in storage for a date, and restore a lost slate. Read only unless apply=1",
         "admin", "free"),
'''


def main():
    for p in (APP, HUB):
        if not os.path.exists(p):
            sys.exit("ABORT: %s not found. Run this from the repo." % p)

    if MARKER in io.open(APP, encoding="utf-8").read():
        print("Already applied. Nothing to do.")
        return 0

    resolved = {}
    for path, old, new, label in (
        (APP, ANCHOR_APP, ROUTE + ANCHOR_APP, "route"),
        (HUB, ANCHOR_HUB, HUB_CARD + ANCHOR_HUB, "admin card"),
    ):
        src = io.open(path, encoding="utf-8").read()
        before = len(src)
        n = src.count(old)
        if n != 1:
            sys.exit("ABORT: %s anchor matched %d times in %s, expected 1.\n"
                     "NOTHING written." % (label, n, os.path.basename(path)))
        resolved[path] = (src.replace(old, new, 1), before)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backups = {}
    for path in resolved:
        b = path + ".%s.bak" % stamp
        shutil.copy2(path, b)
        backups[path] = b
        print("backup   %s" % os.path.basename(b))

    for path, (src, before) in resolved.items():
        io.open(path, "w", encoding="utf-8").write(src)
        print("spliced  %-16s %d -> %d bytes" % (os.path.basename(path), before, len(src)))

    def restore(why):
        for p, b in backups.items():
            shutil.copy2(b, p)
        sys.exit("ABORT: %s. All backups restored." % why)

    for path in resolved:
        try:
            py_compile.compile(path, doraise=True)
        except Exception as e:
            restore("compile failed on %s\n%s" % (os.path.basename(path), e))
    print("compile  OK  (both files)")

    app_src = io.open(APP, encoding="utf-8").read()
    hub_src = io.open(HUB, encoding="utf-8").read()
    checks = [
        ("route registered",      '@app.route("/admin/r2-picks")' in app_src),
        ("read only by default",  'apply_ = request.args.get("apply") == "1"' in app_src),
        ("ON CONFLICT DO NOTHING", "ON CONFLICT (pick_date, game_id, pick_type) DO NOTHING" in app_src),
        ("no DO UPDATE in route", "DO UPDATE" not in ROUTE),
        ("no literal percent in SQL",
         "%" not in ROUTE.split("INSERT INTO picks", 1)[1].split('"""', 1)[0].replace("%s", "")),
        ("admin card added",      "/admin/r2-picks" in hub_src),
        ("export route intact",   '@app.route("/admin/export/picks.csv")' in app_src),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        restore("%d post-check(s) failed" % len(bad))

    print("")
    print("Done. DEPLOY IN THE MORNING, after the 6am run, before first pitch.")
    print("Then visit, read only:")
    print("  statalizers.com/admin/r2-picks?date=2026-09-17")
    return 0


if __name__ == "__main__":
    sys.exit(main())
