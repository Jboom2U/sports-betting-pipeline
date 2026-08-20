"""
admin_hub.py: the /admin page.

WHY THIS IS A MODULE AND NOT INLINE HTML IN app.py (created 2026-08-18)

Justin, looking at the old page: "the current state of the admin page is
terrible hard to read and navigate."

He was right, and the cause is worth recording because it is structural rather
than cosmetic. The old hub was ~130 lines of hand written HTML inside a triple
quoted string in app.py. Every new route appended another hand written card, and
five of them were written by a different hand using <b> and <span> instead of
<div class="card-title"> and <div class="card-desc">. Both of those are INLINE
elements, so they render with no line break, which is why the page showed
"Prop match diagnosticWhy a real prop line is not reaching the board."

The fix is not to correct those five. It is to make the mistake impossible.
Every card here is DATA in ROUTES, rendered through one _card() function. A new
route is a tuple, so it cannot be markup-wrong.

Two other things the old page got wrong, both safety rather than looks:

  1. The two buttons that SPEND ODDS API QUOTA looked exactly like the free ones
     and sat in different sections (props-pull under Diagnostics, force-oddsapi
     under Actions). On a 500 request monthly cap that is a trap. They now live
     in their own section with their cost stated on the card.
  2. "Actions (run on click)" contained several read-only pages (real-roi, clv,
     bets). A header promising side effects over links that have none teaches
     you to ignore the header.
"""
from __future__ import annotations

import html

from model import checklist as CL

try:
    from db import checklist_store as CS
except Exception:                                   # page must render regardless
    CS = None

try:
    import pull_log as PL
except Exception:
    PL = None


# --------------------------------------------------------------------- routes
# (href, title, description, badge, cost)
#   badge: "public" | "admin"
#   cost:  "" | "free" | "quota" | "danger"

ROUTES = [
    ("Daily views", "What you look at every day", [
        ("/", "Main dashboard", "Today's picks, Best Bets, Daily Summary", "public", ""),
        ("/ask", "Statalizer Bot", "Ask about any game, matchup or prop", "public", ""),
        ("/performance-html", "Performance tracker",
         "W/L/ROI by tier and type, sharp action, 7 to 90 day toggles", "public", ""),
        ("/admin/bets", "My bets", "What you actually staked: P/L and your own CLV", "admin", ""),
    ]),
    ("Analysis", "Read the record", [
        ("/admin/analysis", "Nightly analysis report",
         "Day review plus trends. Date picker for any past day, download, email", "admin", ""),
        ("/admin/real-roi", "Real price ROI",
         "ROI from stored prices, not a flat -110. Read this before trusting any backtest", "admin", ""),
        ("/admin/clv", "Closing line value",
         "Did you beat the close. Settles far sooner than win rate", "admin", ""),
        ("/admin/calibration", "Calibration",
         "Predicted vs actual by confidence band, tier and type", "admin", ""),
        ("/admin/calibration-fit", "Calibration fit",
         "Per type Platt coefficients from live graded picks. Read only", "admin", ""),
        ("/admin/strategy-backtest", "Strategy backtest",
         "Threshold rules replayed on graded history, walk forward validated", "admin", ""),
        ("/admin/loss-analysis", "Loss analysis",
         "Reverse engineers losses by type, tier, band and sharp divergence", "admin", ""),
        ("/admin/export/picks.csv", "Export graded picks",
         "CSV of every graded pick with prices and results, for outside analysis", "admin", ""),
        ("/admin/export/scores.csv", "Export all game scores",
         "Finals for every game, the control group for any cover-rate test", "admin", ""),
        ("/analytics", "Analytics dashboard",
         "Natural language database queries over pick history", "admin", ""),
        ("/admin/model-config", "Model control panel",
         "Tune signal weights, preview impact, save config", "admin", ""),
    ]),
    ("Diagnostics", "Read only. Nothing is written and no quota is spent", [
        ("/admin/signal-audit", "Signal audit",
         "Which model inputs actually vary across the slate. Run this before touching weights",
         "admin", "free"),
        ("/admin/pinnacle-odds-test", "Pinnacle odds diagnostic",
         "Dry run ML, RL and total parse. No writes", "admin", "free"),
        ("/admin/pinnacle-k-test", "Pinnacle K line test",
         "Live strikeout lines and prices, parse check", "admin", "free"),
        ("/admin/pinnacle-props-scan", "Pinnacle props scan",
         "Which prop markets really exist, with real lines", "admin", "free"),
        ("/admin/prop-match-diag", "Prop match diagnostic",
         "Why a real prop line is not reaching the board", "admin", "free"),
        ("/admin/props-diag", "Props diagnostic",
         "Props scored today vs saved and graded per date", "admin", "free"),
        ("/admin/pitcher-diag", "Pitcher diagnostic",
         "Season rows, split rows and era_adj for one pitcher by name", "admin", "free"),
        ("/status", "Pipeline status", "Last run, database and R2 health", "admin", "free"),
        ("/schedule-status", "Schedule status",
         "Next pipeline, next refresh, first pitch times as JSON", "admin", "free"),
    ]),
    ("Actions", "These change state when clicked. None of them spend quota", [
        ("/force-pipeline", "Force pipeline", "Trigger the full 6am pipeline now", "admin", "free"),
        ("/force-odds", "Force odds snapshot",
         "Pinnacle ML and RL only. Free, but no per book prices", "admin", "free"),
        ("/admin/refresh-signals", "Refresh signals",
         "Umpire, bullpen, pitcher and team stats", "admin", "free"),
        ("/admin/refresh-gamelogs", "Refresh game logs",
         "Seed player game logs from today's props and the 40 man rosters", "admin", "free"),
        ("/admin/grade-backfill", "Grade backfill",
         "Import and grade past picks from the R2 analysis JSONs", "admin", "free"),
        ("/unstick", "Unstick pipeline", "Clear a stuck pipeline state", "admin", "free"),
    ]),
    ("Spends Odds API quota", "500 requests a month, resets on the 1st. Check the cost before clicking", [
        ("/admin/force-oddsapi", "Pull Odds API",
         "3 credits. Per book prices including Hard Rock, plus the game total", "admin", "quota"),
        ("/admin/props-pull", "Pull batter props",
         "Per event, so cost scales with the slate. Shows the cost before you commit",
         "admin", "quota"),
    ]),
    ("Settings", "", [
        ("/admin/change-site-password", "Change site password",
         "Updates the public site login", "admin", "danger"),
        ("/admin/logout", "Sign out", "End this admin session", "admin", ""),
    ]),
]

_STATUS = {
    CL.DONE:    ("DONE",     "#3fb950", "#132d1c"),
    CL.OPEN:    ("OPEN",     "#f0883e", "#3a2317"),
    CL.PARTIAL: ("PARTIAL",  "#d29922", "#332816"),
    CL.MANUAL:  ("NO CHECK", "#8b949e", "#21262d"),
    CL.UNKNOWN: ("UNKNOWN",  "#a371f7", "#2a1f3d"),
}
_EFFORT = {"S": "small", "M": "medium", "L": "large"}


def _e(x) -> str:
    return html.escape(str(x if x is not None else ""))


def _card(href, title, desc, badge, cost) -> str:
    """The single place a route card is turned into markup.

    Every card goes through here, which is what stops the inline-element bug
    that produced "Prop match diagnosticWhy a real prop line..." from coming
    back the next time a route is added.
    """
    cls = "card" + (f" cost-{cost}" if cost in ("quota", "danger") else "")
    tag = ""
    if badge == "public":
        tag = '<span class="badge badge-public">Public</span>'
    if cost == "quota":
        tag = '<span class="badge badge-quota">Spends quota</span>'
    elif cost == "danger":
        tag = '<span class="badge badge-danger">Changes access</span>'
    return (f'<a class="{cls}" href="{_e(href)}">{tag}'
            f'<div class="card-title">{_e(title)}</div>'
            f'<div class="card-desc">{_e(desc)}</div></a>')


def _item(it) -> str:
    label, fg, bg = _STATUS.get(it["status"], _STATUS[CL.UNKNOWN])
    asserted = ('<span class="asserted" title="Set by hand. No probe exists for this item.">'
                'asserted</span>' if it.get("asserted") else "")
    note = (f'<div class="ev note">{_e(it["note"])}</div>'
            if it.get("note") else "")
    return (
        f'<div class="item" data-status="{it["status"]}">'
        f'  <span class="pill" style="color:{fg};background:{bg}">{label}</span>'
        f'  <div class="itembody">'
        f'    <div class="ititle">{_e(it["title"])}'
        f'      <span class="effort" title="rough size">{_EFFORT.get(it.get("effort"),"")}</span>'
        f'      {asserted}</div>'
        f'    <div class="why">{_e(it["why"])}</div>'
        f'    <div class="ev">{_e(it["evidence"])}</div>'
        f'    {note}'
        f'    <div class="where">{_e(it["where"])}</div>'
        f'  </div>'
        f'</div>')


def _pulls_block() -> str:
    """Today's odds pulls, read out of the snapshots that were actually written.

    Deliberately NOT built from a run log. A log records that a pull was
    attempted; this reports what landed, so a pull that fired and wrote nothing
    shows as zero games rather than a green tick. That difference is the whole
    reason the Polymarket scraper looked healthy for weeks while matching nothing.
    """
    if PL is None:
        return '<p class="empty">Pull log unavailable.</p>'
    try:
        data = PL.summary()
    except Exception as exc:
        return f'<p class="empty">Could not read the odds master: {_e(exc)}</p>'

    q = data["quota"]
    if q["known"]:
        colour = "#3fb950" if q["remaining"] > 150 else ("#d29922" if q["remaining"] > 75 else "#f85149")
        qhtml = (f'<div class="quota"><div style="font-size:12.5px">Odds API quota'
                 f'<span style="color:#6e7681"> &middot; resets on the 1st</span></div>'
                 f'<div class="qtrack"><i style="width:{q["pct"]}%;background:{colour}"></i></div>'
                 f'<div style="font-size:12.5px;color:{colour};font-weight:600">'
                 f'{q["remaining"]} left of {q["cap"]}</div>'
                 f'<div style="font-size:11px;color:#6e7681">read from the API on {_e(q["checked_et"])}</div>'
                 f'</div>')
    else:
        qhtml = ('<div class="quota"><div style="font-size:12.5px;color:#8b949e">'
                 'Odds API quota unknown. It is recorded from the API\'s own headers on the '
                 'next paid pull, so this fills in the first time you use one.</div></div>')

    if not data["pulls"]:
        return (qhtml + '<p class="empty">No odds pulls have landed today yet. '
                'If that is a surprise, check /status for the pipeline state.</p>')

    rows = []
    for p in data["pulls"]:
        src = p["source"]
        cls = ("pinnacle" if src.lower().startswith("pinn")
               else "oddsapi" if "odds" in src.lower() else "unknown")
        chips = "".join(
            f'<span class="chip {"on" if p[k] else "off"}">{lbl} {p[k]}</span>'
            for k, lbl in (("ml", "ML"), ("rl", "RL"), ("total", "total"), ("books", "books")))
        inf = ('<span class="inf" title="Written before the source column existed on '
               '2026-08-18, so this was worked out from the columns present.">inferred</span>'
               if p["inferred"] else "")
        gap = (f'<div class="pgap">{_e("; ".join(p["gaps"]))}</div>' if p["gaps"] else "")
        rows.append(
            f'<div class="pull"><div class="ptime">{_e(p["time_et"])}</div>'
            f'<span class="psrc {cls}">{_e(src)}</span>'
            f'<div class="pbody"><div class="pmain">{_e(p["games_label"])}{chips}{inf}</div>'
            f'{gap}</div></div>')
    return qhtml + "".join(rows)


def _notes_block(notes) -> str:
    if not notes:
        return ('<p class="empty">Nothing added yet. Anything you type above survives the '
                'session and the deploy, which chat does not.</p>')
    out = []
    for n in notes:
        st = (n.get("status") or "new").lower()
        colour = {"new": "#58a6ff", "accepted": "#3fb950",
                  "declined": "#8b949e", "done": "#3fb950"}.get(st, "#8b949e")
        when = n["created_at"].strftime("%b %d") if hasattr(n.get("created_at"), "strftime") else ""
        resp = (f'<div class="ev resp">{_e(n["response"])}</div>' if n.get("response") else
                ('<div class="ev pending">Waiting on a review.</div>' if st == "new" else ""))
        detail = f'<div class="why">{_e(n["detail"])}</div>' if n.get("detail") else ""
        out.append(
            f'<div class="item" data-status="note">'
            f'  <span class="pill" style="color:{colour};background:#21262d">{_e(st.upper())}</span>'
            f'  <div class="itembody">'
            f'    <div class="ititle">{_e(n["title"])}<span class="effort">{_e(when)}</span></div>'
            f'    {detail}{resp}'
            f'  </div></div>')
    return "".join(out)


_CSS = """
*{box-sizing:border-box}
body{background:#0d1117;color:#e6edf3;margin:0;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:14px}
nav{background:#161b22;border-bottom:1px solid #30363d;padding:.7rem 1.5rem;
  display:flex;align-items:center;gap:1.25rem;position:sticky;top:0;z-index:20}
.logo{font-weight:600;font-size:15px}
nav a{color:#8b949e;font-size:13px;text-decoration:none}
nav a:hover{color:#e6edf3}
nav .right{margin-left:auto;display:flex;gap:1.25rem}
.container{max-width:1080px;margin:1.5rem auto 4rem;padding:0 1.5rem}
h1{font-size:22px;font-weight:600;margin:0 0 .2rem}
.sub{color:#8b949e;font-size:13px;margin:0 0 1.5rem}
h2.sec{font-size:12px;color:#8b949e;margin:2.25rem 0 .25rem;text-transform:uppercase;
  letter-spacing:.08em;font-weight:600}
h2.sec .hint{text-transform:none;letter-spacing:0;font-weight:400;color:#6e7681;margin-left:.6rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(268px,1fr));gap:.75rem;margin-top:.75rem}
.card{background:#161b22;border:1px solid #30363d;border-radius:9px;padding:.85rem 1rem;
  text-decoration:none;color:inherit;display:block;transition:border-color .12s,background .12s}
.card:hover{border-color:#58a6ff;background:#1a2029}
.card-title{font-size:14px;font-weight:600;margin-bottom:.25rem;line-height:1.3}
.card-desc{font-size:12.5px;color:#8b949e;line-height:1.45}
.cost-quota{border-color:#7a4a12;background:#1d1710}
.cost-quota:hover{border-color:#d29922}
.cost-danger{border-color:#6e2b26;background:#1d1312}
.cost-danger:hover{border-color:#f85149}
.badge{display:inline-block;font-size:10px;padding:2px 7px;border-radius:20px;
  margin-bottom:.45rem;font-weight:700;letter-spacing:.03em}
.badge-public{background:#132d1c;color:#3fb950}
.badge-quota{background:#3a2a10;color:#d29922}
.badge-danger{background:#3a1a18;color:#f85149}

.bar{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:1rem 1.25rem;
  display:flex;align-items:center;gap:1.5rem;flex-wrap:wrap;margin-bottom:1rem}
.bignum{font-size:26px;font-weight:700;line-height:1}
.bar .lbl{font-size:11.5px;color:#8b949e;text-transform:uppercase;letter-spacing:.06em;margin-top:.2rem}
.track{flex:1;min-width:180px;height:7px;background:#21262d;border-radius:4px;overflow:hidden}
.track i{display:block;height:100%;background:#3fb950}
.meta{font-size:12px;color:#8b949e;line-height:1.6}

details.grp{border:1px solid #30363d;border-radius:9px;background:#161b22;margin-bottom:.6rem}
details.grp>summary{cursor:pointer;padding:.7rem 1rem;list-style:none;display:flex;
  align-items:center;gap:.7rem;font-weight:600;font-size:13.5px;user-select:none}
details.grp>summary::-webkit-details-marker{display:none}
details.grp>summary:hover{background:#1a2029;border-radius:9px}
.count{font-size:11.5px;color:#8b949e;font-weight:400;margin-left:auto;white-space:nowrap}
.gblurb{font-size:12px;color:#6e7681;font-weight:400;margin-left:.2rem}
.chev{color:#6e7681;font-size:11px;transition:transform .15s}
details[open]>summary .chev{transform:rotate(90deg)}
.items{padding:.15rem .85rem .85rem}
.item{display:flex;gap:.75rem;padding:.7rem .25rem;border-top:1px solid #21262d}
.item:first-child{border-top:none}
.pill{flex:0 0 auto;font-size:10px;font-weight:700;padding:3px 7px;border-radius:5px;
  letter-spacing:.04em;height:fit-content;margin-top:.1rem;min-width:62px;text-align:center}
.itembody{min-width:0}
.ititle{font-size:13.5px;font-weight:600;line-height:1.35}
.effort{font-size:10.5px;color:#6e7681;font-weight:400;margin-left:.5rem}
.asserted{font-size:10px;color:#d29922;background:#332816;padding:1px 6px;border-radius:4px;
  margin-left:.4rem;font-weight:600}
.why{font-size:12.5px;color:#8b949e;line-height:1.5;margin-top:.25rem}
.ev{font-size:12px;color:#7d8590;line-height:1.5;margin-top:.35rem;
  font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
  background:#0d1117;border-left:2px solid #30363d;padding:.35rem .6rem;border-radius:0 4px 4px 0}
.ev.note{border-left-color:#d29922}
.ev.resp{border-left-color:#3fb950;color:#8b949e;font-family:inherit}
.ev.pending{border-left-color:#58a6ff;color:#58a6ff;font-family:inherit}
.where{font-size:11px;color:#565f6a;margin-top:.3rem;font-family:ui-monospace,monospace}
.empty{color:#6e7681;font-size:12.5px;padding:.6rem .25rem}

.pulls{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:.35rem 1rem .85rem}
.pull{display:flex;align-items:flex-start;gap:.85rem;padding:.6rem .1rem;border-top:1px solid #21262d}
.pull:first-child{border-top:none}
.ptime{flex:0 0 74px;font-size:12.5px;color:#e6edf3;font-variant-numeric:tabular-nums;
  font-family:ui-monospace,SFMono-Regular,Menlo,monospace;padding-top:.1rem}
.psrc{flex:0 0 auto;font-size:10.5px;font-weight:700;padding:3px 8px;border-radius:5px;letter-spacing:.03em}
.psrc.pinnacle{background:#132d1c;color:#3fb950}
.psrc.oddsapi{background:#3a2a10;color:#d29922}
.psrc.unknown{background:#21262d;color:#8b949e}
.pbody{min-width:0;flex:1}
.pmain{font-size:13px;color:#e6edf3}
.chip{display:inline-block;font-size:10.5px;padding:1px 6px;border-radius:4px;margin-left:.35rem;
  background:#21262d;color:#8b949e;font-family:ui-monospace,monospace}
.chip.on{background:#132d1c;color:#3fb950}
.chip.off{background:#2a1a17;color:#f0883e}
.pgap{font-size:11.5px;color:#8b949e;margin-top:.25rem;line-height:1.5}
.inf{font-size:10px;color:#d29922;background:#332816;padding:1px 6px;border-radius:4px;margin-left:.4rem}
.quota{display:flex;align-items:center;gap:.85rem;padding:.6rem .1rem .7rem;
  border-bottom:1px solid #21262d;margin-bottom:.15rem;flex-wrap:wrap}
.qtrack{flex:1;min-width:140px;height:6px;background:#21262d;border-radius:4px;overflow:hidden}
.qtrack i{display:block;height:100%}
form.add{display:flex;gap:.5rem;margin:.75rem 0 0;flex-wrap:wrap}
form.add input[type=text]{flex:1;min-width:220px;padding:.55rem .8rem;border-radius:7px;
  border:1px solid #30363d;background:#0d1117;color:#e6edf3;font-size:13.5px;font-family:inherit}
form.add input[type=text]:focus{outline:none;border-color:#58a6ff}
form.add button{padding:.55rem 1.1rem;background:#238636;color:#fff;font-weight:600;
  border:none;border-radius:7px;cursor:pointer;font-size:13.5px}
form.add button:hover{opacity:.88}
#filter{width:100%;padding:.6rem .9rem;border-radius:8px;border:1px solid #30363d;
  background:#161b22;color:#e6edf3;font-size:13.5px;font-family:inherit;margin-bottom:1rem}
#filter:focus{outline:none;border-color:#58a6ff}
.flash{background:#132d1c;border:1px solid #238636;color:#3fb950;padding:.6rem .9rem;
  border-radius:8px;font-size:13px;margin-bottom:1rem}
.flash.bad{background:#2d1618;border-color:#6e2b26;color:#f85149}
.hide{display:none !important}
"""

_JS = """
(function(){
  var f = document.getElementById('filter');
  if (f) f.addEventListener('input', function(){
    var q = this.value.trim().toLowerCase();
    document.querySelectorAll('.item').forEach(function(el){
      el.classList.toggle('hide', !!q && el.textContent.toLowerCase().indexOf(q) < 0);
    });
    document.querySelectorAll('.card').forEach(function(el){
      el.classList.toggle('hide', !!q && el.textContent.toLowerCase().indexOf(q) < 0);
    });
    document.querySelectorAll('details.grp').forEach(function(d){
      var any = d.querySelectorAll('.item:not(.hide)').length;
      d.classList.toggle('hide', !!q && !any);
      if (q && any) d.open = true;
    });
    document.querySelectorAll('h2.sec').forEach(function(h){
      var g = h.nextElementSibling;
      if (g && g.classList.contains('grid'))
        h.classList.toggle('hide', !!q && !g.querySelectorAll('.card:not(.hide)').length);
    });
  });
  // Remember which groups are collapsed. Small thing, but the page is long.
  document.querySelectorAll('details.grp').forEach(function(d){
    var k = 'ck_' + d.dataset.key;
    try { if (localStorage.getItem(k) === '0') d.open = false; } catch(e){}
    d.addEventListener('toggle', function(){
      try { localStorage.setItem(k, d.open ? '1' : '0'); } catch(e){}
    });
  });
})();
"""


def render(flash: str = "", flash_bad: bool = False) -> str:
    state = CS.get_state() if CS else {}
    rows  = CL.rows(state=state)
    s     = CL.summary(rows)
    notes = CS.get_notes() if CS else []
    pending = sum(1 for n in notes if (n.get("status") or "") == "new")

    groups = []
    for g in CL.by_group(rows):
        open_n = sum(1 for i in g["items"] if i["status"] in (CL.OPEN, CL.PARTIAL))
        groups.append(
            f'<details class="grp" data-key="{_e(g["key"])}" open>'
            f'  <summary><span class="chev">&#9654;</span>{_e(g["label"])}'
            f'    <span class="gblurb">{_e(g["blurb"])}</span>'
            f'    <span class="count">{g["done"]} done &middot; {open_n} open of {g["total"]}</span>'
            f'  </summary>'
            f'  <div class="items">{"".join(_item(i) for i in g["items"])}</div>'
            f'</details>')

    route_html = []
    for label, hint, cards in ROUTES:
        hint_html = f'<span class="hint">{_e(hint)}</span>' if hint else ""
        route_html.append(f'<h2 class="sec">{_e(label)}{hint_html}</h2>'
                          f'<div class="grid">{"".join(_card(*c) for c in cards)}</div>')

    flash_html = (f'<div class="flash{" bad" if flash_bad else ""}">{_e(flash)}</div>'
                  if flash else "")
    db_warn = ("" if CS and (notes or True) else "")
    pending_html = (f'<div><div class="bignum" style="color:#58a6ff">{pending}</div>'
                    f'<div class="lbl">awaiting review</div></div>' if pending else "")

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Admin - Statalizers</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{_CSS}</style></head>
<body>
<nav><span class="logo">&#9918; Statalizers</span>
  <a href="/">Dashboard</a><a href="/admin">Admin</a><a href="/admin/bets">My bets</a>
  <span class="right"><a href="/admin/logout">Sign out</a></span>
</nav>
<div class="container">
  <h1>Admin</h1>
  <p class="sub">Open work, then every internal route.</p>
  {flash_html}

  <div class="bar">
    <div><div class="bignum">{s['done']}<span style="color:#6e7681;font-size:16px">/{s['total']}</span></div>
      <div class="lbl">complete</div></div>
    <div><div class="bignum" style="color:#f0883e">{s['open'] + s['partial']}</div>
      <div class="lbl">open</div></div>
    {pending_html}
    <div class="track"><i style="width:{s['pct']}%"></i></div>
    <div class="meta">{s['probed']} of {s['total']} items are checked against the code on every
      page load.<br>The remaining {s['manual']} are judgement calls and say so.</div>
  </div>

  <h2 class="sec">Today's data pulls<span class="hint">Read from the snapshots that actually landed, not from a run log</span></h2>
  <div class="pulls">{_pulls_block()}</div>

  <input id="filter" type="text" placeholder="Filter everything on this page: try devig, platoon, quota, statcast">

  <h2 class="sec">Your items<span class="hint">Type it here and it outlives the chat session</span></h2>
  <form class="add" method="post" action="/admin/checklist/add">
    <input type="text" name="title" placeholder="Something to add, fix or look into" maxlength="500" required>
    <button type="submit">Add</button>
  </form>
  <div class="items" style="padding-left:0;padding-right:0">{_notes_block(notes)}</div>

  <h2 class="sec">Checklist<span class="hint">Status comes from reading the code, not from a tick box</span></h2>
  {"".join(groups)}

  {"".join(route_html)}
</div>
<script>{_JS}</script>
</body></html>"""
