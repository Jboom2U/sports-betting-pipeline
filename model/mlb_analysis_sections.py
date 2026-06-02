"""
model/mlb_analysis_sections.py
Pre-rendered HTML sections injected into the dashboard by run_picks_html.py.

  build_hr_watch(props, picks)      -> HTML string — top HR candidates today
  build_monte_carlo(picks)          -> HTML string — 1000-trial ML simulation
"""

# ─────────────────────────────────────────────────────────────────────────────
# HR WATCH
# ─────────────────────────────────────────────────────────────────────────────

def build_hr_watch(all_props, all_picks):
    """
    Return an HTML table of the top HR candidates for today's games.
    Ranked by Poisson HR probability.  Only confirmed (non-projected) props shown.
    """
    hr_candidates = [
        p for p in all_props
        if p.get("prop_type") == "HR"
        and not p.get("projected")
        and p.get("confidence", 0) >= 0.08
    ]
    hr_candidates.sort(key=lambda x: x.get("confidence", 0), reverse=True)
    top = hr_candidates[:10]

    if not top:
        return (
            '<p style="color:#8b949e;padding:20px">'
            "No HR candidates today &mdash; lineups not yet confirmed.</p>"
        )

    thead = (
        '<div style="overflow-x:auto">'
        '<table style="width:100%;border-collapse:collapse;font-size:.88rem">'
        '<thead><tr style="border-bottom:2px solid #30363d;color:#8b949e;'
        'font-size:.75rem;text-transform:uppercase;letter-spacing:.05em">'
        '<th style="padding:8px;text-align:left">Player</th>'
        '<th style="padding:8px;text-align:left">Game</th>'
        '<th style="padding:8px;text-align:center">Order</th>'
        '<th style="padding:8px;text-align:center">HR Prob</th>'
        '<th style="padding:8px;text-align:center">Park HR Factor</th>'
        '<th style="padding:8px;text-align:center">Pitcher HR/9</th>'
        '<th style="padding:8px;text-align:center">Wind</th>'
        '<th style="padding:8px;text-align:left">Season Rate</th>'
        "</tr></thead><tbody>"
    )
    tfoot = (
        "</tbody></table></div>"
        '<p style="margin-top:12px;color:#444;font-size:.75rem">'
        "HR Prob = Poisson model (season HR/PA &times; park factor &times; "
        "pitcher HR/9 &times; wind). "
        "Park &gt;1.10 = hitter-friendly. Pitcher HR/9 &gt;1.40 = HR-prone.</p>"
    )

    rows = []
    for c in top:
        prob       = c.get("confidence", 0)
        pct        = "{:.1%}".format(prob)
        bar_w      = min(80, int(prob * 400))
        park_f     = c.get("hr_park_factor", 1.0)
        pitcher_h9 = c.get("hr_pitcher_hr9", 1.20)
        wind       = c.get("hr_wind_note", "")
        game_lbl   = c.get("game", "")
        tier       = c.get("tier", "")
        order      = c.get("batting_order", "?")
        base_rate  = c.get("hr_base_rate", 0)

        tc  = {"LOCK": "#ffd700", "STRONG": "#4fc3f7", "LEAN": "#00e676"}.get(tier, "#8b949e")
        pkc = "#ef9a9a" if park_f >= 1.10 else "#a5d6a7" if park_f <= 0.90 else "#8b949e"
        pic = "#ef9a9a" if pitcher_h9 >= 1.40 else "#a5d6a7" if pitcher_h9 <= 0.90 else "#8b949e"
        ws  = ('<span style="color:#f59e0b">' + wind + "</span>") if wind else '<span style="color:#444">&#8212;</span>'
        bar = (
            '<span style="display:inline-block;width:' + str(bar_w)
            + 'px;height:6px;background:' + tc
            + ';border-radius:3px;vertical-align:middle;opacity:.7"></span>'
        )

        rows.append(
            '<tr style="border-bottom:1px solid #21262d">'
            + '<td style="padding:10px 8px;font-weight:600;color:#e6edf3">'
            + c.get("player_name", "") + "</td>"
            + '<td style="padding:10px 8px;color:#8b949e;font-size:.82rem">'
            + game_lbl + "</td>"
            + '<td style="padding:10px 8px;text-align:center;color:#8b949e">#'
            + str(order) + "</td>"
            + '<td style="padding:10px 8px;text-align:center">'
            + '<span style="color:' + tc + ';font-weight:700">' + pct + "</span>&nbsp;" + bar
            + "</td>"
            + '<td style="padding:10px 8px;text-align:center;color:' + pkc + '">'
            + "{:.2f}x".format(park_f) + "</td>"
            + '<td style="padding:10px 8px;text-align:center;color:' + pic + '">'
            + "{:.2f}".format(pitcher_h9) + "</td>"
            + '<td style="padding:10px 8px;text-align:center">' + ws + "</td>"
            + '<td style="padding:10px 8px;color:#8b949e;font-size:.8rem">'
            + "{:.3f}/PA".format(base_rate) + "</td>"
            + "</tr>"
        )

    return thead + "".join(rows) + tfoot


# ─────────────────────────────────────────────────────────────────────────────
# MONTE CARLO
# ─────────────────────────────────────────────────────────────────────────────

def build_monte_carlo(all_picks):
    """
    Return an HTML table showing 1000-trial Monte Carlo hit rates for each ML pick.
    Compares model win probability vs market implied probability (EV tag).
    """
    import random
    random.seed(42)

    def american_to_implied(odds):
        try:
            o = float(odds)
            return 100.0 / (o + 100) if o > 0 else (-o) / (-o + 100)
        except Exception:
            return None

    def ev_tag(model_p, market_p):
        if market_p is None:
            return "No Market", "#555555"
        edge = model_p - market_p
        if edge >= 0.05:
            return ("+EV dog" if model_p < 0.50 else "Strong +EV"), "#00e676"
        if edge >= 0.02:
            return "Slight +EV", "#69f0ae"
        if edge >= -0.02:
            return "Fair", "#8b949e"
        if edge >= -0.05:
            return "Thin -EV", "#ff7043"
        return ("Slight -EV" if edge >= -0.08 else "-EV"), "#ef5350"

    ml_picks = [
        p for p in all_picks
        if p.get("pick_type", p.get("type", "")) == "ML" and p.get("confidence", p.get("conf", 0)) >= 0.60
    ]
    if not ml_picks:
        return (
            '<p style="color:#8b949e;padding:20px">'
            "No ML picks meet the 60%+ threshold today.</p>"
        )

    thead = (
        '<p style="color:#8b949e;font-size:.82rem;margin-bottom:16px">'
        "Bars show how often a pick would hit in 1000 simulated games using model "
        "probabilities. (Example: 58% &asymp; 580 hits out of 1000.) "
        "EV tag compares model probability vs market implied odds.</p>"
        '<div style="overflow-x:auto">'
        '<table style="width:100%;border-collapse:collapse;font-size:.88rem">'
        '<thead><tr style="border-bottom:2px solid #30363d;color:#8b949e;'
        'font-size:.75rem;text-transform:uppercase;letter-spacing:.05em">'
        '<th style="padding:8px;text-align:left">Game</th>'
        '<th style="padding:8px;text-align:left">ML Pick</th>'
        '<th style="padding:8px;text-align:center">Model Win Prob</th>'
        '<th style="padding:8px;text-align:center">Market Implied</th>'
        '<th style="padding:8px;text-align:center">Hit-Rate Bar (1000 Sims)</th>'
        '<th style="padding:8px;text-align:center">EV Tag</th>'
        "</tr></thead><tbody>"
    )

    rows = []
    for p in sorted(ml_picks, key=lambda x: -x.get("confidence", 0)):
        model_p  = p.get("confidence", p.get("conf", 0))
        team     = p.get("team", p.get("label", ""))
        game_lbl = p.get("game", "")
        away     = p.get("away", "")
        tier     = p.get("tier", "")
        is_away  = bool(
            away and team
            and (away.lower() in team.lower() or team.lower() in away.lower())
        )
        pick_odds  = p.get("ml_away_odds") if is_away else p.get("ml_home_odds")
        market_imp = american_to_implied(pick_odds)

        hits    = sum(1 for _ in range(1000) if random.random() < model_p)
        hit_pct = hits / 10.0

        tag, tag_color = ev_tag(model_p, market_imp)
        tc       = {"LOCK": "#ffd700", "STRONG": "#4fc3f7", "LEAN": "#00e676"}.get(tier, "#8b949e")
        bar_fill = int(hit_pct)
        mkt_str  = "~{:.0%}".format(market_imp) if market_imp else "N/A"

        bar = (
            '<span style="display:inline-block;background:#21262d;border-radius:4px;'
            'width:120px;height:18px;vertical-align:middle;position:relative;overflow:hidden">'
            '<span style="display:block;width:' + str(bar_fill)
            + "%;height:100%;background:" + tc + ';opacity:.8"></span>'
            + "</span>&nbsp;"
            + '<span style="color:' + tc + ';font-weight:600">'
            + "{:.0f}%".format(hit_pct) + "</span>"
        )
        tag_span = (
            '<span style="background:' + tag_color + "22;color:" + tag_color
            + ";border:1px solid " + tag_color + "55;"
            "border-radius:12px;padding:2px 10px;font-size:.78rem;font-weight:600\">"
            + tag + "</span>"
        )

        rows.append(
            '<tr style="border-bottom:1px solid #21262d">'
            + '<td style="padding:10px 8px;color:#8b949e;font-size:.82rem">'
            + game_lbl + "</td>"
            + '<td style="padding:10px 8px;font-weight:600;color:#e6edf3">'
            + team + " ML</td>"
            + '<td style="padding:10px 8px;text-align:center;color:'
            + tc + ';font-weight:700">' + "{:.0%}".format(model_p) + "</td>"
            + '<td style="padding:10px 8px;text-align:center;color:#8b949e">'
            + mkt_str + "</td>"
            + '<td style="padding:10px 8px;text-align:center">' + bar + "</td>"
            + '<td style="padding:10px 8px;text-align:center">' + tag_span + "</td>"
            + "</tr>"
        )

    return thead + "".join(rows) + "</tbody></table></div>"
