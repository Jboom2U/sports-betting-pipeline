"""
routes/admin.py
Model control panel — adjust weights, preview pick impact, save config.

Routes:
  GET  /admin/model-config           — control panel UI
  GET  /admin/model-config/values    — current config JSON
  POST /admin/model-config/save      — save config to DB
  POST /admin/model-config/preview   — preview today's picks with new weights
  POST /admin/model-config/preset    — load a named preset
"""

import logging
from flask import Blueprint, Response, request, jsonify, current_app

from db.model_config import load_config, save_config, get_defaults, get_presets, DEFAULTS

log = logging.getLogger(__name__)

admin_bp = Blueprint("admin", __name__)

import os as _os

@admin_bp.before_request
def _require_admin_login():
    from flask import session as _sess, redirect as _redir, request as _req
    _pw = _os.environ.get("ADMIN_PASSWORD", "")
    if _pw and not _sess.get("admin_auth"):
        return _redir(f"/admin/login?next={_req.path}")



# ── HTML ──────────────────────────────────────────────────────────────────────

def _panel_html() -> str:
    defaults = get_defaults()
    presets  = list(get_presets().keys())
    cfg      = load_config()

    # Group weights for the UI
    groups = {}
    for name, meta in defaults.items():
        g = meta.get("group", "other")
        groups.setdefault(g, []).append((name, meta, cfg.get(name, meta["value"])))

    group_labels = {
        "core":        "Core model",
        "pitcher":     "Pitcher signals",
        "market":      "Market signals",
        "environment": "Environmental",
        "bullpen":     "Bullpen fatigue",
        "tiers":       "Confidence thresholds",
    }

    sliders_html = ""
    for grp_key in ["core", "pitcher", "market", "environment", "bullpen", "tiers"]:
        items = groups.get(grp_key, [])
        if not items:
            continue
        sliders_html += f'<div class="group"><div class="group-title">{group_labels.get(grp_key, grp_key)}</div>'
        for name, meta, val in items:
            label  = meta.get("label", name)
            mn     = meta.get("min", 0)
            mx     = meta.get("max", 1)
            step   = meta.get("step", 0.01)
            sliders_html += f"""
<div class="slider-row" id="row-{name}">
  <label class="s-label" for="s-{name}">{label}</label>
  <input type="range" id="s-{name}" data-name="{name}"
         min="{mn}" max="{mx}" step="{step}" value="{val}"
         oninput="onSlider(this)">
  <span class="s-val" id="v-{name}">{val}</span>
</div>"""
        sliders_html += "</div>"

    presets_html = "".join(
        f'<button class="preset-btn" onclick="loadPreset(\'{p}\')">{p}</button>'
        for p in presets
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>⚾ Statalizers — Model Config</title>
<style>
:root {{
  --bg:#0d1117;--bg2:#161b22;--bg3:#21262d;--border:#30363d;
  --text:#e6edf3;--muted:#8b949e;--accent:#58a6ff;
  --green:#3fb950;--red:#f85149;--gold:#d29922;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;line-height:1.6}}
.nav{{background:var(--bg2);border-bottom:1px solid var(--border);padding:.75rem 1.5rem;display:flex;align-items:center;gap:1.5rem}}
.nav-logo{{font-size:15px;font-weight:600}}
.nav a{{color:var(--muted);font-size:13px;text-decoration:none}}
.nav a:hover,.nav a.active{{color:var(--text)}}
.container{{max-width:960px;margin:0 auto;padding:2rem 1.5rem}}
.page-head{{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:1.5rem;gap:1rem;flex-wrap:wrap}}
h1{{font-size:20px;font-weight:600}}
.sub{{color:var(--muted);font-size:13px;margin-top:.2rem}}
.active-config{{font-size:12px;color:var(--muted);margin-top:.5rem}}
.active-config span{{color:var(--text);font-weight:600}}
.presets{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:1.5rem;align-items:center}}
.presets-label{{font-size:12px;color:var(--muted);margin-right:2px}}
.preset-btn{{font-size:12px;padding:4px 10px;border-radius:20px;border:1px solid var(--border);color:var(--muted);background:transparent;cursor:pointer}}
.preset-btn:hover{{border-color:var(--accent);color:var(--accent)}}
.two-col{{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1rem}}
@media(max-width:640px){{.two-col{{grid-template-columns:1fr}}}}
.group{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:1rem 1.25rem;margin-bottom:1rem}}
.group-title{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:.875rem}}
.slider-row{{display:grid;grid-template-columns:160px 1fr 52px;align-items:center;gap:10px;margin-bottom:.75rem}}
.slider-row:last-child{{margin-bottom:0}}
.s-label{{font-size:13px;color:var(--muted)}}
.s-val{{font-size:13px;font-weight:600;text-align:right}}
input[type=range]{{width:100%;accent-color:var(--accent)}}
.action-row{{display:flex;gap:8px;margin-top:1.5rem;flex-wrap:wrap;align-items:center}}
.btn{{font-size:13px;padding:.5rem 1rem;border-radius:6px;border:1px solid var(--border);background:var(--bg3);color:var(--text);cursor:pointer}}
.btn:hover{{border-color:var(--accent);color:var(--accent)}}
.btn-primary{{background:var(--accent);color:#000;border-color:transparent;font-weight:600}}
.btn-primary:hover{{opacity:.85;color:#000}}
.btn-save{{background:var(--green);color:#000;border-color:transparent;font-weight:600}}
.btn-save:hover{{opacity:.85}}
.btn:disabled{{opacity:.45;cursor:not-allowed}}
.preview-card{{background:var(--bg3);border-radius:8px;padding:1rem 1.25rem;margin-top:1.25rem}}
.preview-title{{font-size:12px;font-weight:600;color:var(--muted);margin-bottom:.75rem}}
.change-row{{display:flex;align-items:center;justify-content:space-between;font-size:12px;padding:.35rem 0;border-bottom:1px solid var(--border)}}
.change-row:last-child{{border-bottom:none}}
.change-before{{color:var(--muted)}}
.change-arrow{{color:var(--muted);padding:0 4px}}
.up{{color:var(--green);font-weight:600}}
.dn{{color:var(--red);font-weight:600}}
.same{{color:var(--muted)}}
.toast{{position:fixed;bottom:1.5rem;right:1.5rem;background:var(--green);color:#000;font-weight:600;font-size:13px;padding:.625rem 1rem;border-radius:8px;display:none;z-index:999}}
</style>
</head>
<body>
<nav class="nav">
  <span class="nav-logo">⚾ Statalizers</span>
  <a href="/">Dashboard</a>
  <a href="/performance-html">Performance</a>
  <a href="/analytics">Analytics</a>
  <a href="/admin/model-config" class="active">Model Config</a>
  <a href="/status">Status</a>
</nav>
<div class="container">
  <div class="page-head">
    <div>
      <h1>Model control panel</h1>
      <p class="sub">Adjust signal weights, preview today's pick changes, save config.</p>
      <p class="active-config">Active config: <span id="active-preset">custom</span></p>
    </div>
    <div class="action-row" style="margin:0">
      <button class="btn btn-primary" id="previewBtn" onclick="previewPicks()">Preview picks</button>
      <button class="btn btn-save" onclick="saveCfg()">Save config</button>
      <button class="btn" onclick="resetDefaults()">Reset to default</button>
    </div>
  </div>

  <div class="presets">
    <span class="presets-label">Presets:</span>
    {presets_html}
  </div>

  {sliders_html}

  <div id="preview-area"></div>
</div>
<div class="toast" id="toast">Saved!</div>

<script>
const DEFAULTS_META = {{}};
// Populate defaults from server-side
const defaultValues = {{}};

async function loadCurrentConfig() {{
  try {{
    const r = await fetch("/admin/model-config/values");
    const d = await r.json();
    Object.entries(d.config || {{}}).forEach(([k,v]) => {{
      const inp = document.getElementById("s-" + k);
      const out = document.getElementById("v-" + k);
      if (inp) {{ inp.value = v; out.textContent = formatVal(k, v); }}
    }});
  }} catch(e) {{ console.warn("Could not load config:", e); }}
}}

function onSlider(el) {{
  const name = el.dataset.name;
  const val  = parseFloat(el.value);
  document.getElementById("v-" + name).textContent = formatVal(name, val);
  document.getElementById("active-preset").textContent = "custom";
}}

function formatVal(name, v) {{
  v = parseFloat(v);
  if (name.includes("thresh") || name.startsWith("tier_") || name === "home_field_boost") {{
    return (v * 100).toFixed(1) + "%";
  }}
  if (name.includes("weight") || name.includes("blend") || name.includes("delta") || name.includes("adj")) {{
    if (Math.abs(v) < 0.1) return (v * 100).toFixed(1) + "%";
    return (v * 100).toFixed(0) + "%";
  }}
  return v % 1 === 0 ? v.toString() : parseFloat(v.toFixed(3)).toString();
}}

function getCurrent() {{
  const cfg = {{}};
  document.querySelectorAll("input[type=range][data-name]").forEach(el => {{
    cfg[el.dataset.name] = parseFloat(el.value);
  }});
  return cfg;
}}

async function previewPicks() {{
  const btn = document.getElementById("previewBtn");
  btn.disabled = true; btn.textContent = "Running…";
  try {{
    const r = await fetch("/admin/model-config/preview", {{
      method: "POST",
      headers: {{"Content-Type":"application/json"}},
      body: JSON.stringify(getCurrent())
    }});
    const d = await r.json();
    renderPreview(d);
  }} catch(e) {{
    document.getElementById("preview-area").innerHTML =
      '<div style="color:var(--red);font-size:13px;margin-top:1rem">Preview failed: ' + e.message + '</div>';
  }} finally {{
    btn.disabled = false; btn.textContent = "Preview picks";
  }}
}}

function renderPreview(data) {{
  const changes = data.changes || [];
  const area    = document.getElementById("preview-area");
  if (!changes.length) {{
    area.innerHTML = '<div class="preview-card"><div class="preview-title">No pick changes with current settings.</div></div>';
    return;
  }}
  let rows = changes.map(c => {{
    const beforeTier = c.before_tier || "";
    const afterTier  = c.after_tier  || "";
    const beforeConf = ((c.before_conf || 0)*100).toFixed(1) + "%";
    const afterConf  = ((c.after_conf  || 0)*100).toFixed(1) + "%";
    const changed    = afterTier !== beforeTier;
    const cls        = changed ? (c.after_conf > c.before_conf ? "up" : "dn") : "same";
    return `<div class="change-row">
      <span style="color:var(--text)">${{c.game}} — ${{c.label}}</span>
      <span>
        <span class="change-before">${{beforeTier}} ${{beforeConf}}</span>
        <span class="change-arrow">→</span>
        <span class="${{cls}}">${{afterTier}} ${{afterConf}}</span>
      </span>
    </div>`;
  }}).join("");

  const promoted = changes.filter(c => c.after_conf > c.before_conf).length;
  const demoted  = changes.filter(c => c.after_conf < c.before_conf).length;
  area.innerHTML = `<div class="preview-card">
    <div class="preview-title">${{changes.length}} picks affected · ${{promoted}} confidence up · ${{demoted}} down</div>
    ${{rows}}
  </div>`;
}}

async function saveCfg() {{
  const r = await fetch("/admin/model-config/save", {{
    method: "POST",
    headers: {{"Content-Type":"application/json"}},
    body: JSON.stringify(getCurrent())
  }});
  const d = await r.json();
  if (d.ok) {{
    const t = document.getElementById("toast");
    t.style.display = "block";
    setTimeout(() => t.style.display = "none", 2500);
  }}
}}

async function loadPreset(name) {{
  const r = await fetch("/admin/model-config/preset", {{
    method: "POST",
    headers: {{"Content-Type":"application/json"}},
    body: JSON.stringify({{preset: name}})
  }});
  const d = await r.json();
  if (d.config) {{
    Object.entries(d.config).forEach(([k,v]) => {{
      const inp = document.getElementById("s-" + k);
      const out = document.getElementById("v-" + k);
      if (inp) {{ inp.value = v; out.textContent = formatVal(k, v); }}
    }});
    document.getElementById("active-preset").textContent = name;
  }}
}}

async function resetDefaults() {{
  await loadPreset("default-v1");
  document.getElementById("active-preset").textContent = "default-v1";
}}

loadCurrentConfig();
</script>
</body>
</html>"""


# ── Routes ─────────────────────────────────────────────────────────────────────

@admin_bp.route("/admin/model-config")
def model_config_panel():
    return Response(_panel_html(), mimetype="text/html")


@admin_bp.route("/admin/model-config/values")
def model_config_values():
    cfg  = load_config()
    meta = get_defaults()
    return jsonify({"config": cfg, "meta": {k: v for k, v in meta.items()}})


@admin_bp.route("/admin/model-config/save", methods=["POST"])
def model_config_save():
    body = request.get_json(silent=True) or {}
    ok   = save_config(body)
    return jsonify({"ok": ok})


@admin_bp.route("/admin/model-config/preset", methods=["POST"])
def model_config_preset():
    body    = request.get_json(silent=True) or {}
    name    = body.get("preset", "default-v1")
    presets = get_presets()
    if name not in presets:
        return jsonify({"error": f"Unknown preset: {name}"}), 404
    return jsonify({"preset": name, "config": presets[name]})


@admin_bp.route("/admin/model-config/preview", methods=["POST"])
def model_config_preview():
    """
    Re-score today's games with submitted weights and return pick diff vs current config.
    Uses the app's cached model (already loaded) — no new scraping.
    """
    new_cfg = request.get_json(silent=True) or {}
    if not new_cfg:
        return jsonify({"changes": []})

    try:
        # Load model on demand (not stored on app object)
        from model.mlb_model import MLBModel
        from model.mlb_picks import generate_picks
        from db.model_config import load_config

        model = MLBModel()
        model.load()
        if not model._loaded:
            return jsonify({"error": "Model data not available — run the pipeline first"}), 503

        # Current config = DB config (same as what the live picks use)
        old_cfg = load_config() or model.cfg.copy()
        model.cfg = old_cfg

        # Score with current config
        old_games, _ = model.score_today()
        old_picks_by_key = {}
        for p in generate_picks(old_games, old_cfg):
            key = (p.get("game", ""), p.get("label", ""))
            old_picks_by_key[key] = p

        # Score with new config
        merged_cfg = {**old_cfg, **new_cfg}
        model.cfg = merged_cfg
        new_games, _ = model.score_today()
        new_picks = generate_picks(new_games, merged_cfg)

        # Diff
        changes = []
        for p in new_picks:
            key = (p.get("game", ""), p.get("label", ""))
            old = old_picks_by_key.get(key)
            new_conf = p.get("conf", 0)
            old_conf = old.get("conf", 0) if old else 0
            if abs(new_conf - old_conf) >= 0.005 or p.get("tier") != (old or {}).get("tier"):
                changes.append({
                    "game":        p.get("game", ""),
                    "label":       p.get("label", ""),
                    "before_conf": old_conf,
                    "after_conf":  new_conf,
                    "before_tier": (old or {}).get("tier", "—"),
                    "after_tier":  p.get("tier", "—"),
                })

        changes.sort(key=lambda x: abs(x["after_conf"] - x["before_conf"]), reverse=True)
        return jsonify({"changes": changes[:20]})

    except Exception as e:
        log.warning(f"Preview error: {e}")
        return jsonify({"error": str(e)}), 500
