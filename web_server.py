#!/usr/bin/env python3
"""
web_server.py — Flask web UI for the Proxy IP Checker database.

Serves a dark-themed dashboard on http://127.0.0.1:8080 (default).
When run as a Tor hidden service, the same server is reachable at
your .onion address via the Tor Browser.

Usage:
    python web_server.py                    # start on localhost:8080
    python web_server.py --port 9090        # custom port
    python web_server.py --host 0.0.0.0    # listen on all interfaces
    python web_server.py --db path/to/proxies.db

Routes:
    /                   Dashboard (stats + proxy table)
    /api/proxies        JSON list of all proxies (optional ?status= &type=)
    /api/stats          JSON statistics summary
    /api/proxies/online.txt   Plain-text online proxy list (IP:PORT per line)
"""

import argparse
import json
import sys
from pathlib import Path

try:
    from flask import Flask, jsonify, render_template_string, request, Response
except ImportError:
    print("[!] Flask is required:  pip install flask", file=sys.stderr)
    sys.exit(1)

try:
    import proxy_db
except ImportError:
    print("[!] proxy_db.py not found — place it in the same directory.", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# HTML template (dark theme, no external resources required)
# ---------------------------------------------------------------------------

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Proxy IP Checker</title>
<style>
  :root {
    --bg:#0d1117; --bg2:#161b22; --bg3:#21262d;
    --border:#30363d; --text:#c9d1d9; --muted:#8b949e;
    --green:#3fb950; --red:#f85149; --blue:#58a6ff;
    --yellow:#d29922; --purple:#bc8cff; --orange:#e3b341;
  }
  *{box-sizing:border-box;margin:0;padding:0;}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;}
  a{color:var(--blue);text-decoration:none;}
  a:hover{text-decoration:underline;}

  /* ── Header ── */
  header{background:var(--bg2);border-bottom:1px solid var(--border);padding:14px 24px;
    display:flex;align-items:center;gap:12px;flex-wrap:wrap;}
  header h1{font-size:1.15rem;font-weight:700;color:var(--blue);white-space:nowrap;}
  .tag{background:var(--bg3);border:1px solid var(--border);border-radius:12px;
    padding:2px 10px;font-size:11px;color:var(--muted);}
  #live-dot{width:8px;height:8px;border-radius:50%;background:var(--green);
    flex-shrink:0;animation:pulse 2s infinite;}
  @keyframes pulse{0%,100%{opacity:1;}50%{opacity:.3;}}
  #refresh-info{margin-left:auto;color:var(--muted);font-size:12px;white-space:nowrap;}

  /* ── Layout ── */
  main{max-width:1500px;margin:0 auto;padding:20px 24px;}
  section-title{display:block;font-size:11px;color:var(--muted);text-transform:uppercase;
    letter-spacing:.08em;font-weight:600;margin-bottom:10px;}

  /* ── Global stat cards ── */
  .stat-row{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:22px;}
  .stat-card{background:var(--bg2);border:1px solid var(--border);border-radius:8px;
    padding:16px 20px;display:flex;flex-direction:column;gap:4px;}
  .stat-card .lbl{font-size:11px;color:var(--muted);text-transform:uppercase;
    letter-spacing:.06em;font-weight:600;}
  .stat-card .val{font-size:2rem;font-weight:800;line-height:1;}
  .val.blue{color:var(--blue);}
  .val.green{color:var(--green);}
  .val.red{color:var(--red);}

  /* ── Per-type table ── */
  .type-section{margin-bottom:22px;}
  .type-section h2{font-size:12px;color:var(--muted);text-transform:uppercase;
    letter-spacing:.08em;font-weight:600;margin-bottom:10px;}
  .type-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px;}
  .type-card{background:var(--bg2);border:1px solid var(--border);border-radius:8px;
    padding:14px 16px;cursor:pointer;transition:border-color .15s,background .15s;}
  .type-card:hover,.type-card.active{border-color:var(--blue);background:var(--bg3);}
  .type-card .tc-name{font-size:13px;font-weight:700;color:var(--text);margin-bottom:10px;
    display:flex;align-items:center;gap:6px;}
  .type-card .tc-name code{font-size:13px;background:var(--bg3);padding:1px 6px;
    border-radius:4px;color:var(--blue);}
  .tc-row{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px;}
  .tc-label{font-size:11px;color:var(--muted);}
  .tc-num{font-size:1.25rem;font-weight:700;}
  .tc-num.total{color:var(--text);}
  .tc-num.online{color:var(--green);}
  .tc-num.fallen{color:var(--red);}
  .tc-bar{height:4px;border-radius:2px;background:var(--bg3);margin-top:8px;overflow:hidden;}
  .tc-bar-fill{height:100%;background:var(--green);transition:width .4s;}

  /* ── Toolbar ── */
  .toolbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:12px;}
  .toolbar input,.toolbar select{
    background:var(--bg2);border:1px solid var(--border);border-radius:6px;
    color:var(--text);padding:7px 12px;font-size:13px;outline:none;}
  .toolbar input{flex:1;min-width:200px;}
  .toolbar input:focus,.toolbar select:focus{border-color:var(--blue);}
  .btn{background:var(--bg3);border:1px solid var(--border);border-radius:6px;
    color:var(--text);padding:7px 14px;cursor:pointer;font-size:13px;white-space:nowrap;}
  .btn:hover{border-color:var(--blue);color:var(--blue);}
  .btn.primary{background:var(--blue);border-color:var(--blue);color:#fff;font-weight:600;}
  .btn.primary:hover{opacity:.85;}
  #count-lbl{font-size:12px;color:var(--muted);white-space:nowrap;}

  /* ── Table ── */
  .table-wrap{overflow-x:auto;border-radius:8px;border:1px solid var(--border);}
  table{width:100%;border-collapse:collapse;}
  thead th{background:var(--bg2);color:var(--muted);font-weight:600;font-size:11px;
    text-transform:uppercase;letter-spacing:.06em;padding:10px 14px;text-align:left;
    border-bottom:1px solid var(--border);white-space:nowrap;position:sticky;top:0;}
  tbody tr:nth-child(odd){background:var(--bg);}
  tbody tr:nth-child(even){background:var(--bg2);}
  tbody tr:hover{background:var(--bg3);}
  td{padding:7px 14px;border-bottom:1px solid var(--border);vertical-align:middle;}
  td.mono{font-family:monospace;font-size:13px;}

  /* ── Badges ── */
  .badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;}
  .badge.online{background:#1a3f2a;color:var(--green);}
  .badge.fallen{background:#3f1a1a;color:var(--red);}
  .badge.elite{background:#1a2a3f;color:var(--blue);}
  .badge.anon{background:#2a2a1a;color:var(--yellow);}
  .badge.unk{background:var(--bg3);color:var(--muted);}

  .ping{font-family:monospace;}
  .ping.fast{color:var(--green);}
  .ping.med{color:var(--yellow);}
  .ping.slow{color:var(--red);}
  .ping.none{color:var(--muted);}
  .isp-cell{max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .copy-btn{background:none;border:none;color:var(--muted);cursor:pointer;
    padding:1px 4px;border-radius:3px;font-size:11px;}
  .copy-btn:hover{color:var(--blue);}

  .footer{text-align:center;color:var(--muted);font-size:12px;padding:24px 0 12px;}
</style>
</head>
<body>
<header>
  <span id="live-dot"></span>
  <h1>⚡ Proxy IP Checker</h1>
  <span class="tag">Live DB</span>
  <span class="tag">Tor-ready</span>
  <span id="refresh-info">Loading…</span>
</header>

<main>

<!-- ── Global stats ── -->
<div class="stat-row">
  <div class="stat-card">
    <div class="lbl">Total in DB</div>
    <div class="val blue" id="s-total">—</div>
  </div>
  <div class="stat-card">
    <div class="lbl">Online now</div>
    <div class="val green" id="s-online">—</div>
  </div>
  <div class="stat-card">
    <div class="lbl">Fallen</div>
    <div class="val red" id="s-fallen">—</div>
  </div>
</div>

<!-- ── Per-type breakdown ── -->
<div class="type-section">
  <h2>Proxies by type — click a card to filter the list</h2>
  <div class="type-grid" id="type-grid"></div>
</div>

<!-- ── Proxy list ── -->
<div class="toolbar">
  <input type="text" id="search" placeholder="🔍  Search IP, country, ISP…" oninput="applyFilter()">
  <select id="filter-status" onchange="applyFilter()">
    <option value="">All statuses</option>
    <option value="online">Online only</option>
    <option value="fallen">Fallen only</option>
  </select>
  <select id="filter-type" onchange="syncTypeCards();applyFilter()">
    <option value="">All types</option>
    <option>http</option><option>https</option>
    <option>socks4</option><option>socks4a</option>
    <option>socks5</option><option>socks5h</option>
  </select>
  <span id="count-lbl"></span>
  <a class="btn" href="/api/proxies/online.txt" target="_blank">⬇ online.txt</a>
  <a class="btn" href="/api/proxies" target="_blank">⬇ JSON</a>
  <button class="btn primary" onclick="refresh()">⟳ Refresh now</button>
</div>

<div class="table-wrap">
<table>
  <thead>
    <tr>
      <th>Status</th>
      <th>IP : Port</th>
      <th>Type</th>
      <th>Ping</th>
      <th>Anonymity</th>
      <th>Country</th>
      <th>City</th>
      <th>ISP</th>
      <th>Last checked</th>
    </tr>
  </thead>
  <tbody id="tbody"></tbody>
</table>
</div>

</main>
<div class="footer">Proxy IP Checker &mdash; auto-refreshes every 30 s &mdash; Tor-accessible</div>

<script>
const PROXY_TYPES = ['http','https','socks4','socks4a','socks5','socks5h'];
const REFRESH_INTERVAL = 30; // seconds
let allProxies = [];
let countdown = REFRESH_INTERVAL;
let statsData = {};

// ── Stats & type cards ───────────────────────────────────────────────────────
async function loadStats() {
  const r = await fetch('/api/stats');
  statsData = await r.json();

  document.getElementById('s-total').textContent  = fmt(statsData.total);
  document.getElementById('s-online').textContent = fmt(statsData.online);
  document.getElementById('s-fallen').textContent = fmt(statsData.fallen);

  renderTypeCards();
}

function renderTypeCards() {
  const grid = document.getElementById('type-grid');
  const activeTp = document.getElementById('filter-type').value;
  grid.innerHTML = '';

  PROXY_TYPES.forEach(t => {
    const d    = (statsData.by_type || {})[t] || {total:0,online:0,fallen:0};
    const tot  = (d.total  || 0);
    const on   = (d.online || 0);
    const fl   = (d.fallen || 0);
    const pct  = tot > 0 ? Math.round(on / tot * 100) : 0;

    const card = document.createElement('div');
    card.className = 'type-card' + (activeTp === t ? ' active' : '');
    card.dataset.type = t;
    card.innerHTML = \`
      <div class="tc-name"><code>\${t.toUpperCase()}</code></div>
      <div class="tc-row">
        <span class="tc-label">Total</span>
        <span class="tc-num total">\${fmt(tot)}</span>
      </div>
      <div class="tc-row">
        <span class="tc-label">Online</span>
        <span class="tc-num online">\${fmt(on)}</span>
      </div>
      <div class="tc-row">
        <span class="tc-label">Fallen</span>
        <span class="tc-num fallen">\${fmt(fl)}</span>
      </div>
      <div class="tc-bar"><div class="tc-bar-fill" style="width:\${pct}%"></div></div>
    \`;
    card.onclick = () => {
      const sel = document.getElementById('filter-type');
      const isActive = card.classList.contains('active');
      sel.value = isActive ? '' : t;
      syncTypeCards();
      applyFilter();
    };
    grid.appendChild(card);
  });
}

function syncTypeCards() {
  const activeTp = document.getElementById('filter-type').value;
  document.querySelectorAll('.type-card').forEach(c => {
    c.classList.toggle('active', c.dataset.type === activeTp);
  });
}

// ── Proxy list ───────────────────────────────────────────────────────────────
async function loadProxies() {
  const r = await fetch('/api/proxies?limit=5000');
  allProxies = await r.json();
  applyFilter();
}

function pingClass(ms) {
  if (!ms || ms <= 0) return 'none';
  if (ms < 500)  return 'fast';
  if (ms < 2000) return 'med';
  return 'slow';
}

function pingText(ms) {
  if (!ms || ms <= 0) return '—';
  return Math.round(ms) + ' ms';
}

function anonBadge(a) {
  if (!a || a === 'Unknown') return '<span class="badge unk">Unknown</span>';
  if (a === 'Elite')         return '<span class="badge elite">Elite</span>';
  return '<span class="badge anon">Anonymous</span>';
}

function fmt(n) { return (n || 0).toLocaleString(); }

function copyText(t) { navigator.clipboard.writeText(t).catch(()=>{}); }

function applyFilter() {
  const q  = document.getElementById('search').value.toLowerCase();
  const st = document.getElementById('filter-status').value;
  const tp = document.getElementById('filter-type').value;

  const filtered = allProxies.filter(p => {
    if (st && p.status     !== st) return false;
    if (tp && p.proxy_type !== tp) return false;
    if (q) {
      const hay = [p.ip, p.port, p.country_code, p.country,
                   p.city, p.isp, p.anonymity, p.proxy_type]
        .join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });

  document.getElementById('count-lbl').textContent =
    fmt(filtered.length) + ' / ' + fmt(allProxies.length) + ' shown';

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = filtered.slice(0, 2000).map(p => {
    const addr = p.ip + ':' + p.port;
    const ms   = p.response_ms || 0;
    const loc  = p.country_code || '—';
    const dt   = (p.last_checked || '').replace('T',' ').slice(0,16);
    return \`<tr>
      <td><span class="badge \${p.status}">\${p.status}</span></td>
      <td class="mono">\${addr}
        <button class="copy-btn" onclick="copyText('\${addr}')" title="Copy">⧉</button>
      </td>
      <td><code>\${p.proxy_type}</code></td>
      <td><span class="ping \${pingClass(ms)">\${pingText(ms)}</span></td>
      <td>\${anonBadge(p.anonymity)}</td>
      <td>\${loc}</td>
      <td style="color:var(--muted);font-size:12px">\${p.city||'—'}</td>
      <td class="isp-cell" title="\${p.isp||''}">\${p.isp||'—'}</td>
      <td style="white-space:nowrap;font-size:12px;color:var(--muted)">\${dt}</td>
    </tr>\`;
  }).join('');
}

// ── Auto-refresh with countdown ──────────────────────────────────────────────
function updateRefreshInfo() {
  document.getElementById('refresh-info').textContent =
    'Next refresh in ' + countdown + 's  |  ' +
    new Date().toLocaleTimeString();
}

async function refresh() {
  document.getElementById('refresh-info').textContent = 'Refreshing…';
  countdown = REFRESH_INTERVAL;
  await Promise.all([loadStats(), loadProxies()]);
  updateRefreshInfo();
}

// Initial load
refresh();

// Countdown ticker (every second)
setInterval(() => {
  countdown--;
  if (countdown <= 0) {
    refresh();
  } else {
    updateRefreshInfo();
  }
}, 1000);
</script>
</body>
</html>"""

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)
_DB_PATH = proxy_db.DB_PATH


@app.route("/")
def index() -> str:
    return render_template_string(_HTML)


@app.route("/api/stats")
def api_stats():
    stats = proxy_db.get_stats_sync(path=_DB_PATH)
    return jsonify(stats)


@app.route("/api/proxies")
def api_proxies():
    status = request.args.get("status") or None
    ptype  = request.args.get("type")   or None
    limit  = min(int(request.args.get("limit", 5000)), 10000)
    rows   = proxy_db.get_all_proxies_sync(
        status=status, proxy_type=ptype, limit=limit, path=_DB_PATH
    )
    return jsonify(rows)


@app.route("/api/proxies/online.txt")
def api_online_txt():
    rows = proxy_db.get_all_proxies_sync(status="online", limit=10000, path=_DB_PATH)
    lines = "\n".join(f"{r['ip']}:{r['port']}" for r in rows)
    return Response(
        lines,
        mimetype="text/plain",
        headers={"Content-Disposition": "attachment; filename=online_proxies.txt"},
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Proxy IP Checker — web dashboard (Flask)"
    )
    p.add_argument("--host", default="127.0.0.1",
                   help="Bind host (default: 127.0.0.1 — localhost only)")
    p.add_argument("--port", type=int, default=8080,
                   help="Bind port (default: 8080)")
    p.add_argument("--db", metavar="PATH",
                   help="Path to the SQLite database (default: proxy_lists/proxies.db)")
    p.add_argument("--debug", action="store_true",
                   help="Enable Flask debug mode (do NOT use with Tor)")
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()

    if args.db:
        proxy_db.DB_PATH = Path(args.db)
        global _DB_PATH
        _DB_PATH = Path(args.db)

    print(f"[*] Starting Proxy IP Checker web server on {args.host}:{args.port}")
    print(f"[*] Database: {_DB_PATH}")
    if args.host == "127.0.0.1":
        print(f"[*] Dashboard:  http://127.0.0.1:{args.port}/")
        print("[*] To expose via Tor, run:  python tor_service.py")
    app.run(host=args.host, port=args.port, debug=args.debug)
