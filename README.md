# Proxy IP Checker

A **zero-setup** async Python tool that mines, checks, and manages proxy lists — categorising each proxy as **online** or **fallen** across all six major proxy types.

---

## Quick start — no installation needed

```bash
python proxy_checker.py
```

That's it. Missing packages (`aiohttp`, `aiohttp-socks`, `aiosqlite`) are detected and installed automatically with `python -m pip` before the tool starts. No manual `pip install` required.

---

## Features

* **Zero-setup** – missing dependencies (`aiohttp`, `aiohttp-socks`, `aiosqlite`) are auto-installed on first run using `python -m pip`.
* **CDN / false-positive filter** – the proxy check validates that the IP-echo service response actually contains an IPv4 address. CDN block pages, captcha challenges, and Cloudflare landing pages that intercept the connection return HTML without a bare IP and are rejected even when the HTTP status is 200.
* **Four input sources**
  * A local text file (one `IP:PORT` per line) via `add --list`
  * A file of URLs (plain raw URLs *or* `https://github.com/{owner}/{repo}` lines auto-expanded via the GitHub API) via `add --urls`
  * The built-in database of 170+ public proxy-list URLs (`fetch` command)
  * 50 GitHub repositories auto-discovered via the GitHub API (`repos` command)
* **SNTBS queue** – drop `SNTBS-{type}.txt` files (e.g. `SNTBS-socks5.txt`) next to the script to feed proxies into the pending queue; unchecked IPs are automatically saved back to the SNTBS file if a run is interrupted.
* **One checker URL per proxy** – a rotating pool of ~50 IP-echo services prevents rate-limiting.
* **Fully async** – all network I/O runs with `asyncio` + `aiohttp` (default 200 concurrent workers).
* **General info per proxy** – country, city, ISP (via [ipwho.is](https://ipwho.is)), anonymity level (Elite / Anonymous), and response time in ms.
* **SQLite database** – results are also stored in `proxy_lists/proxies.db` for the web dashboard (requires `aiosqlite`).
* **Web dashboard** – `web_server.py` serves a live dark-theme dashboard on `http://127.0.0.1:8080`; also auto-installs Flask if missing.
* **Standalone HTML dashboard** – open `index.html` directly in any browser (no server needed to view the UI — it connects to the running `web_server.py`).
* **Automatic deduplication** – across all three lists (offline / online / fallen).
* **Every major proxy type** – HTTP, HTTPS, SOCKS4, SOCKS4a, SOCKS5, SOCKS5h.

---

## Directory layout

```
proxy_lists/
  offline_http.txt      offline_https.txt      offline_socks4.txt
  offline_socks4a.txt   offline_socks5.txt     offline_socks5h.txt
  online_http.txt       online_https.txt       online_socks4.txt
  online_socks4a.txt    online_socks5.txt      online_socks5h.txt
  fallen_http.txt       fallen_https.txt       fallen_socks4.txt
  fallen_socks4a.txt    fallen_socks5.txt      fallen_socks5h.txt
  proxies.db            ← SQLite database (auto-created)

SNTBS-http.txt          ← optional: drop IPs here to queue them for checking
SNTBS-socks5.txt        ← one file per proxy type you want to feed
…
index.html              ← standalone browser dashboard
```

| File prefix | Content | Format |
|-------------|---------|--------|
| `offline_*` | Unchecked / pending proxies | `IP:PORT` |
| `online_*`  | Verified, working proxies   | `[TYPE]IP:PORT(Xms)[CC][Anon][ISP]` |
| `fallen_*`  | Dead / unreachable proxies  | `IP:PORT` |
| `SNTBS-*`   | Still-Need-To-Be-Scanned queue | `IP:PORT` |

Example online entry: `[SOCKS5]1.2.3.4:1080(312ms)[DE][Elite][Hetzner Online GmbH]`

All directories and files are created automatically on first run.

---

## SNTBS files — persistent pending queue

**SNTBS** (Still Need To Be Scanned) files let you feed proxies into the checker without touching the `proxy_lists/` directory manually.

**Adding proxies:**

Create a file named `SNTBS-{proxytype}.txt` (e.g. `SNTBS-socks5.txt`) in the same directory as `proxy_checker.py`, one `IP:PORT` per line:

```
1.2.3.4:1080
5.6.7.8:9050
```

On the next run, `proxy_checker.py` automatically imports the IPs into `offline_{type}.txt` and clears the SNTBS file.

**Interrupted runs:**

If a run is stopped (Ctrl+C or error) before all proxies are checked, any unchecked IPs are automatically written back to `SNTBS-{type}.txt` so they are not lost and will be picked up on the next run:

```
[~] Saved 4823 unchecked SOCKS5 proxies to SNTBS-socks5.txt
```

---

## Usage

### Run the full pipeline (default)

```bash
python proxy_checker.py
```

Runs all steps in sequence: fetch from 170+ sources → discover GitHub repos → check all offline proxies → print statistics.

```bash
# Repeat every hour forever
python proxy_checker.py run --loop

# Custom interval
python proxy_checker.py run --loop --interval 30m

# Skip the GitHub repo step
python proxy_checker.py run --skip-repos
```

### Fetch from built-in sources

```bash
python proxy_checker.py fetch           # all types
python proxy_checker.py fetch --type socks5
```

### Auto-discover proxies from GitHub repos

```bash
python proxy_checker.py repos           # all 50 repos, all types
python proxy_checker.py repos --type socks5
python proxy_checker.py repos --list-repos   # print configured repos
GITHUB_TOKEN=ghp_xxx python proxy_checker.py repos   # authenticated (5 000 req/h)
```

### Add proxies manually

**From a local file** (one `IP:PORT` per line):
```bash
python proxy_checker.py add --list my_proxies.txt --type http
```

**From a URLs file** (one URL or `https://github.com/{owner}/{repo}` per line):
```bash
python proxy_checker.py add --urls proxy_sources.txt --type socks5
```

### Check proxies

```bash
python proxy_checker.py check                          # all types
python proxy_checker.py check --type socks4a           # one type
python proxy_checker.py check --type http --timeout 15 --workers 100
```

When checking:
1. Each proxy in `offline_{type}.txt` is tested against a unique checker URL.
2. **Working** proxies → enriched with geo + anonymity info → `online_{type}.txt`.
3. **Dead** proxies → `fallen_{type}.txt`.
4. `offline_{type}.txt` is bulk-cleared after all batches complete.
5. Any proxies not reached (interrupted run) are saved to `SNTBS-{type}.txt`.

Example console output:
```
[+] ONLINE  1.2.3.4:1080  312 ms  (SOCKS5)  [Elite]  DE, Frankfurt  Hetzner Online GmbH
[-] FALLEN  9.8.7.6:3128
[~] Saved 4823 unchecked SOCKS5 proxies to SNTBS-socks5.txt
```

### Show statistics

```bash
python proxy_checker.py stats
```

```
=== Proxy List Statistics ===

  Type       Offline    Online    Fallen
  ----------------------------------------
  HTTP           150        42        63
  HTTPS           80        11        30
  SOCKS4          60         9        21
  SOCKS4A         55         7        18
  SOCKS5          40        17        18
  SOCKS5H         38        14        15
```

---

## Web dashboard

### Live server dashboard

```bash
python web_server.py                  # http://127.0.0.1:8080
python web_server.py --port 9090
python web_server.py --host 0.0.0.0  # expose on all interfaces
```

Flask is auto-installed if missing. The dashboard shows live stats, per-type breakdowns, a searchable proxy table with filtering, and a download link for `online_proxies.txt`.

### Standalone HTML dashboard (`index.html`)

Open `index.html` directly in any browser — no web server needed to *view* the UI. It connects to the running `web_server.py` (default `http://127.0.0.1:8080`) and auto-refreshes every 30 seconds.

A connection banner at the top shows whether the server is reachable. You can change the server address in the input field and click **Connect** if you run the server on a different port or host.

```
⚡ Connected to http://127.0.0.1:8080
```

### API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | HTML dashboard |
| `GET /api/stats` | JSON statistics summary |
| `GET /api/proxies` | JSON proxy list (`?status=online&type=socks5&limit=1000`) |
| `GET /api/proxies/online.txt` | Plain-text `IP:PORT` download |

---

## Supported proxy types

| Type | Description |
|------|-------------|
| `http` | Standard HTTP proxy |
| `https` | HTTPS (SSL) proxy |
| `socks4` | SOCKS4 – IPv4 addresses only |
| `socks4a` | SOCKS4a – SOCKS4 with remote hostname resolution |
| `socks5` | SOCKS5 – IPv4, IPv6, and hostnames |
| `socks5h` | SOCKS5h – SOCKS5 with remote DNS resolution |

---

## Optional: Tor hidden service

```bash
python tor_service.py
```

Zero setup — `stem` is auto-installed if missing. Exposes the web dashboard as a Tor `.onion` address.

**Tor itself must be installed separately:**

| Platform | Command |
|----------|---------|
| Debian / Ubuntu | `sudo apt-get install tor` |
| macOS | `brew install tor` |
| Windows | Download the [Expert Bundle](https://www.torproject.org/download/tor/) and either add the `Tor\` folder to PATH or place it next to `tor_service.py` |

The script automatically:
1. Tries to connect to any already-running Tor instance (Tor Browser on port 9151, system Tor on port 9051) — no new process is launched if one is found.
2. Searches common Windows installation paths (`Tor Browser`, Expert Bundle folders) so `tor.exe` does not need to be in PATH.
3. Launches a new Tor process only when no running instance is found.
4. Wraps the launch in a clear error handler with actionable troubleshooting hints.

---

## Notes

* `ssl=False` is used intentionally when connecting through proxies to avoid certificate errors — this is expected behaviour for proxy testing.
* Proxies already present in `online_*` or `fallen_*` are never re-added to `offline_*`.
* The checker URL pool is shuffled before each run so URL assignment varies between sessions.
* Geo and anonymity lookups are rate-limited to 15 concurrent requests to respect free-tier limits of ipwho.is and httpbin.org.
* Set `GITHUB_TOKEN` for the higher 5 000 req/hour GitHub API rate limit (default: 60 req/hour unauthenticated).
* The SQLite database (`proxy_lists/proxies.db`) uses a single shared connection with WAL journal mode and an asyncio write-lock so concurrent checker workers never produce "database is locked" errors.
