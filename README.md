# Proxy IP Checker

A Python script that checks proxy lists, categorises each proxy as **online** or **fallen**, and manages the results in neatly organised files.

---

## Features

* **Three input sources**
  * Option **a** – a local text file (one `IP:PORT` per line)
  * Option **b** – a file containing URLs that serve proxy lists
  * Option **c** – the built-in database of 170+ public proxy-list URLs (`fetch` command), covering GitHub repos, proxyscrape v2/v3, proxyscan.io, openproxy.space, spys.me, geonode, and more
* **One checker URL per proxy** – a rotating pool of ~50 IP-echo services ensures no single service is hammered and rate-limit risk is minimised.
* **Fully async** – all network I/O (URL fetching and proxy checking) runs with `asyncio` + `aiohttp` for maximum throughput (default 200 concurrent checks).
* **General info per proxy** – when a proxy is confirmed online the tool automatically gathers:
  * 🌍 Country, city, and ISP (via [ipwho.is](https://ipwho.is))
  * 🔒 Anonymity level: **Elite** (no proxy headers leaked) or **Anonymous** (httpbin.org header inspection)
  * ⏱ Response time in milliseconds
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
```

| File prefix | Content | Format |
|-------------|---------|--------|
| `offline_*` | Unchecked / pending proxies | `IP:PORT` |
| `online_*`  | Verified, working proxies   | `[TYPE]IP:PORT(Xms)[CC][Anon][ISP]` |
| `fallen_*`  | Dead / unreachable proxies  | `IP:PORT` |

Example online entry: `[SOCKS5]1.2.3.4:1080(312ms)[DE][Elite][Hetzner Online GmbH]`

The directory and files are created automatically on first run.

---

## Installation

```bash
pip install -r requirements.txt
```

> **SOCKS support** is included via `aiohttp-socks` which provides async SOCKS4/SOCKS4a/SOCKS5/SOCKS5h proxy connectors.

---

## Usage

### Fetch proxies from the built-in source list

Download from 170+ public proxy-list sources (GitHub repos + web APIs) in one command:
```bash
# Fetch all types (http, https, socks4, socks4a, socks5, socks5h)
python proxy_checker.py fetch

# Fetch only SOCKS5 proxies
python proxy_checker.py fetch --type socks5
```

### Add proxies to an offline list

**From a local file** (one `IP:PORT` per line):
```bash
python proxy_checker.py add --list my_proxies.txt --type http
python proxy_checker.py add --list socks_proxies.txt --type socks5
```

**From a URLs file** (one URL per line, each URL returns a proxy list):
```bash
python proxy_checker.py add --urls proxy_sources.txt --type https
```

### Check proxies

```bash
# Check all types
python proxy_checker.py check

# Check only SOCKS4a proxies
python proxy_checker.py check --type socks4a

# Custom timeout (15 s) and 100 concurrent workers
python proxy_checker.py check --type http --timeout 15 --workers 100
```

When checking:
1. Each proxy in `offline_{type}.txt` is tested against a unique checker URL.
2. **Working** proxies are enriched with country, city, ISP, and anonymity level, appended to `online_{type}.txt`, and removed from `offline_{type}.txt`.
3. **Dead** proxies are appended to `fallen_{type}.txt` and removed from `offline_{type}.txt`.

Example console output for a working proxy:
```
[+] ONLINE  1.2.3.4:1080  312 ms  (SOCKS5)  [Elite]  DE, Frankfurt  Hetzner Online GmbH
```

### Show statistics

```bash
python proxy_checker.py stats
```

Example output:
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

## Example: URLs source file

`proxy_sources.txt`
```
https://raw.githubusercontent.com/example/proxy-list/main/http.txt
https://somesite.com/proxies/socks5.txt
```

---

## Notes

* `ssl=False` is used intentionally when connecting through proxies to avoid certificate errors; this is expected behaviour for proxy testing.
* Proxies already present in `online_*` or `fallen_*` are never re-added to `offline_*`.
* The checker URL pool is shuffled before each run so the assignment varies between sessions.
* Geo and anonymity lookups are rate-limited to 15 concurrent requests to respect free-tier limits of ipwho.is and httpbin.org.
