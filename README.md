# Proxy IP Checker

A Python script that checks proxy lists, categorises each proxy as **online** or **fallen**, and manages the results in neatly organised files.

---

## Features

* **Three input sources**
  * Option **a** – a local text file (one `IP:PORT` per line)
  * Option **b** – a file containing URLs that serve proxy lists
  * Option **c** – the built-in database of 100+ public proxy-list URLs (`fetch` command)
* **One checker URL per proxy** – a rotating pool of ~50 IP-echo services ensures no single service is hammered and rate-limit risk is minimised.
* **Fully async** – all network I/O (URL fetching and proxy checking) runs with `asyncio` + `aiohttp` for maximum throughput (default 200 concurrent checks).
* **Automatic deduplication** – across all three lists (offline / online / fallen).
* Supports **HTTP, HTTPS, SOCKS4, SOCKS5** proxy types.

---

## Directory layout

```
proxy_lists/
  offline_http.txt     offline_https.txt     offline_socks4.txt     offline_socks5.txt
  online_http.txt      online_https.txt      online_socks4.txt      online_socks5.txt
  fallen_http.txt      fallen_https.txt      fallen_socks4.txt      fallen_socks5.txt
```

| File prefix | Content | Format |
|-------------|---------|--------|
| `offline_*` | Unchecked / pending proxies | `IP:PORT` |
| `online_*`  | Verified, working proxies   | `[TYPE]IP:PORT(ResponseTimeMs)` |
| `fallen_*`  | Dead / unreachable proxies  | `IP:PORT` |

The directory and files are created automatically on first run.

---

## Installation

```bash
pip install -r requirements.txt
```

> **SOCKS support** is included via `aiohttp-socks` which provides async SOCKS4/SOCKS5 proxy connectors.

---

## Usage

### Fetch proxies from the built-in source list

Download from 100+ public proxy-list URLs in one command:
```bash
# Fetch all types (http, https, socks4, socks5)
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

# Check only SOCKS4 proxies
python proxy_checker.py check --type socks4

# Custom timeout (15 s) and 100 concurrent workers
python proxy_checker.py check --type http --timeout 15 --workers 100
```

When checking:
1. Each proxy in `offline_{type}.txt` is tested against a unique checker URL.
2. **Working** proxies are appended to `online_{type}.txt` and removed from `offline_{type}.txt`.
3. **Dead** proxies are appended to `fallen_{type}.txt` and removed from `offline_{type}.txt`.

### Show statistics

```bash
python proxy_checker.py stats
```

Example output:
```
=== Proxy List Statistics ===

  Type      Offline    Online    Fallen
  --------------------------------
  HTTP          150        42        63
  HTTPS          80        11        30
  SOCKS4         60         9        21
  SOCKS5         40        17        18
```

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
