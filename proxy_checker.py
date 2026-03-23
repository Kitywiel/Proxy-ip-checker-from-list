#!/usr/bin/env python3
"""
Proxy IP Checker
================
Checks proxies from a local list (option a), from a file of URLs pointing
to proxy lists (option b), or from the built-in URL database (option c).

All network I/O runs fully asynchronously (asyncio + aiohttp) for maximum
throughput.  Each proxy is tested against a unique checker URL drawn from a
rotating pool of ~50 IP-echo services, which spreads the load and avoids
rate-limits.

Directory layout (auto-created):
  proxy_lists/
    offline_{type}.txt  – unchecked proxies  format: IP:PORT
    online_{type}.txt   – working proxies    format: [TYPE]IP:PORT(ResponseTimeMs)
    fallen_{type}.txt   – dead proxies       format: IP:PORT

Supported types: http, https, socks4, socks5

Usage examples:
  python proxy_checker.py add --list my_proxies.txt --type http
  python proxy_checker.py add --urls url_sources.txt --type socks5
  python proxy_checker.py fetch --type all
  python proxy_checker.py check --type all
  python proxy_checker.py stats
"""

import re
import sys
import time
import random
import asyncio
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiohttp_socks import ProxyConnector

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROXY_LISTS_DIR = Path("proxy_lists")
PROXY_TYPES = ["http", "https", "socks4", "socks5"]

# ~50 IP-echo services used to verify each proxy.
# Each proxy is assigned exactly one URL from this pool (round-robin).
CHECKER_URLS: List[str] = [
    "https://api.ipify.org",
    "https://api.my-ip.io/ip",
    "https://checkip.amazonaws.com",
    "https://ipinfo.io/ip",
    "https://icanhazip.com",
    "https://ident.me",
    "https://ipecho.net/plain",
    "https://myexternalip.com/raw",
    "https://wtfismyip.com/text",
    "https://ip.seeip.org",
    "https://ip4.seeip.org",
    "https://ip.42.pl/raw",
    "https://www.trackip.net/ip",
    "https://ipv4.icanhazip.com",
    "https://httpbin.org/ip",
    "https://ifconfig.me/ip",
    "https://ifconfig.co/ip",
    "https://myip.wtf/text",
    "https://ip.tyk.nu",
    "https://l2.io/ip",
    "https://echoip.de",
    "https://api.ip.sb/ip",
    "https://ipv4bot.whatismyipaddress.com",
    "https://bot.whatismyip.com/ip",
    "https://4.ident.me",
    "https://api4.my-ip.io/ip",
    "https://ipv4.seeip.org",
    "https://ipaddr.site",
    "https://jsonip.com",
    "https://ip-api.io/json",
    "https://ipapi.co/ip",
    "https://ipwho.is",
    "https://ip.nf/me.txt",
    "http://ip-api.com/json",
    "http://checkip.amazonaws.com",
    "http://ident.me",
    "http://ipv4.icanhazip.com",
    "https://api.bigdatacloud.net/data/client-ip",
    "https://api.iplocation.net/?cmd=get-ip",
    "https://freeipapi.com/api/json",
    "https://ipinfo.io/json",
    "https://myip.dnsomatic.com",
    "https://ip.rootnet.in",
    "https://ip.ryans.org",
    "https://checkip4.optimizely.com",
    "https://www.cloudflare.com/cdn-cgi/trace",
    "https://api.ipquery.io/",
    "https://ipgeolocation.io/",
    "https://ipapi.is/json",
    "https://ip.guide",
]

# Built-in proxy-list source URLs, organized by proxy type.
# Used by the `fetch` command.
PROXY_SOURCE_URLS: Dict[str, List[str]] = {
    "http": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/http.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTP.txt",
        "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/caliphdev/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
        "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/free.txt",
        "https://raw.githubusercontent.com/Volodichev/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/hanwayTech/free-proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/http.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/im-razvan/proxy_list/main/http.txt",
        "https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list/data.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
        "https://www.proxy-list.download/api/v1/get?type=http",
        "https://raw.githubusercontent.com/RX4096/proxy-list/main/online/http.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
        "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt",
        "https://raw.githubusercontent.com/manuGMG/proxy-365/main/SOCKS5.txt",
        "https://raw.githubusercontent.com/zinon/proxy-lists/master/lists/all-proxies.txt",
        "https://raw.githubusercontent.com/Nocturnusx/Proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/yuceltoluyag/GoodProxy/main/raw.txt",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=http",
    ],
    "https": [
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
        "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/https.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/https_proxies.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/https.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/https.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt",
        "https://www.proxy-list.download/api/v1/get?type=https",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=https",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=https",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=https&timeout=10000&country=all",
    ],
    "socks4": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks4.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS4.txt",
        "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/caliphdev/Proxy-List/master/socks4.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt",
        "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "https://raw.githubusercontent.com/hanwayTech/free-proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks4/socks4.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks4.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/socks4.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt",
        "https://raw.githubusercontent.com/RX4096/proxy-list/main/online/socks4.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000&country=all",
        "https://www.proxy-list.download/api/v1/get?type=socks4",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks4",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks4",
    ],
    "socks5": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks5.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS5.txt",
        "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/caliphdev/Proxy-List/master/socks5.txt",
        "https://raw.githubusercontent.com/Volodichev/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
        "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "https://raw.githubusercontent.com/hanwayTech/free-proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/socks5.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/manuGMG/proxy-365/main/SOCKS5.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt",
        "https://raw.githubusercontent.com/RX4096/proxy-list/main/online/socks5.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all",
        "https://www.proxy-list.download/api/v1/get?type=socks5",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks5",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks5",
    ],
}

# ---------------------------------------------------------------------------
# Async-safe file helpers
# ---------------------------------------------------------------------------

_file_locks: Dict[str, asyncio.Lock] = {}


def _get_lock(path: Path) -> asyncio.Lock:
    """Return (creating lazily) an asyncio.Lock for *path*.

    Safe to call without any outer lock because asyncio is single-threaded:
    no other coroutine can interleave between the dict lookup and the
    assignment (there is no ``await`` in this function).
    """
    key = str(path.resolve())
    if key not in _file_locks:
        _file_locks[key] = asyncio.Lock()
    return _file_locks[key]


def read_proxies(path: Path) -> List[str]:
    """Return deduplicated, non-empty lines from *path* (preserves order)."""
    if not path.exists():
        return []
    seen: set = set()
    result: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and line not in seen:
            seen.add(line)
            result.append(line)
    return result


def write_proxies(path: Path, proxies: List[str]) -> None:
    """Write *proxies* to *path*, deduplicating while preserving order."""
    seen: set = set()
    unique: List[str] = []
    for p in proxies:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    path.write_text("\n".join(unique) + ("\n" if unique else ""), encoding="utf-8")


async def append_proxy(path: Path, entry: str) -> None:
    """Async-safely append *entry* to *path* if not already present."""
    lock = _get_lock(path)
    async with lock:
        existing = await asyncio.to_thread(read_proxies, path)
        if entry not in existing:
            existing.append(entry)
            await asyncio.to_thread(write_proxies, path, existing)


async def remove_proxy(path: Path, entry: str) -> None:
    """Async-safely remove *entry* from *path*."""
    lock = _get_lock(path)
    async with lock:
        existing = await asyncio.to_thread(read_proxies, path)
        updated = [p for p in existing if p != entry]
        await asyncio.to_thread(write_proxies, path, updated)


# ---------------------------------------------------------------------------
# Directory & file setup
# ---------------------------------------------------------------------------

def setup_directories() -> None:
    """Create proxy_lists/ and all required files if they do not exist."""
    PROXY_LISTS_DIR.mkdir(exist_ok=True)
    for prefix in ("offline", "online", "fallen"):
        for ptype in PROXY_TYPES:
            filepath = PROXY_LISTS_DIR / f"{prefix}_{ptype}.txt"
            if not filepath.exists():
                filepath.touch()


# ---------------------------------------------------------------------------
# Proxy parsing helpers
# ---------------------------------------------------------------------------

_TYPE_PREFIX_RE = re.compile(r"^\[.*?\]")
_RESPONSE_TIME_RE = re.compile(r"\(.*?\)$")


def strip_decorations(entry: str) -> str:
    """Remove [TYPE] prefix and (ResponseTime) suffix from an entry."""
    entry = _TYPE_PREFIX_RE.sub("", entry).strip()
    entry = _RESPONSE_TIME_RE.sub("", entry).strip()
    return entry


def parse_proxy(entry: str) -> Optional[Tuple[str, int]]:
    """
    Parse a proxy string into (ip, port).
    Accepts formats: IP:PORT, [TYPE]IP:PORT, [TYPE]IP:PORT(123ms)
    Returns None if the format is invalid.
    """
    bare = strip_decorations(entry)
    parts = bare.split(":")
    if len(parts) != 2:
        return None
    ip = parts[0].strip()
    try:
        port = int(parts[1].strip())
    except ValueError:
        return None
    if not ip or not (1 <= port <= 65535):
        return None
    return ip, port


# ---------------------------------------------------------------------------
# Async proxy testing
# ---------------------------------------------------------------------------

async def check_proxy(
    ip: str,
    port: int,
    proxy_type: str,
    checker_url: str,
    timeout: int = 10,
) -> Tuple[bool, float]:
    """
    Asynchronously attempt to reach *checker_url* through the proxy at ip:port.
    Returns (success, response_time_ms).
    """
    proxy_url = f"{proxy_type}://{ip}:{port}"
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    start = time.monotonic()
    try:
        if proxy_type in ("socks4", "socks5"):
            connector = ProxyConnector.from_url(proxy_url, ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    checker_url,
                    timeout=client_timeout,
                    ssl=False,
                    allow_redirects=True,
                ) as resp:
                    elapsed_ms = (time.monotonic() - start) * 1000.0
                    if 200 <= resp.status < 300:
                        return True, round(elapsed_ms, 2)
        else:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    checker_url,
                    proxy=proxy_url,
                    timeout=client_timeout,
                    ssl=False,
                    allow_redirects=True,
                ) as resp:
                    elapsed_ms = (time.monotonic() - start) * 1000.0
                    if 200 <= resp.status < 300:
                        return True, round(elapsed_ms, 2)
    except Exception:
        pass
    return False, 0.0


async def _check_worker(
    raw: str,
    proxy_type: str,
    checker_url: str,
    timeout: int,
    semaphore: asyncio.Semaphore,
    offline: Path,
    online: Path,
    fallen: Path,
    counters: Dict[str, int],
) -> None:
    """Async worker: test one proxy and update the three list files."""
    async with semaphore:
        parsed = parse_proxy(raw)
        if parsed is None:
            await remove_proxy(offline, raw)
            return

        ip, port = parsed
        bare = f"{ip}:{port}"
        success, ms = await check_proxy(ip, port, proxy_type, checker_url, timeout)

        await remove_proxy(offline, raw)

        if success:
            entry = f"[{proxy_type.upper()}]{ip}:{port}({ms}ms)"
            await append_proxy(online, entry)
            print(f"[+] ONLINE  {ip}:{port}  {ms:.0f} ms  ({proxy_type.upper()})")
            counters["online"] += 1
        else:
            await append_proxy(fallen, bare)
            print(f"[-] FALLEN  {ip}:{port}")
            counters["fallen"] += 1

        counters["checked"] += 1


# ---------------------------------------------------------------------------
# Async URL fetching
# ---------------------------------------------------------------------------

async def _fetch_url(
    session: aiohttp.ClientSession,
    url: str,
) -> List[str]:
    """Download a proxy list from *url* and return non-empty lines."""
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=15),
            ssl=False,
        ) as resp:
            resp.raise_for_status()
            text = await resp.text(errors="replace")
            return [
                ln.strip()
                for ln in text.splitlines()
                if ln.strip() and not ln.strip().startswith("#")
            ]
    except Exception as exc:
        print(f"[!] Failed to fetch {url}: {exc}")
        return []


# ---------------------------------------------------------------------------
# High-level async operations
# ---------------------------------------------------------------------------

async def check_proxies(
    proxy_type: str,
    timeout: int = 10,
    max_workers: int = 200,
) -> None:
    """
    Read all proxies from offline_{proxy_type}.txt, test each one
    asynchronously, then:
      - working → online_{proxy_type}.txt  as [TYPE]IP:PORT(Xms)
      - dead    → fallen_{proxy_type}.txt  as IP:PORT
    Tested proxies are removed from the offline list.
    """
    offline = PROXY_LISTS_DIR / f"offline_{proxy_type}.txt"
    online = PROXY_LISTS_DIR / f"online_{proxy_type}.txt"
    fallen = PROXY_LISTS_DIR / f"fallen_{proxy_type}.txt"

    proxies = read_proxies(offline)
    if not proxies:
        print(f"[i] No unchecked proxies in {offline}")
        return

    print(f"[*] Checking {len(proxies)} {proxy_type.upper()} proxies …")

    # Assign each proxy a unique checker URL (round-robin over a shuffled pool)
    pool = CHECKER_URLS.copy()
    random.shuffle(pool)

    semaphore = asyncio.Semaphore(max_workers)
    counters: Dict[str, int] = {"checked": 0, "online": 0, "fallen": 0}

    tasks = [
        _check_worker(
            raw=raw,
            proxy_type=proxy_type,
            checker_url=pool[i % len(pool)],
            timeout=timeout,
            semaphore=semaphore,
            offline=offline,
            online=online,
            fallen=fallen,
            counters=counters,
        )
        for i, raw in enumerate(proxies)
    ]

    await asyncio.gather(*tasks)

    print(
        f"\n[*] {proxy_type.upper()} — checked {counters['checked']}: "
        f"{counters['online']} online, {counters['fallen']} fallen.\n"
    )


def add_from_list(source_file: str, proxy_type: str) -> None:
    """
    Import proxies from a local file into offline_{proxy_type}.txt.
    Duplicates (across all three lists) are silently ignored.
    """
    src = Path(source_file)
    if not src.exists():
        sys.exit(f"[!] File not found: {source_file}")

    raw_lines = read_proxies(src)
    offline = PROXY_LISTS_DIR / f"offline_{proxy_type}.txt"
    online = PROXY_LISTS_DIR / f"online_{proxy_type}.txt"
    fallen = PROXY_LISTS_DIR / f"fallen_{proxy_type}.txt"

    existing_offline = set(read_proxies(offline))
    already_online = {strip_decorations(e) for e in read_proxies(online)}
    already_fallen = set(read_proxies(fallen))

    added = 0
    for line in raw_lines:
        parsed = parse_proxy(line)
        if parsed is None:
            print(f"[!] Skipping invalid entry: {line}")
            continue
        ip, port = parsed
        bare = f"{ip}:{port}"
        if bare in existing_offline or bare in already_online or bare in already_fallen:
            continue
        existing_offline.add(bare)
        added += 1

    write_proxies(offline, sorted(existing_offline))
    print(f"[*] Added {added} new {proxy_type.upper()} proxies to {offline}")


async def add_from_urls(urls_file: str, proxy_type: str) -> None:
    """
    Download proxy lists from each URL in *urls_file* concurrently and import
    them into offline_{proxy_type}.txt, ignoring duplicates.
    """
    uf = Path(urls_file)
    if not uf.exists():
        sys.exit(f"[!] URLs file not found: {urls_file}")

    urls = [
        ln.strip()
        for ln in uf.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
    if not urls:
        print("[!] No URLs found in the file.")
        return

    await _import_from_url_list(urls, proxy_type)


async def fetch_builtin(proxy_type: str) -> None:
    """
    Download proxy lists from the built-in PROXY_SOURCE_URLS for *proxy_type*
    concurrently and import them into offline_{proxy_type}.txt.
    """
    types = PROXY_TYPES if proxy_type == "all" else [proxy_type]
    for ptype in types:
        urls = PROXY_SOURCE_URLS.get(ptype, [])
        if not urls:
            print(f"[!] No built-in sources for type: {ptype}")
            continue
        print(f"\n[*] Fetching {len(urls)} built-in sources for {ptype.upper()} …")
        await _import_from_url_list(urls, ptype)


async def _import_from_url_list(urls: List[str], proxy_type: str) -> None:
    """
    Fetch all *urls* concurrently, parse proxy entries, and append new ones
    to offline_{proxy_type}.txt (deduplicating against all three lists).
    """
    offline = PROXY_LISTS_DIR / f"offline_{proxy_type}.txt"
    online = PROXY_LISTS_DIR / f"online_{proxy_type}.txt"
    fallen = PROXY_LISTS_DIR / f"fallen_{proxy_type}.txt"

    existing_offline = set(read_proxies(offline))
    already_online = {strip_decorations(e) for e in read_proxies(online)}
    already_fallen = set(read_proxies(fallen))

    connector = aiohttp.TCPConnector(ssl=False, limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await asyncio.gather(
            *[_fetch_url(session, url) for url in urls],
            return_exceptions=True,
        )

    total_added = 0
    for url, lines in zip(urls, results):
        if isinstance(lines, Exception):
            print(f"[!] Error fetching {url}: {lines}")
            continue
        added = 0
        for line in lines:
            parsed = parse_proxy(line)
            if parsed is None:
                continue
            ip, port = parsed
            bare = f"{ip}:{port}"
            if bare in existing_offline or bare in already_online or bare in already_fallen:
                continue
            existing_offline.add(bare)
            added += 1
        if added:
            print(f"    {url}  → {added} new")
        total_added += added

    write_proxies(offline, sorted(existing_offline))
    print(f"\n[*] Total added: {total_added} new {proxy_type.upper()} proxies to {offline}\n")


def show_stats() -> None:
    """Print a summary table of all proxy list sizes."""
    print("\n=== Proxy List Statistics ===\n")
    header = f"  {'Type':<8}  {'Offline':>8}  {'Online':>8}  {'Fallen':>8}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for ptype in PROXY_TYPES:
        counts = {}
        for prefix in ("offline", "online", "fallen"):
            path = PROXY_LISTS_DIR / f"{prefix}_{ptype}.txt"
            counts[prefix] = len(read_proxies(path))
        print(
            f"  {ptype.upper():<8}  {counts['offline']:>8}  "
            f"{counts['online']:>8}  {counts['fallen']:>8}"
        )
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="proxy_checker",
        description="Proxy IP checker — add, fetch, check and manage proxy lists.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # -- add --
    add_p = sub.add_parser("add", help="Add proxies to an offline list")
    src = add_p.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--list", "-l",
        metavar="FILE",
        help="Path to a local proxy list (one IP:PORT per line)",
    )
    src.add_argument(
        "--urls", "-u",
        metavar="FILE",
        help="Path to a file containing URLs that serve proxy lists",
    )
    add_p.add_argument(
        "--type", "-t",
        choices=PROXY_TYPES,
        required=True,
        metavar="TYPE",
        help="Proxy type: http | https | socks4 | socks5",
    )

    # -- fetch --
    fetch_p = sub.add_parser(
        "fetch",
        help="Download proxies from the built-in source URL list",
    )
    fetch_p.add_argument(
        "--type", "-t",
        choices=PROXY_TYPES + ["all"],
        default="all",
        metavar="TYPE",
        help="Which type(s) to fetch (default: all)",
    )

    # -- check --
    chk_p = sub.add_parser("check", help="Test proxies from the offline lists")
    chk_p.add_argument(
        "--type", "-t",
        choices=PROXY_TYPES + ["all"],
        default="all",
        metavar="TYPE",
        help="Which type to check (default: all)",
    )
    chk_p.add_argument(
        "--timeout", "-T",
        type=int,
        default=10,
        metavar="SECS",
        help="Per-proxy timeout in seconds (default: 10)",
    )
    chk_p.add_argument(
        "--workers", "-w",
        type=int,
        default=200,
        metavar="N",
        help="Max concurrent async checks (default: 200)",
    )

    # -- stats --
    sub.add_parser("stats", help="Display proxy list counts")

    return parser


async def async_main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_directories()

    if args.command == "add":
        if args.list:
            add_from_list(args.list, args.type)
        else:
            await add_from_urls(args.urls, args.type)

    elif args.command == "fetch":
        await fetch_builtin(args.type)

    elif args.command == "check":
        types = PROXY_TYPES if args.type == "all" else [args.type]
        for ptype in types:
            await check_proxies(ptype, timeout=args.timeout, max_workers=args.workers)

    elif args.command == "stats":
        show_stats()

    else:
        parser.print_help()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
