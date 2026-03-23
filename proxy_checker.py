#!/usr/bin/env python3
"""
Proxy IP Checker
================
Checks proxies from a local list (option a) or from a file of URLs pointing
to proxy lists (option b).

Each proxy is tested against a unique checker URL drawn from a rotating pool
of ~50 IP-echo services, which spreads the load and avoids rate-limits.

Directory layout (auto-created):
  proxy_lists/
    offline_{type}.txt  – unchecked proxies  format: IP:PORT
    online_{type}.txt   – working proxies    format: [TYPE]IP:PORT(ResponseTimeMs)
    fallen_{type}.txt   – dead proxies       format: IP:PORT

Supported types: http, https, socks4, socks5

Usage examples:
  python proxy_checker.py add --list my_proxies.txt --type http
  python proxy_checker.py add --urls url_sources.txt --type socks5
  python proxy_checker.py check --type all
  python proxy_checker.py stats
"""

import re
import sys
import time
import random
import argparse
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import urllib3
import requests

# Suppress SSL warnings for proxy testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROXY_LISTS_DIR = Path("proxy_lists")
PROXY_TYPES = ["http", "https", "socks4", "socks5"]

# ~50 IP-echo services used to test each proxy.
# Each proxy is assigned exactly one URL from this list (round-robin).
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

# ---------------------------------------------------------------------------
# Thread-safe file helpers
# ---------------------------------------------------------------------------

_file_locks: Dict[str, threading.Lock] = {}
_locks_lock = threading.Lock()


def _get_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _locks_lock:
        if key not in _file_locks:
            _file_locks[key] = threading.Lock()
        return _file_locks[key]


def read_proxies(path: Path) -> List[str]:
    """Return deduplicated, non-empty lines from *path* (preserves order)."""
    if not path.exists():
        return []
    seen = set()
    result = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and line not in seen:
            seen.add(line)
            result.append(line)
    return result


def write_proxies(path: Path, proxies: List[str]) -> None:
    """Write *proxies* to *path*, deduplicating while preserving order."""
    seen = set()
    unique: List[str] = []
    for p in proxies:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    path.write_text("\n".join(unique) + ("\n" if unique else ""), encoding="utf-8")


def append_proxy(path: Path, entry: str) -> None:
    """Thread-safely append *entry* to *path* if it is not already present."""
    lock = _get_lock(path)
    with lock:
        existing = read_proxies(path)
        if entry not in existing:
            existing.append(entry)
            write_proxies(path, existing)


def remove_proxy(path: Path, entry: str) -> None:
    """Thread-safely remove *entry* from *path*."""
    lock = _get_lock(path)
    with lock:
        existing = read_proxies(path)
        updated = [p for p in existing if p != entry]
        write_proxies(path, updated)


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
# Proxy testing
# ---------------------------------------------------------------------------

def check_proxy(
    ip: str,
    port: int,
    proxy_type: str,
    checker_url: str,
    timeout: int = 10,
) -> Tuple[bool, float]:
    """
    Attempt to reach *checker_url* through the proxy at ip:port.
    Returns (success, response_time_ms).
    """
    scheme = proxy_type if proxy_type in ("http", "https") else proxy_type
    proxy_url = f"{scheme}://{ip}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}

    start = time.monotonic()
    try:
        resp = requests.get(
            checker_url,
            proxies=proxies,
            timeout=timeout,
            verify=False,
            allow_redirects=True,
        )
        elapsed_ms = (time.monotonic() - start) * 1000.0
        if 200 <= resp.status_code < 300:
            return True, round(elapsed_ms, 2)
    except Exception:
        pass
    return False, 0.0


def _worker(
    raw: str,
    proxy_type: str,
    checker_url: str,
    timeout: int,
) -> Tuple[str, bool, float]:
    """Worker function run in a thread pool."""
    parsed = parse_proxy(raw)
    if parsed is None:
        return raw, False, 0.0
    ip, port = parsed
    success, ms = check_proxy(ip, port, proxy_type, checker_url, timeout)
    return raw, success, ms


# ---------------------------------------------------------------------------
# High-level operations
# ---------------------------------------------------------------------------

def check_proxies(proxy_type: str, timeout: int = 10, max_workers: int = 50) -> None:
    """
    Read all proxies from offline_{proxy_type}.txt, test each one, then:
      - working  → online_{proxy_type}.txt  as [TYPE]IP:PORT(Xms)
      - dead     → fallen_{proxy_type}.txt  as IP:PORT
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
    tasks = [
        (raw, proxy_type, pool[i % len(pool)], timeout)
        for i, raw in enumerate(proxies)
    ]

    checked = online_count = fallen_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_worker, *t): t[0] for t in tasks}
        for future in as_completed(futures):
            raw = futures[future]
            try:
                raw_result, success, ms = future.result()
            except Exception as exc:
                print(f"[!] Unexpected error for {raw}: {exc}")
                continue

            parsed = parse_proxy(raw_result)
            if parsed is None:
                remove_proxy(offline, raw_result)
                continue

            ip, port = parsed
            bare = f"{ip}:{port}"

            remove_proxy(offline, raw_result)

            if success:
                entry = f"[{proxy_type.upper()}]{ip}:{port}({ms}ms)"
                append_proxy(online, entry)
                print(f"[+] ONLINE  {ip}:{port}  {ms:.0f} ms  ({proxy_type.upper()})")
                online_count += 1
            else:
                append_proxy(fallen, bare)
                print(f"[-] FALLEN  {ip}:{port}")
                fallen_count += 1

            checked += 1

    print(
        f"\n[*] {proxy_type.upper()} — checked {checked}: "
        f"{online_count} online, {fallen_count} fallen.\n"
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


def _fetch_url(url: str) -> List[str]:
    """Download a proxy list from *url* and return non-empty lines."""
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return [ln.strip() for ln in resp.text.splitlines() if ln.strip() and not ln.startswith("#")]
    except Exception as exc:
        print(f"[!] Failed to fetch {url}: {exc}")
        return []


def add_from_urls(urls_file: str, proxy_type: str) -> None:
    """
    Download proxy lists from each URL in *urls_file* and import them into
    offline_{proxy_type}.txt, ignoring duplicates.
    """
    uf = Path(urls_file)
    if not uf.exists():
        sys.exit(f"[!] URLs file not found: {urls_file}")

    urls = [ln.strip() for ln in uf.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.startswith("#")]
    if not urls:
        print("[!] No URLs found in the file.")
        return

    offline = PROXY_LISTS_DIR / f"offline_{proxy_type}.txt"
    online = PROXY_LISTS_DIR / f"online_{proxy_type}.txt"
    fallen = PROXY_LISTS_DIR / f"fallen_{proxy_type}.txt"

    existing_offline = set(read_proxies(offline))
    already_online = {strip_decorations(e) for e in read_proxies(online)}
    already_fallen = set(read_proxies(fallen))

    total_added = 0
    for url in urls:
        print(f"[*] Fetching: {url}")
        lines = _fetch_url(url)
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
        print(f"    → {added} new proxies")
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
        description="Proxy IP checker — add, check and manage proxy lists.",
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
        default=50,
        metavar="N",
        help="Concurrent worker threads (default: 50)",
    )

    # -- stats --
    sub.add_parser("stats", help="Display proxy list counts")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_directories()

    if args.command == "add":
        if args.list:
            add_from_list(args.list, args.type)
        else:
            add_from_urls(args.urls, args.type)

    elif args.command == "check":
        types = PROXY_TYPES if args.type == "all" else [args.type]
        for ptype in types:
            check_proxies(ptype, timeout=args.timeout, max_workers=args.workers)

    elif args.command == "stats":
        show_stats()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
