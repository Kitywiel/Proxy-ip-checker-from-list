#!/usr/bin/env python3
"""
Proxy IP Checker
================
Zero-setup automatic proxy miner and checker.

Run with no arguments to start the full auto-mine pipeline:

  python proxy_checker.py          ← fetch all sources + check all proxies
  python proxy_checker.py run --loop --interval 1h   ← mine forever

All network I/O runs fully asynchronously (asyncio + aiohttp) for maximum
throughput.  Each proxy is tested against a unique checker URL drawn from a
rotating pool of ~50 IP-echo services, which spreads the load and avoids
rate-limits.

When a proxy is confirmed online the tool also gathers general info:
  • Country, city, and ISP via ipwho.is
  • Anonymity level (Elite / Anonymous) via httpbin.org/get header inspection

Directory layout (auto-created):
  proxy_lists/
    offline_{type}.txt  – unchecked proxies  format: IP:PORT
    online_{type}.txt   – working proxies    format: [TYPE]IP:PORT(Xms)[CC][Anon][ISP]
    fallen_{type}.txt   – dead proxies       format: IP:PORT

Supported types: http, https, socks4, socks4a, socks5, socks5h

GitHub auto-discovery:
  The `repos` command (and the `run` pipeline) queries the GitHub API for
  every repo in GITHUB_REPO_SOURCES, discovers all proxy-list .txt files,
  classifies them by type, and imports the raw content automatically.  Set
  the GITHUB_TOKEN environment variable for the higher 5 000 req/hour
  authenticated rate limit (default: 60 req/hour unauthenticated).

All commands:
  python proxy_checker.py                      # run (default)
  python proxy_checker.py run                 # fetch all + check all (once)
  python proxy_checker.py run --loop          # repeat every hour forever
  python proxy_checker.py run --interval 30m  # repeat every 30 minutes
  python proxy_checker.py run --skip-repos    # skip GitHub API step
  python proxy_checker.py run --skip-check    # only fetch, don't check
  python proxy_checker.py fetch --type all     # fetch built-in sources only
  python proxy_checker.py repos --type all     # fetch GitHub repos only
  python proxy_checker.py repos --list-repos   # list configured repos
  python proxy_checker.py check --type all     # check offline proxies only
  python proxy_checker.py add --list FILE --type http
  python proxy_checker.py add --urls FILE --type socks5h
  python proxy_checker.py stats
"""

import os
import re
import sys
import time
import json
import random
import asyncio
import logging
import argparse
import traceback
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Zero-setup: auto-install missing dependencies before anything else imports
# ---------------------------------------------------------------------------

def _bootstrap_deps() -> None:
    """Install any missing third-party packages using the current interpreter."""
    _required = [
        ("aiohttp",        "aiohttp>=3.9.0"),
        ("aiohttp_socks",  "aiohttp-socks>=0.8.0"),
        ("aiosqlite",      "aiosqlite>=0.19.0"),
    ]
    missing = []
    for module, pkg_spec in _required:
        try:
            __import__(module)
        except ImportError:
            missing.append(pkg_spec)
    if missing:
        print(f"[*] Auto-installing missing packages: {', '.join(missing)} …", flush=True)
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing
        )
        print("[*] Packages installed — continuing.", flush=True)

_bootstrap_deps()

import aiohttp
from aiohttp_socks import ProxyConnector

# Optional database support (requires aiosqlite).
try:
    import proxy_db as _proxy_db
    _DB_ENABLED = True
except ImportError:
    _DB_ENABLED = False

# ---------------------------------------------------------------------------
# Global error handler
#
# Catches every unhandled exception — both synchronous (sys.excepthook) and
# asyncio task exceptions — logs them to proxy_checker_errors.log WITH a
# full traceback and timestamp, and prints a short summary to stderr.
# The process is NEVER terminated by an unhandled exception; run_loop
# wraps pipeline iterations so errors cause a retry, not a crash.
# ---------------------------------------------------------------------------

_ERROR_LOG = Path("proxy_checker_errors.log")

logging.basicConfig(
    filename=str(_ERROR_LOG),
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_log = logging.getLogger("proxy_checker")


def _fmt_exc(exc_type, exc_value, exc_tb) -> str:
    return "".join(traceback.format_exception(exc_type, exc_value, exc_tb))


def _global_excepthook(exc_type, exc_value, exc_tb) -> None:
    """sys.excepthook — catches any unhandled top-level exception."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    msg = _fmt_exc(exc_type, exc_value, exc_tb)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[!] [{ts}] UNHANDLED EXCEPTION:\n{msg}", file=sys.stderr, flush=True)
    _log.error("Unhandled exception:\n%s", msg)


def _asyncio_exception_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    """asyncio event-loop exception handler — catches unhandled task errors."""
    exc = context.get("exception")
    desc = context.get("message", "Unknown asyncio error")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    if exc is not None:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        full = f"{desc}\n{tb}"
    else:
        full = desc
    print(f"\n[!] [{ts}] ASYNCIO ERROR: {full}", file=sys.stderr, flush=True)
    _log.error("Asyncio error: %s", full)


def _install_error_handlers() -> None:
    """Install both error hooks.  Call once at program start."""
    sys.excepthook = _global_excepthook
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(_asyncio_exception_handler)
    except RuntimeError:
        pass  # no running loop yet — asyncio handler set in async_main instead

PROXY_LISTS_DIR = Path("proxy_lists")
PROXY_TYPES = ["http", "https", "socks4", "socks4a", "socks5", "socks5h"]
# ~100 IP-echo services used to verify each proxy.
# Each proxy gets exactly one URL (round-robin). One check per proxy.
# Grouped: plain-text IP, JSON IP, HTTP fallbacks.
CHECKER_URLS: List[str] = [
    # ── Plain-text IP response (fastest — no JSON parsing needed) ────────────
    "https://api.ipify.org",
    "https://api4.ipify.org",
    "https://ipv4.icanhazip.com",
    "https://icanhazip.com",
    "https://ident.me",
    "https://4.ident.me",
    "https://ipecho.net/plain",
    "https://checkip.amazonaws.com",
    "https://myexternalip.com/raw",
    "https://wtfismyip.com/text",
    "https://ip.seeip.org",
    "https://ip4.seeip.org",
    "https://ipv4.seeip.org",
    "https://ifconfig.me/ip",
    "https://ifconfig.co/ip",
    "https://ifconfig.io/ip",
    "https://ifconfig.es",
    "https://ip.tyk.nu",
    "https://l2.io/ip",
    "https://echoip.de",
    "https://myip.wtf/text",
    "https://ip.42.pl/raw",
    "https://myip.dnsomatic.com",
    "https://ip.rootnet.in",
    "https://ip.ryans.org",
    "https://ipaddr.site",
    "https://ip.nf/me.txt",
    "https://www.trackip.net/ip",
    "https://api.my-ip.io/ip",
    "https://api4.my-ip.io/ip",
    "https://checkip4.optimizely.com",
    "https://ip.oxylabs.io",
    "https://ipv4.clarketm.com",
    "https://ip.websupport.sk/",
    "https://curlmyip.net",
    "https://wgetip.com",
    "https://eth0.me",
    "https://ipof.in/txt",
    "https://ip.neustar.biz",
    "https://myip.com.au/ip.txt",
    "https://ip.seeip.org/",
    # ── JSON IP response ─────────────────────────────────────────────────────
    "https://ipinfo.io/ip",
    "https://ipinfo.io/json",
    "https://httpbin.org/ip",
    "https://jsonip.com",
    "https://ip-api.io/json",
    "https://ipapi.co/ip",
    "https://ipapi.co/json",
    "https://ipwho.is",
    "https://api.ip.sb/ip",
    "https://api.ip.sb/geoip",
    "https://ipv4bot.whatismyipaddress.com",
    "https://bot.whatismyip.com/ip",
    "https://api.bigdatacloud.net/data/client-ip",
    "https://api.iplocation.net/?cmd=get-ip",
    "https://freeipapi.com/api/json",
    "https://ipgeolocation.io/",
    "https://ipapi.is/json",
    "https://ip.guide",
    "https://api.ipquery.io/",
    "https://api.ipdata.co?api-key=test",
    "https://ip-api.com/json",
    "https://ipwhois.app/json/",
    "https://ipstack.com/",
    "https://freegeoip.app/json/",
    "https://geoipify.whoisxmlapi.com/api/v1",
    "https://ipregistry.co/?key=tryout",
    "https://extreme-ip-lookup.com/json/",
    "https://www.geoplugin.net/json.gp",
    "https://get.geojs.io/v1/ip",
    "https://get.geojs.io/v1/ip/geo.json",
    "https://api.db-ip.com/v2/free/self",
    "https://geolocation-db.com/json/",
    "https://api.hostip.info/get_json.php",
    "https://iplist.cc/api",
    "https://api.techniknews.net/ipgeo/",
    "https://ipdetective.io/json",
    "https://iplogger.org/api/geolocation",
    "https://ip-api.com/json/?fields=query",
    # ── Cloudflare / CDN traces ───────────────────────────────────────────────
    "https://www.cloudflare.com/cdn-cgi/trace",
    "https://1.1.1.1/cdn-cgi/trace",
    "https://speed.cloudflare.com/meta",
    # ── HTTP (non-TLS) fallbacks — useful for checking plain HTTP proxies ─────
    "http://checkip.amazonaws.com",
    "http://ident.me",
    "http://ipv4.icanhazip.com",
    "http://ip-api.com/json",
    "http://ifconfig.me/ip",
    "http://ifconfig.co/ip",
    "http://myexternalip.com/raw",
    "http://api.ipify.org",
    "http://wtfismyip.com/text",
    "http://ipecho.net/plain",
    "http://curlmyip.net",
    "http://wgetip.com",
    "http://eth0.me",
    "http://ip.seeip.org",
    "http://myip.dnsomatic.com",
    "http://ipapi.co/ip",
]

# Built-in proxy-list source URLs, organized by proxy type.
# Sources include: GitHub auto-updated repos, proxyscrape v2/v3,
# proxy-list.download, geonode, proxyscan.io, openproxy.space, spys.me.
# Used by the `fetch` command.
PROXY_SOURCE_URLS: Dict[str, List[str]] = {
    "http": [
        # ── GitHub repos ────────────────────────────────────────────────────
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTP.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
        "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/free.txt",
        "https://raw.githubusercontent.com/hanwayTech/free-proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/http.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/im-razvan/proxy_list/main/http.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
        "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt",
        "https://raw.githubusercontent.com/yuceltoluyag/GoodProxy/main/raw.txt",
        "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
        "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/http_proxies.txt",
        "https://raw.githubusercontent.com/themiralay/Proxy-List-World/master/data.txt",
        "https://raw.githubusercontent.com/andigwandi/free-proxy/main/proxy_list.txt",
        "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/http/global/http_checked.txt",
        # ── Web APIs ────────────────────────────────────────────────────────
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=http&timeout=10000&proxy_format=ipport&format=text",
        "https://spys.me/proxy.txt",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=http",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=http",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=4&sort_by=lastChecked&sort_type=desc&protocols=http",
    ],
    "https": [
        # ── GitHub repos ────────────────────────────────────────────────────
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/https_proxies.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
        "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Https.txt",
        "https://raw.githubusercontent.com/zloi-user/hideip.me/main/https.txt",
        "https://raw.githubusercontent.com/aslisk/proxyhttps/main/https.txt",
        # ── Web APIs ────────────────────────────────────────────────────────
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=https&timeout=10000&country=all",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=https&timeout=10000&proxy_format=ipport&format=text",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=https",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=https",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=https",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=4&sort_by=lastChecked&sort_type=desc&protocols=https",
    ],
    "socks4": [
        # ── GitHub repos ────────────────────────────────────────────────────
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS4.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt",
        "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "https://raw.githubusercontent.com/hanwayTech/free-proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks4.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/socks4.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt",
        "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks4.txt",
        "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt",
        "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/socks4_proxies.txt",
        "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/socks4/global/socks4_checked.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks4_proxies.txt",
        # ── Web APIs ────────────────────────────────────────────────────────
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000&country=all",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=socks4&timeout=10000&proxy_format=ipport&format=text",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks4",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks4",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=socks4",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=4&sort_by=lastChecked&sort_type=desc&protocols=socks4",
    ],
    "socks5": [
        # ── GitHub repos ────────────────────────────────────────────────────
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS5.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
        "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "https://raw.githubusercontent.com/hanwayTech/free-proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/socks5.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt",
        "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks5.txt",
        "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt",
        "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/socks5_proxies.txt",
        "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/socks5/global/socks5_checked.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks5_proxies.txt",
        # ── Web APIs ────────────────────────────────────────────────────────
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=socks5&timeout=10000&proxy_format=ipport&format=text",
        "https://spys.me/socks.txt",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks5",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks5",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=socks5",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=4&sort_by=lastChecked&sort_type=desc&protocols=socks5",
    ],
    # socks4a = SOCKS4 with remote hostname resolution.
    # Public scrapers don't always separate socks4 / socks4a, so we reuse
    # the same raw lists — any SOCKS4 server that supports domain lookup
    # will work here too.
    "socks4a": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS4.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks4.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt",
        "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks4.txt",
        "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt",
        "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/socks4/global/socks4_checked.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000&country=all",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=socks4&timeout=10000&proxy_format=ipport&format=text",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks4",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks4",
    ],
    # socks5h = SOCKS5 with remote DNS resolution.
    # Same raw sources as socks5 — any SOCKS5 server can be used as socks5h.
    "socks5h": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS5.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt",
        "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks5.txt",
        "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt",
        "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/socks5/global/socks5_checked.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks5_proxies.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=socks5&timeout=10000&proxy_format=ipport&format=text",
        "https://spys.me/socks.txt",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks5",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks5",
    ],
}

# ---------------------------------------------------------------------------
# GitHub repo auto-discovery
# ---------------------------------------------------------------------------

# Matches https://github.com/{owner}/{repo} (optional trailing slash)
_GITHUB_REPO_RE = re.compile(
    r"^https?://github\.com/([A-Za-z0-9_.\-]+)/([A-Za-z0-9_.\-]+)/?$"
)

# Proxy-list GitHub repositories whose .txt files are auto-discovered via the
# GitHub API when the `repos` command is run.  Includes all repos from the
# provided master list plus the other repos whose raw URLs are already in
# PROXY_SOURCE_URLS — so every source is reachable both ways.
GITHUB_REPO_SOURCES: List[str] = [
    # ── Master list repos ────────────────────────────────────────────────
    "https://github.com/TheSpeedX/PROXY-List",
    "https://github.com/TheSpeedX/SOCKS-List",
    "https://github.com/ShiftyTR/Proxy-List",
    "https://github.com/monosans/proxy-list",
    "https://github.com/jetkai/proxy-list",
    "https://github.com/clarketm/proxy-list",
    "https://github.com/roosterkid/openproxylist",
    "https://github.com/mmpx12/proxy-list",
    "https://github.com/hookzof/socks5_list",
    "https://github.com/opsxcq/proxy-list",
    # ── Additional repos whose raw URLs are already in PROXY_SOURCE_URLS ─
    "https://github.com/B4RC0DE-TM/proxy-list",
    "https://github.com/zevtyardt/proxy-list",
    "https://github.com/MuRongPIG/Proxy-Master",
    "https://github.com/prxchk/proxy-list",
    "https://github.com/Anonym0usWork1221/Free-Proxies",
    "https://github.com/proxifly/free-proxy-list",
    "https://github.com/vakhov/fresh-proxy-list",
    "https://github.com/proxy4parsing/proxy-list",
    "https://github.com/Zaeem20/FREE_PROXIES_LIST",
    "https://github.com/r00tee/Proxy-List",
    "https://github.com/zloi-user/hideip.me",
    "https://github.com/proxylist-to/proxy-list",
    "https://github.com/dpangestuw/Free-Proxy",
    "https://github.com/elliottophellia/yakumo",
    "https://github.com/ErcinDedeoglu/proxies",
    "https://github.com/ALIILAPRO/Proxy",
    "https://github.com/ObcbO/getproxy",
    "https://github.com/rdavydov/proxy-list",
    "https://github.com/sunny9577/proxy-scraper",
    "https://github.com/hanwayTech/free-proxy-list",
    "https://github.com/hendrikbgr/Free-Proxy-Repo",
    "https://github.com/yuceltoluyag/GoodProxy",
    "https://github.com/aslisk/proxyhttps",
    "https://github.com/saisuiu/Lionkings-Http-Proxys-Proxies",
    "https://github.com/im-razvan/proxy_list",
    "https://github.com/themiralay/Proxy-List-World",
    "https://github.com/andigwandi/free-proxy",
]


def _classify_proxy_file(path: str) -> Optional[str]:
    """Classify a .txt file path as a proxy type based on keywords.

    Checks are ordered most-specific → least-specific to avoid false matches
    (e.g. ``socks5`` before ``socks4``, ``https`` before ``http``).
    Returns one of the ``PROXY_TYPES`` strings or ``None`` if the file does
    not appear to contain a typed proxy list.
    """
    lower = path.lower()
    if "socks5h" in lower:
        return "socks5h"
    if "socks4a" in lower:
        return "socks4a"
    if "socks5" in lower:
        return "socks5"
    if "socks4" in lower:
        return "socks4"
    if "socks" in lower:
        return "socks5"    # bare "socks" → treat as socks5
    if "https" in lower:
        return "https"
    if "http" in lower:
        return "http"
    # Common generic filenames that typically contain HTTP proxies
    name = path.rsplit("/", 1)[-1].lower()
    if name in {
        "proxy.txt", "proxies.txt", "list.txt", "raw.txt", "data.txt",
        "free.txt", "proxy-list-raw.txt", "proxy_list.txt",
        "proxy-list.txt", "proxylist.txt",
    }:
        return "http"
    return None


async def _resolve_github_repo(
    repo_url: str,
    session: aiohttp.ClientSession,
    token: Optional[str] = None,
) -> Dict[str, List[str]]:
    """Query the GitHub API to discover all proxy-list ``.txt`` files in
    *repo_url* and return a mapping of ``{proxy_type: [raw_url, ...]}``.

    *repo_url* must match ``https://github.com/{owner}/{repo}``.

    *token* is a GitHub personal access token; if omitted the
    ``GITHUB_TOKEN`` env var is used as a fallback.  Authenticated calls
    have a 5 000 req/hour rate limit vs 60 req/hour unauthenticated.
    """
    m = _GITHUB_REPO_RE.match(repo_url.rstrip("/"))
    if not m:
        print(f"[!] Not a valid GitHub repo URL: {repo_url}")
        return {}
    owner, repo = m.group(1), m.group(2)

    effective_token = token or os.environ.get("GITHUB_TOKEN")
    headers: Dict[str, str] = {"Accept": "application/vnd.github.v3+json"}
    if effective_token:
        headers["Authorization"] = f"Bearer {effective_token}"

    api_base = f"https://api.github.com/repos/{owner}/{repo}"

    # Discover the default branch first (avoids hard-coding "main"/"master").
    default_branch = "main"
    try:
        async with session.get(
            api_base,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
            ssl=False,
        ) as resp:
            if resp.status == 200:
                meta = await resp.json(content_type=None)
                default_branch = meta.get("default_branch", "main")
    except Exception:
        pass

    # Fetch the full recursive file tree for the default branch.
    branches_to_try = [default_branch] + (
        ["master"] if default_branch != "master" else ["main"]
    )
    data: dict = {}
    used_branch = default_branch
    for branch in branches_to_try:
        tree_url = f"{api_base}/git/trees/{branch}?recursive=1"
        try:
            async with session.get(
                tree_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    used_branch = branch
                    break
                if resp.status == 404:
                    continue
                # Rate-limited or unexpected error
                body = await resp.text()
                print(
                    f"[!] GitHub API {resp.status} for {owner}/{repo}: "
                    f"{body[:120]}"
                )
                return {}
        except Exception as exc:
            print(f"[!] GitHub API request failed for {owner}/{repo}: {exc}")
            return {}
    else:
        print(f"[!] Could not resolve branch for {owner}/{repo}")
        return {}

    raw_base = f"https://raw.githubusercontent.com/{owner}/{repo}/{used_branch}"
    result: Dict[str, List[str]] = {}
    for item in data.get("tree", []):
        if item.get("type") != "blob":
            continue
        path = item["path"]
        if not path.endswith(".txt"):
            continue
        ptype = _classify_proxy_file(path)
        if ptype is None:
            continue
        result.setdefault(ptype, []).append(f"{raw_base}/{path}")

    found = sum(len(v) for v in result.values())
    if found:
        summary = ", ".join(
            f"{t}×{len(urls)}" for t, urls in sorted(result.items())
        )
        print(f"    {owner}/{repo}  → {found} file(s)  [{summary}]")
    else:
        print(f"    {owner}/{repo}  → no proxy .txt files found")
    return result


async def fetch_github_repos(
    proxy_type: str,
    token: Optional[str] = None,
) -> None:
    """Query every repo in ``GITHUB_REPO_SOURCES`` via the GitHub API,
    discover raw proxy-list URLs, and import the proxies.

    *proxy_type* is ``"all"`` or one of the six specific types.
    *token* overrides the ``GITHUB_TOKEN`` env var.
    """
    effective_token = token or os.environ.get("GITHUB_TOKEN")
    if not effective_token:
        print(
            "[i] No GITHUB_TOKEN set — using unauthenticated GitHub API "
            "(60 req/hour limit).  Set GITHUB_TOKEN env var for higher limits."
        )

    types_wanted: set = set(PROXY_TYPES if proxy_type == "all" else [proxy_type])
    print(f"\n[*] Resolving {len(GITHUB_REPO_SOURCES)} GitHub repos via API …\n")

    # Use a low concurrency limit to avoid hammering the GitHub API.
    connector = aiohttp.TCPConnector(ssl=False, limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await asyncio.gather(
            *[
                _resolve_github_repo(url, session, effective_token)
                for url in GITHUB_REPO_SOURCES
            ],
            return_exceptions=True,
        )

    # Merge: group discovered raw URLs by proxy type.
    by_type: Dict[str, List[str]] = {}
    for repo_url, result in zip(GITHUB_REPO_SOURCES, results):
        if isinstance(result, Exception):
            print(f"[!] Exception resolving {repo_url}: {result}")
            continue
        for ptype, urls in result.items():
            if ptype in types_wanted:
                by_type.setdefault(ptype, []).extend(urls)

    # Deduplicate (the same raw URL can appear in multiple repos).
    for ptype in by_type:
        seen: set = set()
        by_type[ptype] = [
            u for u in by_type[ptype] if not (u in seen or seen.add(u))  # type: ignore[func-returns-value]
        ]

    # Import each type's URLs.
    for ptype in sorted(by_type):
        urls = by_type[ptype]
        if urls:
            print(f"\n[*] Fetching {len(urls)} discovered raw URLs for {ptype.upper()} …")
            await _import_from_url_list(urls, ptype)


# ---------------------------------------------------------------------------
# In-memory write buffer
#
# All proxy list file I/O goes through this layer:
#   • Reads are served from RAM (_mem_cache) after the first disk load.
#   • Writes update only _mem_cache and mark the path dirty — zero disk I/O.
#   • A background task (_buffer_flusher_task) persists dirty files to disk
#     every BUFFER_FLUSH_INTERVAL seconds, or when BUFFER_FLUSH_EVERY_N
#     proxies have been scanned since the last flush — whichever comes first.
#   • flush_write_buffer() can also be called explicitly (e.g. after a full
#     check cycle or on graceful shutdown).
#
# Because asyncio is cooperative (single-threaded) and append_proxy /
# remove_proxy contain no ``await`` between their read and write, the
# read-modify-write is inherently atomic — no per-file locking is needed.
# ---------------------------------------------------------------------------

#: Seconds between automatic flush-to-disk cycles (safety net).
BUFFER_FLUSH_INTERVAL: int = 60
#: Also flush after this many proxies have been scanned since the last flush.
BUFFER_FLUSH_EVERY_N: int = 1000
#: Number of proxies processed per asyncio.gather batch.
#: Keeps Task object memory bounded (~30 MB per batch vs ~2 GB for 800k at once).
SCAN_BATCH_SIZE: int = 10_000

# resolved-path-string → ordered, deduplicated list of entries (live state)
_mem_cache: Dict[str, List[str]] = {}
# resolved-path-strings that have unsaved changes
_mem_dirty: set = set()

_last_flush_time: float = 0.0          # monotonic timestamp of last flush
_flusher_handle: Optional[asyncio.Task] = None  # background task reference
_checked_since_flush: int = 0          # proxies scanned since last flush


def _cache_key(path: Path) -> str:
    return str(path.resolve())


def _cache_size_bytes() -> int:
    """Approximate number of bytes held in the RAM cache across all files."""
    return sum(
        sum(len(e) + 1 for e in entries)   # +1 for newline per entry
        for entries in _mem_cache.values()
    )


def read_proxies(path: Path) -> List[str]:
    """Return deduplicated, non-empty lines for *path*.

    Served from the RAM cache when available; otherwise the file is read
    from disk, cached, and a copy is returned so callers may mutate freely.
    """
    key = _cache_key(path)
    if key in _mem_cache:
        return list(_mem_cache[key])      # return a copy
    if not path.exists():
        _mem_cache[key] = []
        return []
    seen: set = set()
    result: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and line not in seen:
            seen.add(line)
            result.append(line)
    _mem_cache[key] = result
    return list(result)


def write_proxies(path: Path, proxies: List[str]) -> None:
    """Update the RAM cache for *path* and mark it dirty.

    Deduplicates by bare IP:PORT (stripping all decorations) so the same
    proxy can never appear twice even if its decorated form differs between
    checks (e.g. different response times).  When duplicates exist the
    *last* occurrence wins, which keeps the most recent check result.

    No disk I/O happens here.  The background flusher (or an explicit
    flush_write_buffer() call) persists the data to disk.
    """
    # Use an ordered dict keyed by bare IP:PORT so the last write wins.
    deduped: Dict[str, str] = {}
    for p in proxies:
        deduped[strip_decorations(p)] = p
    key = _cache_key(path)
    _mem_cache[key] = list(deduped.values())
    _mem_dirty.add(key)


def _flush_one(key: str) -> None:
    """Atomically write one cached file to disk (temp → os.replace)."""
    entries = _mem_cache.get(key, [])
    path = Path(key)
    content = "\n".join(entries) + ("\n" if entries else "")
    tmp = path.with_suffix(".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def flush_write_buffer() -> int:
    """Flush all dirty cache entries to disk right now.

    Returns the number of files successfully written.
    Resets _checked_since_flush so the count-based trigger restarts.
    """
    global _last_flush_time, _checked_since_flush
    _last_flush_time = time.monotonic()
    _checked_since_flush = 0
    if not _mem_dirty:
        return 0
    dirty_snapshot = list(_mem_dirty)
    errors: List[str] = []
    for key in dirty_snapshot:
        try:
            _flush_one(key)
            _mem_dirty.discard(key)
        except Exception as exc:
            errors.append(f"  {Path(key).name}: {exc}")
    if errors:
        print(f"[!] Flush errors ({len(errors)}):")
        for e in errors:
            print(e)
    return len(dirty_snapshot) - len(errors)


async def _buffer_flusher_task() -> None:
    """Background asyncio task: flush dirty files every BUFFER_FLUSH_INTERVAL
    seconds OR after BUFFER_FLUSH_EVERY_N proxies have been scanned —
    whichever comes first.
    """
    global _last_flush_time
    _last_flush_time = time.monotonic()
    while True:
        await asyncio.sleep(5)
        elapsed   = time.monotonic() - _last_flush_time
        due_time  = elapsed >= BUFFER_FLUSH_INTERVAL
        due_count = _checked_since_flush >= BUFFER_FLUSH_EVERY_N
        if (due_time or due_count) and _mem_dirty:
            n  = flush_write_buffer()
            ts = time.strftime("%H:%M:%S")
            reason = "count" if due_count else "timer"
            print(
                f"[~] [{ts}] Flushed {n} file(s) to disk  "
                f"({_checked_since_flush} scanned, {elapsed:.0f}s elapsed, reason={reason})"
            )


def start_buffer_flusher() -> "asyncio.Task[None]":
    """Schedule the background flusher as an asyncio task.

    Must be called from inside a running event loop (i.e. inside an
    async function).  Safe to call multiple times — only one task is
    ever created.
    """
    global _flusher_handle
    if _flusher_handle is None or _flusher_handle.done():
        _flusher_handle = asyncio.create_task(_buffer_flusher_task())
    return _flusher_handle


# append_proxy / remove_proxy contain no ``await`` between their read and
# write, so the read-modify-write is atomic from asyncio's perspective.
# No per-file locking is required.

async def append_proxy(path: Path, entry: str) -> None:
    """Append *entry* to the RAM cache for *path*, deduplicating by bare IP:PORT.

    If the same IP:PORT is already in the list (regardless of decorations
    such as response time or country tag), the old entry is replaced with
    the new one so the list always reflects the most recent check result.
    Marks the file dirty — no disk I/O.
    """
    bare = strip_decorations(entry)
    existing = read_proxies(path)
    updated = [e for e in existing if strip_decorations(e) != bare]
    if len(updated) == len(existing) and entry in existing:
        return  # exact duplicate, nothing to do
    updated.append(entry)
    write_proxies(path, updated)


async def remove_proxy(path: Path, entry: str) -> None:
    """Remove *entry* from the RAM cache for *path*.
    Marks the file dirty — no disk I/O.
    """
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


def load_sntbs_files() -> None:
    """Import proxies from any SNTBS-{type}.txt files found in the current directory.

    SNTBS (Still Need To Be Scanned) files act as a persistent pending queue.
    Each file is named ``SNTBS-{proxytype}.txt`` (e.g. ``SNTBS-socks5.txt``)
    and contains one ``IP:PORT`` per line.

    Behaviour:
      - IPs already in the online or fallen list are silently dropped.
      - IPs already in the offline queue are silently skipped (already pending).
      - All remaining IPs are appended to the corresponding
        ``offline_{type}.txt`` so they will be checked on the next run.
      - After importing, the SNTBS file is cleared because the imported IPs
        are now tracked in the offline list.  Any IPs that go unchecked during
        a run are written back to the SNTBS file by :func:`check_proxies`.
    """
    for ptype in PROXY_TYPES:
        sntbs = Path(f"SNTBS-{ptype}.txt")
        if not sntbs.exists():
            continue

        sntbs_entries = read_proxies(sntbs)
        if not sntbs_entries:
            continue

        offline = PROXY_LISTS_DIR / f"offline_{ptype}.txt"
        online  = PROXY_LISTS_DIR / f"online_{ptype}.txt"
        fallen  = PROXY_LISTS_DIR / f"fallen_{ptype}.txt"

        existing_offline = set(read_proxies(offline))
        already_online   = {strip_decorations(e) for e in read_proxies(online)}
        already_fallen   = {strip_decorations(e) for e in read_proxies(fallen)}

        added = 0
        for line in sntbs_entries:
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
            write_proxies(offline, sorted(existing_offline))
            print(f"[*] SNTBS: imported {added} new {ptype.upper()} proxies → {offline.name}")

        # Clear the SNTBS file; unchecked IPs will be written back by check_proxies.
        sntbs.write_text("", encoding="utf-8")


async def _init_db_once() -> None:
    """Initialise the SQLite database (no-op if aiosqlite is not installed)."""
    if _DB_ENABLED:
        try:
            await _proxy_db.init_db()
        except Exception as exc:
            print(f"[!] DB init failed: {exc}")


# ---------------------------------------------------------------------------
# Proxy parsing helpers
# ---------------------------------------------------------------------------

_TYPE_PREFIX_RE = re.compile(r"^\[.*?\]")
_RESPONSE_TIME_RE = re.compile(r"\(.*?\)$")
_BRACKET_SUFFIX_RE = re.compile(r"(\[[^\]]*\])+$")
# Used to validate that a checker-URL response body actually contains an IPv4
# address.  CDN error pages, captcha blocks and landing pages don't.
_IP_PATTERN = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def strip_decorations(entry: str) -> str:
    """Remove all decorations from a stored proxy entry, leaving only IP:PORT.

    Handles the full stored format:
      ``[TYPE]IP:PORT(Xms)[COUNTRY][ANONYMITY][ISP]``
    """
    entry = _TYPE_PREFIX_RE.sub("", entry).strip()    # remove leading [TYPE]
    entry = _BRACKET_SUFFIX_RE.sub("", entry).strip() # remove [COUNTRY][...] suffixes
    entry = _RESPONSE_TIME_RE.sub("", entry).strip()  # remove (Xms) suffix
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
    http_session: Optional[aiohttp.ClientSession] = None,
) -> Tuple[bool, float]:
    """
    Asynchronously attempt to reach *checker_url* through the proxy at ip:port.
    Returns (success, response_time_ms).

    Supported proxy types: http, https, socks4, socks4a, socks5, socks5h.

    *http_session* — a pre-created shared ClientSession for HTTP/HTTPS proxy
    types.  Reusing one session across all workers eliminates per-check TCP
    setup overhead while keeping full detection accuracy (each request still
    goes through a different proxy via the ``proxy=`` parameter).
    SOCKS proxies require a per-request ProxyConnector and always create their
    own short-lived session.

    Response-body validation: the body must contain at least one IPv4 address.
    All legitimate IP-echo services return the client's IP in their response
    (plain text, JSON, or key=value).  CDN block pages, captcha challenges,
    and misconfigured hosts that intercept the connection return HTML without
    a bare IP address, so they are rejected even when the HTTP status is 200.
    """
    proxy_url = f"{proxy_type}://{ip}:{port}"
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    start = time.monotonic()
    try:
        if proxy_type in ("socks4", "socks4a", "socks5", "socks5h"):
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
                        body = await resp.content.read(8192)
                        if _IP_PATTERN.search(body.decode("utf-8", errors="ignore")):
                            return True, round(elapsed_ms, 2)
        else:
            # HTTP / HTTPS — use the shared session when available so we
            # don't create+destroy a TCPConnector for every single proxy.
            if http_session is not None:
                async with http_session.get(
                    checker_url,
                    proxy=proxy_url,
                    timeout=client_timeout,
                    ssl=False,
                    allow_redirects=True,
                ) as resp:
                    elapsed_ms = (time.monotonic() - start) * 1000.0
                    if 200 <= resp.status < 300:
                        body = await resp.content.read(8192)
                        if _IP_PATTERN.search(body.decode("utf-8", errors="ignore")):
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
                            body = await resp.content.read(8192)
                            if _IP_PATTERN.search(body.decode("utf-8", errors="ignore")):
                                return True, round(elapsed_ms, 2)
    except Exception:
        pass
    return False, 0.0


# ---------------------------------------------------------------------------
# Constants for geo-lookup and anonymity checking
# ---------------------------------------------------------------------------

# URL used to detect proxy-revealing headers (anonymity check).
_ANONYMITY_CHECK_URL = "https://httpbin.org/get"

# Headers that indicate a non-Elite proxy when forwarded to the server.
_ANONYMITY_PROXY_HEADERS = frozenset({
    "X-Forwarded-For", "X-Real-Ip", "Via", "Proxy-Connection",
    "Forwarded", "X-Proxy-Id", "X-Forwarded-Host",
})

# Semaphore to avoid flooding free geo-lookup and anonymity-check services.
_GEO_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _get_geo_semaphore() -> asyncio.Semaphore:
    global _GEO_SEMAPHORE
    if _GEO_SEMAPHORE is None:
        _GEO_SEMAPHORE = asyncio.Semaphore(50)
    return _GEO_SEMAPHORE


async def _geo_lookup(
    ip: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> Dict[str, str]:
    """
    Retrieve geographic and network information for *ip* using ipwho.is.
    Returns a dict with keys: ``country_code``, ``country``, ``city``, ``isp``.
    Falls back to empty strings on any error.

    Pass a shared *session* (created once in check_proxies) to avoid
    creating and tearing down a new TCP connection for every lookup.
    """
    result = {"country_code": "", "country": "", "city": "", "isp": ""}
    geo_timeout = aiohttp.ClientTimeout(total=8)
    async with _get_geo_semaphore():
        try:
            url = f"https://ipwho.is/{ip}"
            if session is not None:
                async with session.get(url, timeout=geo_timeout, ssl=False) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        result["country_code"] = data.get("country_code") or ""
                        result["country"] = data.get("country") or ""
                        result["city"] = data.get("city") or ""
                        result["isp"] = (
                            data.get("connection", {}).get("isp")
                            or data.get("org") or ""
                        )
            else:
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as s:
                    async with s.get(url, timeout=geo_timeout, ssl=False) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            result["country_code"] = data.get("country_code") or ""
                            result["country"] = data.get("country") or ""
                            result["city"] = data.get("city") or ""
                            result["isp"] = (
                                data.get("connection", {}).get("isp")
                                or data.get("org") or ""
                            )
        except Exception:
            pass
    return result


async def _anonymity_check(
    ip: str,
    port: int,
    proxy_type: str,
    timeout: int,
) -> str:
    """
    Detect the anonymity level of a proxy by requesting
    ``https://httpbin.org/get`` through it and inspecting the headers
    the server received.

    Returns one of:
      ``"Elite"``     – no proxy-revealing headers forwarded
      ``"Anonymous"`` – proxy-type headers present but no client IP leaked
      ``"Unknown"``   – could not reach the check endpoint
    """
    proxy_url = f"{proxy_type}://{ip}:{port}"
    client_timeout = aiohttp.ClientTimeout(total=timeout)   # fix: was NameError
    async with _get_geo_semaphore():
        try:
            if proxy_type in ("socks4", "socks4a", "socks5", "socks5h"):
                connector = ProxyConnector.from_url(proxy_url, ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        _ANONYMITY_CHECK_URL, timeout=client_timeout, ssl=False
                    ) as resp:
                        data = await resp.json(content_type=None)
            else:
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        _ANONYMITY_CHECK_URL, proxy=proxy_url,
                        timeout=client_timeout, ssl=False
                    ) as resp:
                        data = await resp.json(content_type=None)

            received_headers = set(data.get("headers", {}).keys())
            if received_headers & _ANONYMITY_PROXY_HEADERS:
                return "Anonymous"
            return "Elite"
        except Exception:
            return "Unknown"


async def _check_worker(
    raw: str,
    proxy_type: str,
    checker_url: str,
    timeout: int,
    semaphore: asyncio.Semaphore,
    online: Path,
    fallen: Path,
    counters: Dict[str, int],
    http_session: Optional[aiohttp.ClientSession] = None,
    geo_session: Optional[aiohttp.ClientSession] = None,
    checked_set: Optional[Set[str]] = None,
) -> None:
    """Async worker: test one proxy against one checker URL.

    Each proxy gets exactly one checker URL (round-robin across the pool).
    The offline list is NOT modified here; check_proxies bulk-clears it
    after the whole batch completes, avoiding O(n²) per-proxy rewrites.
    _checked_since_flush is incremented so the background flusher knows
    when 1000 proxies have been processed and triggers a disk write.
    When *checked_set* is provided, the bare ``IP:PORT`` of every processed
    proxy is added to it so that :func:`check_proxies` can detect which
    proxies were not reached (e.g. on interruption) and save them to the
    SNTBS file.
    """
    global _checked_since_flush
    async with semaphore:
        parsed = parse_proxy(raw)
        if parsed is None:
            return

        ip, port = parsed
        bare = f"{ip}:{port}"
        success, ms = await check_proxy(
            ip, port, proxy_type, checker_url, timeout, http_session
        )

        if success:
            geo, anon = await asyncio.gather(
                _geo_lookup(ip, geo_session),
                _anonymity_check(ip, port, proxy_type, timeout),
            )
            country = geo["country_code"] or "??"
            isp = geo["isp"]
            entry = f"[{proxy_type.upper()}]{ip}:{port}({ms}ms)[{country}][{anon}]"
            if isp:
                entry += f"[{isp}]"
            await append_proxy(online, entry)

            geo_str = f"  {country}"
            if geo["city"]:
                geo_str += f", {geo['city']}"
            if isp:
                geo_str += f"  {isp}"
            print(
                f"[+] ONLINE  {ip}:{port}  {ms:.0f} ms  ({proxy_type.upper()})"
                f"  [{anon}]{geo_str}"
            )
            counters["online"] += 1
        else:
            await append_proxy(fallen, bare)
            print(f"[-] FALLEN  {ip}:{port}")
            counters["fallen"] += 1

        # Mark as checked immediately after the result is committed to the
        # RAM cache — before any optional DB awaits.  This ensures checked_set
        # is accurate even if a CancelledError fires during the DB write.
        counters["checked"] += 1
        _checked_since_flush += 1
        if checked_set is not None:
            checked_set.add(bare)

        if _DB_ENABLED:
            try:
                if success:
                    await _proxy_db.upsert_proxy(
                        ip=ip, port=port, proxy_type=proxy_type,
                        status="online",
                        country_code=geo.get("country_code", ""),
                        country=geo.get("country", ""),
                        city=geo.get("city", ""),
                        isp=geo.get("isp", ""),
                        anonymity=anon,
                        response_ms=ms,
                    )
                else:
                    await _proxy_db.upsert_proxy(
                        ip=ip, port=port, proxy_type=proxy_type,
                        status="fallen",
                    )
            except Exception as exc:
                print(f"[!] DB write failed for {bare}: {exc}")


# ---------------------------------------------------------------------------
# Async URL fetching
# ---------------------------------------------------------------------------

def _parse_json_proxies(text: str) -> List[str]:
    """
    Extract ``IP:PORT`` strings from a JSON response body.

    Handles the two most common API shapes:
      • geonode  – ``{"data": [{"ip": "...", "port": "..."}, ...]}``
      • plain array – ``[{"ip": "...", "port": "..."}, ...]``
                   or ``["ip:port", ...]``
    Returns an empty list if *text* is not valid JSON or contains no proxies.
    """
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return []

    items: List = []
    if isinstance(data, dict):
        # geonode and similar: look for a top-level list value
        for key in ("data", "proxies", "proxy", "list", "result", "results"):
            if isinstance(data.get(key), list):
                items = data[key]
                break
    elif isinstance(data, list):
        items = data

    proxies: List[str] = []
    for item in items:
        if isinstance(item, str) and ":" in item:
            proxies.append(item.strip())
        elif isinstance(item, dict):
            ip = item.get("ip") or item.get("host") or item.get("address") or ""
            port = item.get("port") or item.get("Port") or ""
            if ip and port:
                proxies.append(f"{ip}:{port}")
    return proxies


async def _fetch_url(
    session: aiohttp.ClientSession,
    url: str,
) -> List[str]:
    """Download a proxy list from *url* and return ``IP:PORT`` strings.

    • JSON responses (detected by content-type or leading ``{``/``[``) are
      parsed with :func:`_parse_json_proxies` so that API sources like geonode
      work out of the box.
    • Plain-text responses are split line-by-line; only the first
      whitespace-separated token of each line is kept, which handles sources
      that append country / type metadata after the address
      (e.g. ``spys.me`` format ``1.2.3.4:8080 HTTP-N-N``).
    """
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=15),
            ssl=False,
        ) as resp:
            resp.raise_for_status()
            text = await resp.text(errors="replace")

            # ── JSON response ────────────────────────────────────────────
            ct = resp.headers.get("Content-Type", "")
            stripped = text.lstrip()
            if "json" in ct or stripped.startswith(("{", "[")):
                parsed = _parse_json_proxies(text)
                if parsed:
                    return parsed
                # If JSON parsing yields nothing fall through to text parsing

            # ── Plain-text response ──────────────────────────────────────
            result: List[str] = []
            for raw_line in text.splitlines():
                line = raw_line.strip()
                token = line.split()[0] if line else ""
                if token and not token.startswith("#"):
                    result.append(token)
            return result

    except Exception as exc:
        print(f"[!] Failed to fetch {url}: {exc}")
        return []


# ---------------------------------------------------------------------------
# High-level async operations
# ---------------------------------------------------------------------------

async def check_proxies(
    proxy_type: str,
    timeout: int = 10,
    max_workers: int = 500,
) -> None:
    """
    Read all unchecked proxies from offline_{proxy_type}.txt, test each one
    asynchronously (one checker URL per proxy, round-robin), then:
      - working → online_{proxy_type}.txt  as [TYPE]IP:PORT(Xms)[CC][Anon][ISP]
      - dead    → fallen_{proxy_type}.txt  as IP:PORT

    Check-once guarantee: any proxy already present in the online or fallen
    list is silently skipped — it will never be re-tested.

    Processes up to SCAN_BATCH_SIZE proxies per asyncio.gather call so that
    Task-object RAM stays bounded even with 800k+ input proxies.

    Uses one shared aiohttp.ClientSession per role (HTTP proxy checks / geo
    lookups) to avoid creating and tearing down a connection for every proxy.

    The offline list is bulk-cleared after all batches complete (O(1) instead
    of O(n²) per-worker removes).  Results are also written to SQLite if
    aiosqlite is installed.
    """
    await _init_db_once()

    offline = PROXY_LISTS_DIR / f"offline_{proxy_type}.txt"
    online  = PROXY_LISTS_DIR / f"online_{proxy_type}.txt"
    fallen  = PROXY_LISTS_DIR / f"fallen_{proxy_type}.txt"

    all_offline = read_proxies(offline)
    if not all_offline:
        print(f"[i] No unchecked proxies in {offline.name}")
        return

    # ── Check-once: skip anything already in online or fallen ──────────────
    already_done: set = {strip_decorations(e) for e in read_proxies(online)}
    already_done |= set(read_proxies(fallen))
    proxies = [p for p in all_offline if strip_decorations(p) not in already_done]
    skipped = len(all_offline) - len(proxies)
    if skipped:
        print(f"[i] Skipped {skipped} already-checked {proxy_type.upper()} proxies.")
    if not proxies:
        print(f"[i] Nothing new to check for {proxy_type.upper()}.")
        return

    total = len(proxies)
    print(f"[*] Checking {total} {proxy_type.upper()} proxies "
          f"(batches of {SCAN_BATCH_SIZE}, {max_workers} workers) …")

    # Shuffle the checker URL pool once; assign round-robin per proxy index.
    pool = CHECKER_URLS.copy()
    random.shuffle(pool)
    n_pool = len(pool)

    semaphore = asyncio.Semaphore(max_workers)
    counters: Dict[str, int] = {"checked": 0, "online": 0, "fallen": 0}
    checked_set: Set[str] = set()

    # Shared sessions — one TCPConnector each, reused across all workers.
    http_connector = aiohttp.TCPConnector(ssl=False, limit=0, ttl_dns_cache=300)
    geo_connector  = aiohttp.TCPConnector(ssl=False, limit=100, ttl_dns_cache=300)
    http_session = aiohttp.ClientSession(connector=http_connector)
    geo_session  = aiohttp.ClientSession(connector=geo_connector)

    n_flushed = 0
    try:
        # ── Process in batches to cap Task-object RAM ───────────────────────
        for batch_start in range(0, total, SCAN_BATCH_SIZE):
            batch = proxies[batch_start : batch_start + SCAN_BATCH_SIZE]
            tasks = [
                _check_worker(
                    raw=raw,
                    proxy_type=proxy_type,
                    checker_url=pool[(batch_start + i) % n_pool],
                    timeout=timeout,
                    semaphore=semaphore,
                    online=online,
                    fallen=fallen,
                    counters=counters,
                    http_session=http_session,
                    geo_session=geo_session,
                    checked_set=checked_set,
                )
                for i, raw in enumerate(batch)
            ]
            await asyncio.gather(*tasks)

            done = min(batch_start + SCAN_BATCH_SIZE, total)
            print(f"[*] Progress: {done}/{total} checked "
                  f"({counters['online']} online, {counters['fallen']} fallen)")

    finally:
        # Always close shared sessions and bulk-clear the offline list.
        await http_session.close()
        await geo_session.close()
        write_proxies(offline, [])   # bulk clear — O(1) vs O(n²) per-worker

        # Defence-in-depth: exclude any proxy that is already in the online or
        # fallen RAM cache even if checked_set was not updated (e.g. a
        # CancelledError between append_proxy and checked_set.add).
        already_committed: Set[str] = (
            {strip_decorations(e) for e in read_proxies(online)}
            | {strip_decorations(e) for e in read_proxies(fallen)}
        )
        unchecked = [
            strip_decorations(p) for p in proxies
            if strip_decorations(p) not in checked_set
            and strip_decorations(p) not in already_committed
        ]
        if unchecked:
            sntbs = Path(f"SNTBS-{proxy_type}.txt")
            existing_sntbs: Set[str] = set()
            if sntbs.exists():
                existing_sntbs = set(read_proxies(sntbs))
            existing_sntbs.update(unchecked)
            sntbs.write_text(
                "\n".join(sorted(existing_sntbs)) + "\n", encoding="utf-8"
            )
            print(
                f"[~] Saved {len(unchecked)} unchecked {proxy_type.upper()} "
                f"proxies to {sntbs.name}"
            )

        # Flush results to disk inside the finally block so they always reach
        # disk even when an exception (including CancelledError / Ctrl+C)
        # propagates out of check_proxies.
        n_flushed = flush_write_buffer()

    print(
        f"\n[*] {proxy_type.upper()} — checked {counters['checked']}: "
        f"{counters['online']} online, {counters['fallen']} fallen"
        + (f"  ({n_flushed} file(s) flushed)" if n_flushed else "")
        + ".\n"
    )


def add_from_list(source_file: str, proxy_type: str) -> None:
    """
    Import proxies from a local file into offline_{proxy_type}.txt.

    Proxies already confirmed online are skipped (they work fine).
    Proxies already in the offline queue are skipped (already pending).
    Proxies in the fallen list ARE re-imported — a source list re-listing
    them is a signal they may have recovered.  They are removed from fallen
    and placed back in offline for a fresh check.
    """
    src = Path(source_file)
    if not src.exists():
        sys.exit(f"[!] File not found: {source_file}")

    raw_lines = read_proxies(src)
    offline = PROXY_LISTS_DIR / f"offline_{proxy_type}.txt"
    online  = PROXY_LISTS_DIR / f"online_{proxy_type}.txt"
    fallen  = PROXY_LISTS_DIR / f"fallen_{proxy_type}.txt"

    existing_offline = set(read_proxies(offline))
    already_online   = {strip_decorations(e) for e in read_proxies(online)}
    fallen_entries   = read_proxies(fallen)
    already_fallen   = {strip_decorations(e) for e in fallen_entries}

    added = 0
    revived = 0
    to_remove_from_fallen: set = set()
    for line in raw_lines:
        parsed = parse_proxy(line)
        if parsed is None:
            print(f"[!] Skipping invalid entry: {line}")
            continue
        ip, port = parsed
        bare = f"{ip}:{port}"
        if bare in existing_offline or bare in already_online:
            continue
        existing_offline.add(bare)
        added += 1
        if bare in already_fallen:
            to_remove_from_fallen.add(bare)
            revived += 1

    write_proxies(offline, sorted(existing_offline))
    if to_remove_from_fallen:
        write_proxies(fallen, [
            e for e in fallen_entries
            if strip_decorations(e) not in to_remove_from_fallen
        ])
    msg = f"[*] Added {added} new {proxy_type.upper()} proxies to {offline.name}"
    if revived:
        msg += f"  ({revived} revived from fallen)"
    print(msg)


async def add_from_urls(urls_file: str, proxy_type: str) -> None:
    """
    Download proxy lists from each URL in *urls_file* concurrently and import
    them into offline_{proxy_type}.txt, ignoring duplicates.

    If *urls_file* contains ``https://github.com/{owner}/{repo}`` lines the
    GitHub API is queried to discover the raw .txt files for *proxy_type*
    automatically — no need to look up the raw URLs manually.
    """
    uf = Path(urls_file)
    if not uf.exists():
        sys.exit(f"[!] URLs file not found: {urls_file}")

    raw_lines = [
        ln.strip()
        for ln in uf.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
    if not raw_lines:
        print("[!] No URLs found in the file.")
        return

    # Split into plain fetch URLs and GitHub repo URLs.
    repo_urls = [u for u in raw_lines if _GITHUB_REPO_RE.match(u.rstrip("/"))]
    plain_urls = [u for u in raw_lines if not _GITHUB_REPO_RE.match(u.rstrip("/"))]

    expanded: List[str] = []
    if repo_urls:
        print(f"[*] Expanding {len(repo_urls)} GitHub repo URL(s) via API …")
        token = os.environ.get("GITHUB_TOKEN")
        connector = aiohttp.TCPConnector(ssl=False, limit=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            results = await asyncio.gather(
                *[_resolve_github_repo(u, session, token) for u in repo_urls],
                return_exceptions=True,
            )
        for repo_url, result in zip(repo_urls, results):
            if isinstance(result, Exception):
                print(f"[!] Exception resolving {repo_url}: {result}")
                continue
            expanded.extend(result.get(proxy_type, []))

    await _import_from_url_list(plain_urls + expanded, proxy_type)


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
    already_fallen = {strip_decorations(e) for e in read_proxies(fallen)}

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


def _parse_interval(value: str) -> int:
    """Convert a human-friendly interval string to an integer number of seconds.

    Accepted formats (case-insensitive):
      • Plain integer → treated as seconds  (e.g. ``"3600"``)
      • ``Nd``  → N days                    (e.g. ``"1d"``)
      • ``Nh``  → N hours                   (e.g. ``"2h"``)
      • ``Nm``  → N minutes                 (e.g. ``"30m"``)
      • ``Ns``  → N seconds                 (e.g. ``"90s"``)
    """
    s = value.strip().lower()
    multipliers = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    if s and s[-1] in multipliers:
        try:
            return int(s[:-1]) * multipliers[s[-1]]
        except ValueError:
            pass
    try:
        return int(s)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid interval '{value}'. "
            "Use an integer (seconds) or a suffix: 30m, 2h, 1d."
        )


async def run_pipeline(
    skip_fetch: bool = False,
    skip_repos: bool = False,
    skip_check: bool = False,
    token: Optional[str] = None,
    timeout: int = 10,
    workers: int = 200,
) -> None:
    """Run one full mining cycle for **all** proxy types.

    Pipeline:
      1. Fetch from 170+ built-in source URLs (``fetch`` step)
      2. Auto-discover raw files from 50 GitHub repos via the API (``repos`` step)
      3. Check every offline proxy and enrich online ones with geo + anonymity info
      4. Print updated statistics

    Any step can be skipped with the corresponding ``skip_*`` flag.
    """
    width = 62
    print("\n" + "=" * width)
    print("  ██████╗ ██████╗  ██████╗ ██╗  ██╗██╗   ██╗")
    print("  ██╔══██╗██╔══██╗██╔═══██╗╚██╗██╔╝╚██╗ ██╔╝")
    print("  ██████╔╝██████╔╝██║   ██║ ╚███╔╝  ╚████╔╝ ")
    print("  ██╔═══╝ ██╔══██╗██║   ██║ ██╔██╗   ╚██╔╝  ")
    print("  ██║     ██║  ██║╚██████╔╝██╔╝ ██╗   ██║   ")
    print("  ╚═╝     ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝   ╚═╝  ")
    print("  AUTO MINER  —  all 6 proxy types")
    print("=" * width + "\n")

    step = 1
    total_steps = sum([not skip_fetch, not skip_repos, not skip_check]) + 1  # +1 for stats

    if not skip_fetch:
        print(f"[STEP {step}/{total_steps}] Fetching from 170+ built-in sources …")
        step += 1
        await fetch_builtin("all")

    if not skip_repos:
        print(f"[STEP {step}/{total_steps}] Auto-discovering raw files from 50 GitHub repos …")
        step += 1
        await fetch_github_repos("all", token=token)

    if not skip_check:
        print(f"[STEP {step}/{total_steps}] Checking all offline proxies …")
        step += 1
        for ptype in PROXY_TYPES:
            await check_proxies(ptype, timeout=timeout, max_workers=workers)

    print(f"[STEP {step}/{total_steps}] Results:")
    show_stats()


async def run_loop(
    interval: int,
    skip_fetch: bool = False,
    skip_repos: bool = False,
    skip_check: bool = False,
    token: Optional[str] = None,
    timeout: int = 10,
    workers: int = 500,
) -> None:
    """Run :func:`run_pipeline` forever, sleeping *interval* seconds between
    cycles.  Every pipeline error is caught, logged, and retried with
    exponential backoff — the loop never dies on its own.  Ctrl+C stops it.
    """
    cycle = 0
    consecutive_errors = 0
    while True:
        cycle += 1
        print(f"\n{'─' * 62}")
        print(f"  RUN CYCLE #{cycle}  —  {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'─' * 62}")
        try:
            await run_pipeline(
                skip_fetch=skip_fetch,
                skip_repos=skip_repos,
                skip_check=skip_check,
                token=token,
                timeout=timeout,
                workers=workers,
            )
            consecutive_errors = 0
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            consecutive_errors += 1
            wait = min(60 * consecutive_errors, 600)   # up to 10 min backoff
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            msg = f"Pipeline error in cycle #{cycle}: {exc}"
            print(f"\n[!] [{ts}] {msg}  — retrying in {wait}s …", flush=True)
            _log.error("%s\n%s", msg, traceback.format_exc())
            await asyncio.sleep(wait)
            continue

        hrs, rem = divmod(interval, 3600)
        mins, secs = divmod(rem, 60)
        interval_str = (
            f"{hrs}h {mins}m {secs}s" if hrs
            else f"{mins}m {secs}s" if mins
            else f"{secs}s"
        )
        print(f"\n[*] Next cycle in {interval_str}  (Ctrl+C to stop)\n")
        await asyncio.sleep(interval)


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

    # -- run --  (also the default when no command is supplied)
    run_p = sub.add_parser(
        "run",
        help="Full auto pipeline: fetch all sources → check all proxies [DEFAULT]",
    )
    run_p.add_argument(
        "--loop",
        action="store_true",
        help="Repeat the mining cycle indefinitely (use with --interval)",
    )
    run_p.add_argument(
        "--interval",
        type=_parse_interval,
        default=3600,
        metavar="SECS|Nm|Nh",
        help=(
            "Time between mining cycles when --loop is set "
            "(default: 3600).  Accepts: 3600, 60m, 1h, 2d."
        ),
    )
    run_p.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip fetching from the 170+ built-in source URLs",
    )
    run_p.add_argument(
        "--skip-repos",
        action="store_true",
        help="Skip the GitHub repo auto-discovery step",
    )
    run_p.add_argument(
        "--skip-check",
        action="store_true",
        help="Fetch proxies only — skip the checking step",
    )
    run_p.add_argument(
        "--token",
        metavar="TOKEN",
        default=None,
        help="GitHub token for the repos step (overrides GITHUB_TOKEN env var)",
    )
    run_p.add_argument(
        "--timeout", "-T",
        type=int,
        default=10,
        metavar="SECS",
        help="Per-proxy check timeout in seconds (default: 10)",
    )
    run_p.add_argument(
        "--workers", "-w",
        type=int,
        default=200,
        metavar="N",
        help="Max concurrent proxy checks (default: 200)",
    )

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
        help="Proxy type: http | https | socks4 | socks4a | socks5 | socks5h",
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

    # -- repos --
    repos_p = sub.add_parser(
        "repos",
        help="Auto-discover raw proxy files from GitHub repos via the API",
    )
    repos_p.add_argument(
        "--type", "-t",
        choices=PROXY_TYPES + ["all"],
        default="all",
        metavar="TYPE",
        help="Which proxy type(s) to import (default: all)",
    )
    repos_p.add_argument(
        "--token",
        metavar="TOKEN",
        default=None,
        help=(
            "GitHub personal access token — overrides the GITHUB_TOKEN "
            "env var.  Raises the rate limit from 60 to 5 000 req/hour."
        ),
    )
    repos_p.add_argument(
        "--list-repos",
        action="store_true",
        help="Print the list of configured GitHub repo sources and exit",
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
    load_sntbs_files()  # import any SNTBS-{type}.txt files into the offline queue

    # Start the background write-buffer flusher (flushes dirty files every
    # BUFFER_FLUSH_INTERVAL seconds or when buffer exceeds BUFFER_FLUSH_SIZE_KB).
    start_buffer_flusher()

    try:
        # ── run (default when no command given) ────────────────────────────
        if args.command in (None, "run"):
            run_args = args if args.command == "run" else argparse.Namespace(
                loop=False, interval=3600,
                skip_fetch=False, skip_repos=False, skip_check=False,
                token=None, timeout=10, workers=200,
            )
            try:
                if run_args.loop:
                    await run_loop(
                        interval=run_args.interval,
                        skip_fetch=run_args.skip_fetch,
                        skip_repos=run_args.skip_repos,
                        skip_check=run_args.skip_check,
                        token=run_args.token,
                        timeout=run_args.timeout,
                        workers=run_args.workers,
                    )
                else:
                    await run_pipeline(
                        skip_fetch=run_args.skip_fetch,
                        skip_repos=run_args.skip_repos,
                        skip_check=run_args.skip_check,
                        token=run_args.token,
                        timeout=run_args.timeout,
                        workers=run_args.workers,
                    )
            except KeyboardInterrupt:
                print("\n[*] Run stopped by user.")

        elif args.command == "add":
            if args.list:
                add_from_list(args.list, args.type)
            else:
                await add_from_urls(args.urls, args.type)

        elif args.command == "fetch":
            await fetch_builtin(args.type)

        elif args.command == "repos":
            if args.list_repos:
                print(f"\n=== Configured GitHub Repository Sources ({len(GITHUB_REPO_SOURCES)}) ===\n")
                for url in GITHUB_REPO_SOURCES:
                    print(f"  {url}")
                print()
            else:
                await fetch_github_repos(args.type, token=args.token)

        elif args.command == "check":
            types = PROXY_TYPES if args.type == "all" else [args.type]
            for ptype in types:
                await check_proxies(ptype, timeout=args.timeout, max_workers=args.workers)

        elif args.command == "stats":
            show_stats()

    finally:
        # Always flush whatever is still in the buffer before exiting,
        # so no data is lost on normal exit or Ctrl+C.
        n = flush_write_buffer()
        if n:
            print(f"[~] Final flush: {n} file(s) written to disk.")
        if _DB_ENABLED:
            try:
                await _proxy_db.close_db()
            except Exception:
                pass


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
