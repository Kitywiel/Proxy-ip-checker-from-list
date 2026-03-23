#!/usr/bin/env python3
"""
tor_service.py — Launch a Tor hidden service that exposes the Proxy IP Checker
web dashboard as a .onion address.

Prerequisites
─────────────
  1. Install Tor:
       Linux/Debian:  sudo apt-get install tor
       macOS:         brew install tor
       Windows:       https://www.torproject.org/download/tor/
  2. Install Python deps:
       pip install stem flask aiosqlite

How it works
────────────
  1. Starts (or connects to an already running) Tor process.
  2. Uses stem's Controller API to create an ephemeral hidden service that
     forwards requests on port 80 to localhost:<web_port>.
  3. Prints the .onion address.
  4. Keeps running until you press Ctrl+C.

The web dashboard (web_server.py) must be running at the same time.
You can start both together with the --start-web flag.

Usage
─────
  # Start web server first, then expose via Tor:
  python web_server.py &
  python tor_service.py

  # Start both in one command:
  python tor_service.py --start-web

  # Custom web port:
  python tor_service.py --web-port 9090 --start-web

  # Persist the hidden service key so the .onion address stays the same:
  python tor_service.py --key-file my_hidden_service.key

Manual torrc alternative
────────────────────────
If stem is not available, add the following to your torrc
(usually /etc/tor/torrc) and restart Tor:

  HiddenServiceDir  /var/lib/tor/proxy_checker/
  HiddenServicePort 80 127.0.0.1:8080

Then read the hostname:
  cat /var/lib/tor/proxy_checker/hostname
"""

import argparse
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_stem() -> None:
    try:
        import stem  # noqa: F401
    except ImportError:
        print(textwrap.dedent("""
        [!] stem is not installed.  Install it with:

              pip install stem

        If you prefer to configure Tor manually, add these lines to your
        torrc file (usually /etc/tor/torrc) and restart Tor:

          HiddenServiceDir  /var/lib/tor/proxy_checker/
          HiddenServicePort 80 127.0.0.1:8080

        Then find your .onion address with:
          cat /var/lib/tor/proxy_checker/hostname
        """).strip())
        sys.exit(1)


def _tor_in_path() -> bool:
    import shutil
    return shutil.which("tor") is not None


def _start_web_server(port: int) -> subprocess.Popen:
    """Launch web_server.py as a background subprocess."""
    web_script = Path(__file__).parent / "web_server.py"
    if not web_script.exists():
        print("[!] web_server.py not found — cannot start web server automatically.")
        sys.exit(1)
    print(f"[*] Starting web server on port {port} …")
    proc = subprocess.Popen(
        [sys.executable, str(web_script), "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)  # give Flask a moment to bind
    return proc


def _load_or_generate_key(key_file: Path):
    """
    Load a persisted ED25519 private key, or return None to generate a fresh one.
    """
    from stem.control import KeyType  # type: ignore
    if key_file and key_file.exists():
        raw = key_file.read_text().strip()
        # stem stores keys as "ED25519-V3:<base64>"
        if ":" in raw:
            ktype_str, kdata = raw.split(":", 1)
            return (KeyType.NEW if ktype_str == "NEW" else ktype_str, kdata)
    return ("NEW", "ED25519-V3")


def _save_key(key_file: Path, key_content: str) -> None:
    key_file.write_text(key_content)
    key_file.chmod(0o600)
    print(f"[*] Hidden service key saved to:  {key_file}")


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def run(web_port: int = 8080, key_file: Path = None, start_web: bool = False) -> None:
    _require_stem()

    from stem.control import Controller  # type: ignore
    from stem.process import launch_tor_with_config  # type: ignore

    web_proc: subprocess.Popen = None
    tor_process = None

    try:
        # ── optionally start the Flask web server ──────────────────────────
        if start_web:
            web_proc = _start_web_server(web_port)

        # ── start Tor (or connect to an existing Tor) ──────────────────────
        tor_ctrl_port = 9051
        tor_socks_port = 9050

        print("[*] Launching Tor …  (this may take up to 60 s)")

        if _tor_in_path():
            tor_process = launch_tor_with_config(
                config={
                    "ControlPort": str(tor_ctrl_port),
                    "SOCKSPort":   str(tor_socks_port),
                    "Log":         "notice stdout",
                },
                init_msg_handler=lambda line: (
                    print(f"    {line}", end="") if "Bootstrapped" in line else None
                ),
            )
            print()  # newline after bootstrap output
        else:
            print(
                "[!] 'tor' binary not found in PATH.\n"
                "    Please install Tor first:\n"
                "      Debian/Ubuntu: sudo apt-get install tor\n"
                "      macOS:         brew install tor\n"
                "      Windows:       https://www.torproject.org/download/tor/"
            )
            sys.exit(1)

        # ── connect controller and create hidden service ───────────────────
        with Controller.from_port(port=tor_ctrl_port) as ctrl:
            ctrl.authenticate()

            # Load persisted key or generate a fresh ephemeral one
            key_type, key_content = _load_or_generate_key(key_file) \
                if key_file else ("NEW", "ED25519-V3")

            response = ctrl.create_ephemeral_hidden_service(
                ports={80: web_port},
                key_type=key_type,
                key_content=key_content,
                await_publication=True,
            )

            onion_address = f"{response.service_id}.onion"

            # Persist the key for next run (same .onion address)
            if key_file and not key_file.exists():
                _save_key(key_file, f"{response.private_key_type}:{response.private_key}")

            print()
            print("=" * 62)
            print("  ✓  Tor hidden service is LIVE")
            print()
            print(f"  .onion address:  http://{onion_address}/")
            print()
            print("  Open in Tor Browser:  http://" + onion_address + "/")
            print("  Dashboard API:        http://" + onion_address + "/api/stats")
            print("  Online proxies txt:   http://" + onion_address + "/api/proxies/online.txt")
            print()
            print("  Press Ctrl+C to stop.")
            print("=" * 62)

            # ── keep alive ─────────────────────────────────────────────────
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                print("\n[*] Shutting down hidden service …")

    finally:
        if tor_process is not None:
            tor_process.kill()
            print("[*] Tor process stopped.")
        if web_proc is not None:
            web_proc.terminate()
            print("[*] Web server stopped.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Expose the Proxy IP Checker web UI as a Tor hidden service"
    )
    p.add_argument(
        "--web-port", type=int, default=8080, metavar="PORT",
        help="Port the web server is listening on (default: 8080)",
    )
    p.add_argument(
        "--key-file", metavar="FILE", default=None,
        help=(
            "Path to persist the hidden service private key so the same "
            ".onion address is reused across restarts.  If the file does "
            "not exist it will be created on first run."
        ),
    )
    p.add_argument(
        "--start-web", action="store_true",
        help="Also start web_server.py automatically before launching Tor",
    )
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()
    key_file = Path(args.key_file) if args.key_file else None
    run(web_port=args.web_port, key_file=key_file, start_web=args.start_web)
