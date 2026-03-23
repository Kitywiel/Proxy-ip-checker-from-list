#!/usr/bin/env python3
"""
tor_service.py — Launch a Tor hidden service that exposes the Proxy IP Checker
web dashboard as a .onion address.

Zero setup — the script auto-installs the ``stem`` Python library if it is
missing.  Tor itself must be installed separately; see the Prerequisites
section below for per-platform instructions.

Prerequisites
─────────────
  Tor (the binary) must be installed:
    Linux/Debian:  sudo apt-get install tor
    macOS:         brew install tor
    Windows:       Download the Expert Bundle from
                   https://www.torproject.org/download/tor/
                   and either add the Tor\\ folder to PATH or place tor.exe
                   next to this script.

  stem (Python library) — installed automatically on first run.

How it works
────────────
  1. Auto-installs stem if it is missing.
  2. Tries to connect to an already-running Tor instance (ports 9051 / 9151)
     so Tor Browser or a system Tor service can be reused without launching a
     second process.
  3. If no Tor is running, locates the tor binary (PATH + common Windows
     paths) and launches it.
  4. Uses stem's Controller API to create an ephemeral hidden service that
     forwards requests on port 80 to localhost:<web_port>.
  5. Prints the .onion address.
  6. Keeps running until you press Ctrl+C.

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
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Zero-setup: auto-install stem
# ---------------------------------------------------------------------------

def _bootstrap_stem() -> None:
    """Install stem using the current interpreter if it is not already present."""
    try:
        import stem  # noqa: F401
        return
    except ImportError:
        pass
    print("[*] stem not found — auto-installing stem …", flush=True)
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "stem>=1.8.0"]
        )
        print("[*] stem installed — continuing.", flush=True)
    except subprocess.CalledProcessError as exc:
        print(
            "[!] Failed to install stem automatically.\n"
            f"    Error: {exc}\n"
            "    Please install it manually with:  pip install stem",
            file=sys.stderr,
        )
        sys.exit(1)


_bootstrap_stem()

# ---------------------------------------------------------------------------
# Tor binary discovery
# ---------------------------------------------------------------------------

# Common Windows installation paths for the Tor Expert Bundle and Tor Browser.
_WINDOWS_TOR_PATHS = [
    # Expert Bundle extracted next to this script
    Path(__file__).parent / "Tor" / "tor.exe",
    Path(__file__).parent / "tor-win64" / "Tor" / "tor.exe",
    Path(__file__).parent / "tor.exe",
    # Tor Browser (default install locations)
    Path(os.environ.get("PROGRAMFILES", r"C:\Program Files"))
    / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
    Path(os.environ.get("LOCALAPPDATA", r"C:\Users\Default\AppData\Local"))
    / "Programs" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
    Path(os.path.expanduser("~")) / "Desktop" / "Tor Browser" / "Browser"
    / "TorBrowser" / "Tor" / "tor.exe",
]


def _find_tor_binary() -> str:
    """Return the path to the tor binary, or raise FileNotFoundError."""
    # 1. Check PATH first (works on Linux, macOS, and Windows if Tor is in PATH).
    tor_in_path = shutil.which("tor")
    if tor_in_path:
        return tor_in_path

    # 2. Check well-known Windows locations.
    for candidate in _WINDOWS_TOR_PATHS:
        if candidate.exists():
            return str(candidate)

    raise FileNotFoundError(
        "'tor' binary not found.  Please install Tor:\n"
        "  Debian/Ubuntu: sudo apt-get install tor\n"
        "  macOS:         brew install tor\n"
        "  Windows:       https://www.torproject.org/download/tor/\n"
        "                 (download the Expert Bundle and place the Tor\\ folder\n"
        "                  next to tor_service.py, or add it to your PATH)"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
    from stem.control import Controller  # type: ignore
    from stem.process import launch_tor_with_config  # type: ignore

    web_proc: subprocess.Popen = None
    tor_process = None

    try:
        # ── optionally start the Flask web server ──────────────────────────
        if start_web:
            web_proc = _start_web_server(web_port)

        # ── locate the Tor control port ────────────────────────────────────
        tor_ctrl_port = 9051
        tor_socks_port = 9050
        using_existing = False

        # Try to connect to an already-running Tor instance (standard port or
        # Tor Browser's port) before launching a new process.
        for candidate_port in (9051, 9151):
            try:
                ctrl_test = Controller.from_port(port=candidate_port)
                ctrl_test.authenticate()
                ctrl_test.close()
                tor_ctrl_port = candidate_port
                using_existing = True
                print(f"[*] Found existing Tor on control port {tor_ctrl_port}.")
                break
            except Exception:
                pass

        if not using_existing:
            print("[*] Launching Tor …  (this may take up to 60 s)")
            try:
                tor_binary = _find_tor_binary()
            except FileNotFoundError as exc:
                print(f"[!] {exc}")
                sys.exit(1)

            try:
                tor_process = launch_tor_with_config(
                    tor_cmd=tor_binary,
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
            except Exception as exc:
                print(
                    f"[!] Failed to launch Tor: {exc}\n\n"
                    "    Troubleshooting:\n"
                    "      • Make sure Tor is installed and working.\n"
                    "      • Try starting Tor manually, then re-run this script\n"
                    "        (it will connect to the running Tor automatically).\n"
                    "      • On Windows, run as Administrator if the control\n"
                    "        port cannot be bound."
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

