import http.server
import json
import logging
import logging.handlers
import os
import socketserver
import sys
import threading
import time
import argparse
from http import HTTPStatus

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("JellyfinLibraryScanManager")

# Log to file so crashes leave evidence when running as a PyInstaller exe
try:
    _log_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
    _fh = logging.handlers.RotatingFileHandler(
        os.path.join(_log_dir, "JellyfinLibraryScanManager.log"),
        maxBytes=5 * 1024 * 1024, backupCount=3,
    )
    _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logging.getLogger().addHandler(_fh)
except Exception:
    pass


class JellyfinClient:
    """Talks to the Jellyfin API to discover libraries and trigger refreshes."""

    def __init__(self, address, api_key):
        self.address = address.rstrip("/")
        self.api_key = api_key
        self._libraries = []          # [{id, name, locations, collection_type}]
        self._lock = threading.Lock()
        self._last_fetch = 0
        self._cache_ttl = 300         # re-fetch library list every 5 min

    # helpers

    def _url(self, endpoint):
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return f"{self.address}{endpoint}"

    @staticmethod
    def _normalize(path):
        """Lowercase, forward-slash, no trailing slash."""
        return path.replace("\\", "/").rstrip("/").lower()

    def _request(self, method, endpoint, **kwargs):
        headers = kwargs.pop("headers", {})
        headers.setdefault("Authorization", f'MediaBrowser Token="{self.api_key}"')
        headers.setdefault("Content-Type", "application/json")
        kwargs["headers"] = headers
        kwargs.setdefault("timeout", 15)
        url = self._url(endpoint)
        try:
            resp = requests.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            logger.error("Jellyfin %s %s failed: %s", method, endpoint, exc)
            return None

    # library discovery

    @staticmethod
    def _folder_name(path):
        """Return the last path component, normalized to lowercase."""
        return path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1].lower()

    def fetch_libraries(self):
        """GET /Library/VirtualFolders and cache the result."""
        resp = self._request("GET", "/Library/VirtualFolders")
        if resp is None:
            return False

        try:
            data = resp.json()
        except (ValueError, TypeError):
            logger.error("Jellyfin /Library/VirtualFolders returned invalid JSON")
            return False

        libraries = []
        for folder in data:
            locations = folder.get("Locations", [])
            libraries.append({
                "id": folder.get("ItemId"),
                "name": folder.get("Name"),
                "locations": locations,
                "folder_names": [self._folder_name(p) for p in locations],
                "collection_type": folder.get("CollectionType", ""),
            })

        with self._lock:
            self._libraries = libraries
            self._last_fetch = time.time()

        logger.info("Loaded %d Jellyfin libraries:", len(libraries))
        for lib in libraries:
            logger.info("  %s (%s) folders: %s", lib["name"], lib["collection_type"] or "mixed", lib["folder_names"])
        return True

    def _ensure_fresh(self):
        with self._lock:
            stale = time.time() - self._last_fetch > self._cache_ttl
        if stale:
            self.fetch_libraries()

    def get_libraries(self):
        self._ensure_fresh()
        with self._lock:
            return list(self._libraries)

    #folder name → library mapping

    def libraries_for_folder_name(self, folder_name):
        """Return every library that has a source folder matching *folder_name*."""
        norm = folder_name.lower()
        matched = []
        for lib in self.get_libraries():
            if norm in lib["folder_names"]:
                matched.append(lib)
        return matched

    # refresh

    def refresh_library(self, library_id):
        """POST /Items/{id}/Refresh to scan a single library for changes."""
        resp = self._request(
            "POST",
            f"/Items/{library_id}/Refresh"
            "?metadataRefreshMode=Default&imageRefreshMode=Default",
        )
        return resp is not None

    def refresh_all(self):
        """POST /Library/Refresh — full library scan (legacy fallback)."""
        resp = self._request("POST", "/Library/Refresh")
        return resp is not None

    def get_library_scan_status(self):
        """Return {library_id: bool} — True if the library is currently scanning.

        Uses RefreshProgress from /Library/VirtualFolders: when it is not None
        the library has an active scan (global or targeted).
        """
        resp = self._request("GET", "/Library/VirtualFolders")
        if resp is None:
            return None
        try:
            data = resp.json()
        except (ValueError, TypeError):
            logger.error("Jellyfin /Library/VirtualFolders returned invalid JSON (scan status)")
            return None
        result = {}
        for folder in data:
            item_id = folder.get("ItemId")
            progress = folder.get("RefreshProgress")
            result[item_id] = progress is not None
        return result


# flag style refresh manager
class LibraryRefreshManager:
    """Per-library dirty-flag queue drained by a polling loop.

    When a refresh is requested for a library that is already scanning, the
    request is collapsed into a single "dirty" flag.  A background thread
    polls Jellyfin for scan completion and fires one follow-up refresh per
    dirty library once the current scan finishes.
    """

    def __init__(self, client, poll_interval=3, hold_seconds=5):
        self.client = client
        self.poll_interval = poll_interval
        self._hold_seconds = hold_seconds
        self._lock = threading.Lock()
        # library_id -> {refreshing: bool, dirty: bool, name: str, triggered_at: float}
        self._state = {}
        self._stop = threading.Event()

    # -- called by webhook handler

    def request_refresh(self, library_id, library_name):
        """Request a scan for *library_id*.

        If the library is not currently scanning, fire immediately.
        If it is, set the dirty flag so the poll loop fires a follow-up.
        """
        fire = False
        with self._lock:
            state = self._state.get(library_id)
            if state is None:
                state = {
                    "refreshing": False,
                    "dirty": False,
                    "name": library_name,
                    "triggered_at": 0,
                }
                self._state[library_id] = state

            if state["refreshing"]:
                state["dirty"] = True
                logger.info("[%s] Scan in progress — marked dirty", library_name)
                return "queued"
            else:
                state["refreshing"] = True
                state["dirty"] = False
                state["triggered_at"] = time.time()
                fire = True

        if fire:
            ok = self.client.refresh_library(library_id)
            if ok:
                logger.info("[%s] Refresh triggered", library_name)
                return "triggered"
            else:
                with self._lock:
                    self._state[library_id]["refreshing"] = False
                logger.error("[%s] Refresh FAILED", library_name)
                return "failed"

    # -- background poll loop
    def poll_loop(self):
        """Monitor scan status and drain the dirty queue."""
        while not self._stop.is_set():
            self._stop.wait(self.poll_interval)
            if self._stop.is_set():
                break

            try:
                self._poll_tick()
            except Exception:
                logger.exception("Unexpected error in poll loop (will retry next cycle)")

    def _poll_tick(self):
        """Single iteration of the scan-status poll."""
        with self._lock:
            has_active = any(s["refreshing"] for s in self._state.values())
        if not has_active:
            return

        status = self.client.get_library_scan_status()
        if status is None:
            return  # API error — retry next cycle

        now = time.time()
        to_fire = []

        with self._lock:
            for lib_id, state in self._state.items():
                if not state["refreshing"]:
                    continue

                is_scanning = status.get(lib_id, False)
                hold_elapsed = (now - state["triggered_at"]) >= self._hold_seconds

                if is_scanning or not hold_elapsed:
                    continue

                # Scan finished for this library
                if state["dirty"]:
                    state["dirty"] = False
                    state["triggered_at"] = now
                    to_fire.append((lib_id, state["name"]))
                    logger.info(
                        "[%s] Scan complete — dirty flag set, triggering rescan",
                        state["name"],
                    )
                else:
                    state["refreshing"] = False
                    logger.info("[%s] Scan complete, queue clear", state["name"])

        # Fire queued rescans outside the lock
        for lib_id, name in to_fire:
            ok = self.client.refresh_library(lib_id)
            if not ok:
                logger.error("[%s] Queued rescan FAILED", name)
                with self._lock:
                    if lib_id in self._state:
                        self._state[lib_id]["refreshing"] = False

    def stop(self):
        self._stop.set()

    def get_status(self):
        """Snapshot of current state for the debug endpoint."""
        with self._lock:
            return {lid: dict(s) for lid, s in self._state.items()}


# Webhook payload parsing
def _parent_folder_name(path):
    """Extract the last component of the parent directory, lowercased.

    Sonarr:  series.path  = /data/autotelevision/Show Name  → autotelevision
    Radarr:  movie.folderPath = /data/automovies/Movie (2024) → automovies
    """
    norm = path.replace("\\", "/").rstrip("/")
    parent = norm.rsplit("/", 1)[0] if "/" in norm else norm
    return parent.rsplit("/", 1)[-1].lower()


def extract_root_folder_name(payload):
    """Derive the root/library folder name from a Sonarr or Radarr webhook.

    The webhook provides the series/movie path (e.g. /data/autotv/Show Name).
    The parent of that path is the root folder, and its last component is the
    folder name we match against Jellyfin library source folder names.
    """
    event_type = payload.get("eventType", "Unknown")

    # Sonarr — series.path is the series directory; parent is the root folder
    series = payload.get("series")
    if series:
        path = series.get("path")
        if path:
            name = _parent_folder_name(path)
            logger.info("Sonarr [%s] series.path = %s -> root folder: %s", event_type, path, name)
            return name

    # Radarr — movie.folderPath is the movie directory; parent is the root folder
    movie = payload.get("movie")
    if movie:
        path = movie.get("folderPath") or movie.get("path")
        if path:
            name = _parent_folder_name(path)
            logger.info("Radarr [%s] movie.folderPath = %s -> root folder: %s", event_type, path, name)
            return name

    # Fallback: derive from file path (go up two levels: file -> media dir -> root)
    for key in ("episodeFile", "movieFile"):
        file_obj = payload.get(key)
        if file_obj:
            fp = file_obj.get("path")
            if fp:
                # file -> series/movie dir -> root folder
                media_dir = fp.replace("\\", "/").rstrip("/").rsplit("/", 1)[0]
                name = _parent_folder_name(media_dir)
                logger.info("Derived root folder from %s.path: %s", key, name)
                return name

    # Bulk import
    dest = payload.get("destinationPath")
    if dest:
        name = _parent_folder_name(dest)
        logger.info("Root folder from destinationPath: %s", name)
        return name

    logger.warning("Could not extract root folder from %s webhook", event_type)
    return None


# HTTP handler
class WebhookHandler(http.server.BaseHTTPRequestHandler):

    # Attached to the class by main() before the server starts.
    jellyfin: JellyfinClient
    manager: LibraryRefreshManager

    def log_message(self, fmt, *args):
        logger.info("%s - %s", self.client_address[0], fmt % args)

    def do_GET(self):
        try:
            self._handle_get()
        except Exception:
            logger.exception("Unexpected error handling GET request")
            try:
                self._respond("Internal server error.", HTTPStatus.INTERNAL_SERVER_ERROR)
            except Exception:
                pass

    def _handle_get(self):
        path = self.path.lower().rstrip("/")

        if path in ("/libraries", "/status"):
            self.jellyfin.fetch_libraries()
            body = json.dumps(
                {"libraries": self.jellyfin.get_libraries(), "queue": self.manager.get_status()},
                indent=2,
                default=str,
            )
            self._respond(body, HTTPStatus.OK, "application/json")

        elif "jellyfin" in path:
            ok = self.jellyfin.refresh_all()
            msg = "Full library refresh triggered." if ok else "Failed to trigger full refresh."
            self._respond(msg, HTTPStatus.OK)

        else:
            self._respond(
                "Endpoints:\n"
                "  POST /          — Sonarr / Radarr webhook (targeted refresh)\n"
                "  GET  /libraries  — show library mapping + queue status\n"
                "  GET  /jellyfin   — trigger a full library refresh\n",
                HTTPStatus.OK,
            )

    def do_POST(self):
        try:
            self._handle_post()
        except Exception:
            logger.exception("Unexpected error handling POST request")
            try:
                self._respond("Internal server error.", HTTPStatus.INTERNAL_SERVER_ERROR)
            except Exception:
                pass

    def _handle_post(self):
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self._respond("Empty body.", HTTPStatus.BAD_REQUEST)
            return

        try:
            raw = self.rfile.read(content_length)
            payload = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Bad webhook body: %s", exc)
            self._respond(f"Invalid JSON: {exc}", HTTPStatus.BAD_REQUEST)
            return

        event_type = payload.get("eventType", "Unknown")
        logger.info("Webhook received: eventType=%s", event_type)

        if event_type.lower() == "test":
            logger.info("Test webhook OK.")
            self._respond("Test webhook received.", HTTPStatus.OK)
            return

        folder_name = extract_root_folder_name(payload)
        if not folder_name:
            self._respond("Acknowledged (no root folder to map).", HTTPStatus.OK)
            return

        matched = self.jellyfin.libraries_for_folder_name(folder_name)
        if not matched:
            logger.warning("No Jellyfin library matched folder name: %s — falling back to full scan", folder_name)
            ok = self.jellyfin.refresh_all()
            msg = f"No library matched '{folder_name}', full refresh {'triggered' if ok else 'FAILED'}"
            self._respond(msg, HTTPStatus.OK)
            return

        lines = []
        for lib in matched:
            result = self.manager.request_refresh(lib["id"], lib["name"])
            lines.append(f"{lib['name']}: {result}")

        self._respond("\n".join(lines), HTTPStatus.OK)

    def _respond(self, body, status, content_type="text/plain"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))


def _run_service(config):
    """One run of the service — raises on failure so the retry loop can restart."""
    jellyfin = JellyfinClient(config.address, config.apikey)
    manager = LibraryRefreshManager(jellyfin, config.poll_interval, config.hold_delay)

    logger.info("Connecting to Jellyfin at %s ...", config.address)
    if not jellyfin.fetch_libraries():
        raise ConnectionError("Could not reach Jellyfin — check --address and --apikey")

    WebhookHandler.jellyfin = jellyfin
    WebhookHandler.manager = manager

    logger.info("--- Configuration ---")
    logger.info("Jellyfin : %s", config.address)
    logger.info("Listen   : %s:%d", config.host, config.port)
    logger.info("Poll     : every %ds", config.poll_interval)
    logger.info("---")
    logger.info("Point your Sonarr / Radarr webhook to: http://<this-host>:%d/", config.port)
    logger.info("---")

    poll_thread = threading.Thread(target=manager.poll_loop, daemon=True)
    poll_thread.start()

    try:
        socketserver.TCPServer.allow_reuse_address = True
        with socketserver.TCPServer((config.host, config.port), WebhookHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        manager.stop()


def _run_forever(config):
    """Retry loop — restarts the service on any failure."""
    while True:
        try:
            _run_service(config)
            return  # clean shutdown (KeyboardInterrupt caught inside)
        except KeyboardInterrupt:
            logger.info("Shutting down.")
            return
        except Exception:
            logger.exception("Service error — restarting in 10s")
            try:
                time.sleep(10)
            except KeyboardInterrupt:
                logger.info("Shutting down.")
                return


def main():
    try:
        parser = argparse.ArgumentParser(
            description=(
                "Receives Sonarr / Radarr webhooks and triggers targeted Jellyfin "
                "library refreshes.  Only the libraries whose source folders match "
                "the imported media path are refreshed.  Concurrent requests for the "
                "same library are collapsed into a single follow-up scan."
            ),
        )
        parser.add_argument("-a", "--address", required=True,
                            help="Jellyfin base URL (e.g. http://192.168.4.4:8096)")
        parser.add_argument("-k", "--apikey", required=True,
                            help="Jellyfin API key")
        parser.add_argument("-H", "--host", default="0.0.0.0",
                            help="Listen address (default: 0.0.0.0)")
        parser.add_argument("-p", "--port", type=int, default=5000,
                            help="Listen port (default: 5000)")
        parser.add_argument("-i", "--poll-interval", type=int, default=3,
                            help="Seconds between scan-status polls (default: 3)")
        parser.add_argument("-d", "--hold-delay", type=int, default=5,
                            help="Seconds to wait after triggering a refresh before trusting "
                                 "scan status from the API (default: 5)")

        config = parser.parse_args()
        _run_forever(config)
    except Exception:
        logger.exception("Fatal error")
        if getattr(sys, "frozen", False):
            try:
                input("Press Enter to exit...")
            except Exception:
                pass
        sys.exit(1)


if __name__ == "__main__":
    main()
