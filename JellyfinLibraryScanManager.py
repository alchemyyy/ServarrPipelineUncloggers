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
from urllib.parse import quote

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



def invalidate_nfs_cache(path):
    """Force the NFS client to drop its cached directory listing for *path*.

    NFS attribute caching (actimeo/acdirmin/acdirmax) tells the kernel to
    reuse cached stat() and readdir() results for a configured duration.
    When Sonarr/Radarr writes a new file, the NFS client may still serve a
    stale directory listing for up to actimeo seconds — meaning Jellyfin's
    ValidateChildren() won't see the new file even though it exists on disk.

    Performing an os.listdir() on the target directory (and its parent) forces
    the NFS client to issue a fresh READDIR RPC to the server, bypassing the
    attribute cache for that specific directory.  This is surgically targeted:
    it only invalidates the one directory we care about, leaving the rest of
    the NFS cache intact for performance.

    This must run on the same host where the NFS mount is active — i.e. the
    machine running Jellyfin (or at least one that shares the same NFS mount).
    If this script runs on a different host than the NFS client, this call is
    a no-op (the directory won't exist locally).
    """
    for p in (path, os.path.dirname(path)):
        if not p:
            continue
        try:
            os.listdir(p)
            logger.debug("NFS cache invalidated: %s", p)
        except OSError as exc:
            logger.debug("NFS cache invalidation skipped for %s: %s", p, exc)


class JellyfinClient:
    """Talks to the Jellyfin API — item-level targeted refreshes."""

    def __init__(self, address, api_key):
        self.address = address.rstrip("/")
        self.api_key = api_key
        self._libraries = []          # [{id, name, locations, collection_type}]
        self._lock = threading.Lock()
        self._last_fetch = 0
        self._cache_ttl = 300

    # -- helpers --

    def _url(self, endpoint):
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return f"{self.address}{endpoint}"

    @staticmethod
    def _normalize(path):
        return path.replace("\\", "/").rstrip("/").lower()

    def _request(self, method, endpoint, **kwargs):
        headers = kwargs.pop("headers", {})
        headers.setdefault("Authorization", f'MediaBrowser Token="{self.api_key}"')
        headers.setdefault("Content-Type", "application/json")
        kwargs["headers"] = headers
        kwargs.setdefault("timeout", 30)
        url = self._url(endpoint)
        try:
            resp = requests.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            logger.error("Jellyfin %s %s failed: %s", method, endpoint, exc)
            return None

    # -- startup precheck --

    def precheck_configuration(self):
        """GET system config, force-set LibraryScanFanoutConcurrency, POST back."""
        resp = self._request("GET", "/System/Configuration")
        if resp is None:
            logger.error("Precheck: could not read system configuration")
            return False
        try:
            config = resp.json()
        except (ValueError, TypeError):
            logger.error("Precheck: invalid JSON from /System/Configuration")
            return False

        changed = False

        current_delay = config.get("LibraryMonitorDelay", 60)
        if current_delay != 0:
            logger.info("Precheck: LibraryMonitorDelay %s -> 0 (instant FileSystemWatcher response)", current_delay)
            config["LibraryMonitorDelay"] = 0
            changed = True
        else:
            logger.info("Precheck: LibraryMonitorDelay already 0")

        current_update = config.get("LibraryUpdateDuration", 30)
        if current_update != 0:
            logger.info("Precheck: LibraryUpdateDuration %s -> 0 (instant UI notification)", current_update)
            config["LibraryUpdateDuration"] = 0
            changed = True
        else:
            logger.info("Precheck: LibraryUpdateDuration already 0")

        if changed:
            resp = self._request("POST", "/System/Configuration", json=config)
            if resp is None:
                logger.error("Precheck: failed to update system configuration")
                return False
            logger.info("Precheck: system configuration updated")
        return True

    # -- library discovery --

    @staticmethod
    def _folder_name(path):
        return path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1].lower()

    def fetch_libraries(self):
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

    def libraries_for_folder_name(self, folder_name):
        norm = folder_name.lower()
        matched = []
        for lib in self.get_libraries():
            if norm in lib["folder_names"]:
                matched.append(lib)
        return matched

    def find_root_folder_item(self, library_id, folder_name):
        """Find the item ID of a specific physical root folder within a library.

        A library (CollectionFolder) can have multiple source locations.
        Each one is a Folder item that is a direct child of the library.
        Refreshing just that folder is much faster than the entire library.
        """
        params = {
            "ParentId": library_id,
            "fields": "Path",
        }
        resp = self._request("GET", "/Items", params=params)
        if resp is None:
            return None
        try:
            data = resp.json()
        except (ValueError, TypeError):
            return None

        norm = folder_name.lower()
        for item in data.get("Items", []):
            item_path = item.get("Path", "")
            if self._folder_name(item_path) == norm:
                logger.info("Found root folder item: %s -> id=%s", item_path, item["Id"])
                return item
        return None

    # -- item lookup --

    def find_item(self, title, item_type, provider_ids=None, parent_id=None):
        """Search Jellyfin for a Series or Movie by title, verified by provider IDs.

        Searches by title via the /Items endpoint, then matches ONLY when certain:
          1. Provider ID match (Tvdb, Tmdb, or Imdb) -- guaranteed correct
          2. Exact title match (case-insensitive)

        If neither matches, returns None so the caller falls back to a library-level
        refresh.  We never guess — refreshing the wrong item is worse than a slightly
        slower library scan.

        provider_ids: dict like {"Tvdb": "297271", "Tmdb": "12345", "Imdb": "tt1234567"}
        parent_id:    Jellyfin library ID to scope the search to a specific library.
                      This matters when the same series exists in multiple libraries
                      (e.g. "Monster" in both "TV Shows" and "Anime").

        Returns the item dict (with Id, Name, Path, ProviderIds) or None.
        """
        if not title:
            return None

        params = {
            "searchTerm": title,
            "IncludeItemTypes": item_type,
            "fields": "Path,ProviderIds",
            "Recursive": "true",
            "Limit": "10",
        }
        if parent_id:
            params["ParentId"] = parent_id
        resp = self._request("GET", "/Items", params=params)
        if resp is None:
            return None
        try:
            data = resp.json()
        except (ValueError, TypeError):
            return None

        items = data.get("Items", [])
        if not items:
            return None

        # Priority 1: match by provider ID (most reliable)
        if provider_ids:
            for item in items:
                item_pids = item.get("ProviderIds", {})
                for provider, pid in provider_ids.items():
                    if pid and str(item_pids.get(provider, "")) == str(pid):
                        logger.info("Matched [%s] by %s=%s -> id=%s",
                                    item.get("Name"), provider, pid, item["Id"])
                        return item

        # Priority 2: exact title match
        for item in items:
            if item.get("Name", "").lower() == title.lower():
                logger.info("Matched [%s] by exact title -> id=%s", title, item["Id"])
                return item

        # No reliable match — return None so the caller falls back to library-level refresh.
        # We intentionally do NOT use fuzzy/best-guess matching here because refreshing the
        # wrong item is worse than a slightly slower library-level scan.
        logger.info("No provider ID or exact title match for [%s] among %d results — skipping targeted refresh",
                     title, len(items))
        return None

    # -- refresh --

    def refresh_item(self, item_id, item_name="?"):
        """POST /Items/{id}/Refresh — targeted, bypasses FileRefresher chain entirely."""
        resp = self._request(
            "POST",
            f"/Items/{item_id}/Refresh"
            "?metadataRefreshMode=Default&imageRefreshMode=Default",
        )
        if resp is not None:
            logger.info("Targeted refresh queued for [%s] (id=%s)", item_name, item_id)
            return True
        logger.error("Targeted refresh FAILED for [%s] (id=%s)", item_name, item_id)
        return False

    def refresh_library(self, library_id):
        """POST /Items/{id}/Refresh on a library root — slower fallback."""
        resp = self._request(
            "POST",
            f"/Items/{library_id}/Refresh"
            "?metadataRefreshMode=Default&imageRefreshMode=Default",
        )
        return resp is not None

    def refresh_all(self):
        resp = self._request("POST", "/Library/Refresh")
        return resp is not None

    def get_library_scan_status(self):
        resp = self._request("GET", "/Library/VirtualFolders")
        if resp is None:
            return None
        try:
            data = resp.json()
        except (ValueError, TypeError):
            return None
        result = {}
        for folder in data:
            item_id = folder.get("ItemId")
            progress = folder.get("RefreshProgress")
            result[item_id] = progress is not None
        return result


class RefreshManager:
    """Handles targeted item refreshes with library-level fallback.

    On webhook:
      1. Try to find the specific Series/Movie item in Jellyfin by provider ID or title
      2. If found -> POST /Items/{itemId}/Refresh (fast, ~5-15s)
      3. If NOT found (new item) -> fall back to library-level refresh
    """

    def __init__(self, client, hold_seconds=5, poll_interval=3, path_maps=None):
        self.client = client
        self._hold_seconds = hold_seconds
        self._poll_interval = poll_interval
        self._path_maps = path_maps or []
        self._lock = threading.Lock()
        # library_id -> {refreshing, dirty, name, triggered_at}  (only for library-level fallbacks)
        self._lib_state = {}
        self._stop = threading.Event()
        # stats
        self._stats = {"targeted": 0, "library_fallback": 0, "failed": 0}

    def _translate_path(self, path):
        """Apply --path-map rules to convert a webhook path to a local NFS path.

        Sonarr/Radarr run in containers with their own mount layout (e.g.
        /data/tv/ShowName) but the NFS mount on this host may be at a
        different path (e.g. /shares/video/tv/ShowName).  The --path-map
        flag lets you bridge this gap so invalidate_nfs_cache() hits the
        right directory on the local filesystem.
        """
        if not path:
            return path
        norm = path.replace("\\", "/")
        for remote, local in self._path_maps:
            remote_norm = remote.replace("\\", "/")
            if norm.startswith(remote_norm + "/") or norm == remote_norm:
                translated = local + norm[len(remote_norm):]
                logger.debug("Path translated: %s -> %s", path, translated)
                return translated
        return path

    def request_targeted_refresh(self, media_info):
        """Try to refresh a specific item; fall back to library-level if not found.

        media_info: dict with keys title, item_type, root_folder_name,
                    and optionally tvdb_id, tmdb_id, imdb_id.
        Returns a status string.
        """
        title = media_info.get("title")
        item_type = media_info.get("item_type")  # "Series" or "Movie"

        # Step 0: bust NFS attribute cache for the media directory so Jellyfin
        # sees the new file immediately instead of a stale cached listing.
        # The webhook path (from Sonarr/Radarr's container) is translated to
        # the local NFS mount path via --path-map rules before invalidation.
        media_path = media_info.get("media_path")
        if media_path:
            local_path = self._translate_path(media_path)
            invalidate_nfs_cache(local_path)

        # Step 1: resolve the target library so we scope the search correctly.
        # This prevents matching the wrong copy when the same series exists in
        # multiple libraries (e.g. "Monster" in both "TV Shows" and "Anime").
        root_folder = media_info.get("root_folder_name")
        library_id = None
        if root_folder:
            matched_libs = self.client.libraries_for_folder_name(root_folder)
            if matched_libs:
                library_id = matched_libs[0]["id"]

        # Step 2: search by title within that library, verify by provider IDs
        provider_ids = {}
        for provider, key in [("Tvdb", "tvdb_id"), ("Tmdb", "tmdb_id"), ("Imdb", "imdb_id")]:
            pid = media_info.get(key)
            if pid:
                provider_ids[provider] = str(pid)

        item = self.client.find_item(title, item_type, provider_ids or None, parent_id=library_id)

        # Step 3: targeted refresh if found
        if item:
            ok = self.client.refresh_item(item["Id"], item.get("Name", title))
            if ok:
                with self._lock:
                    self._stats["targeted"] += 1
                return f"targeted:{item.get('Name', title)}"
            with self._lock:
                self._stats["failed"] += 1
            return "failed"

        # Step 4: item not found in Jellyfin — fall back to library-level refresh
        logger.info("[%s] not found in Jellyfin — falling back to library-level refresh", title)
        return self._library_fallback(media_info)

    def _library_fallback(self, media_info):
        """Refresh the specific root folder within the library (not the whole library).

        A library can have multiple source locations (e.g. /media/tv, /media/anime).
        We find the specific root folder item and refresh just that one, which scopes
        ValidateChildren to only that directory tree instead of the entire library.
        Falls back to full library refresh if the root folder item can't be found.
        """
        root_folder = media_info.get("root_folder_name")
        if not root_folder:
            logger.warning("No root folder name — cannot determine library")
            with self._lock:
                self._stats["failed"] += 1
            return "failed:no_root_folder"

        matched = self.client.libraries_for_folder_name(root_folder)
        if not matched:
            logger.warning("No library matched folder '%s' — triggering full scan", root_folder)
            ok = self.client.refresh_all()
            with self._lock:
                self._stats["library_fallback"] += 1
            return f"full_scan:{'ok' if ok else 'failed'}"

        results = []
        for lib in matched:
            # Try to scope the refresh to just the matching root folder
            root_item = self.client.find_root_folder_item(lib["id"], root_folder)
            if root_item:
                refresh_id = root_item["Id"]
                refresh_label = f"{lib['name']}/{root_folder}"
                logger.info("Scoped fallback to root folder [%s] (id=%s)", refresh_label, refresh_id)
            else:
                refresh_id = lib["id"]
                refresh_label = lib["name"]
                logger.info("Could not find root folder item, refreshing entire library [%s]", lib["name"])
            result = self._request_library_refresh(refresh_id, refresh_label)
            results.append(f"{refresh_label}:{result}")

        with self._lock:
            self._stats["library_fallback"] += 1
        return ";".join(results)

    def _request_library_refresh(self, library_id, library_name):
        fire = False
        with self._lock:
            state = self._lib_state.get(library_id)
            if state is None:
                state = {"refreshing": False, "dirty": False, "name": library_name, "triggered_at": 0}
                self._lib_state[library_id] = state

            if state["refreshing"]:
                state["dirty"] = True
                logger.info("[%s] Library scan in progress — marked dirty", library_name)
                return "queued"
            else:
                state["refreshing"] = True
                state["dirty"] = False
                state["triggered_at"] = time.time()
                fire = True

        if fire:
            ok = self.client.refresh_library(library_id)
            if ok:
                logger.info("[%s] Library-level refresh triggered", library_name)
                return "triggered"
            else:
                with self._lock:
                    self._lib_state[library_id]["refreshing"] = False
                return "failed"

    # -- background poll loop for library-level fallback tracking --

    def poll_loop(self):
        while not self._stop.is_set():
            self._stop.wait(self._poll_interval)
            if self._stop.is_set():
                break
            try:
                self._poll_tick()
            except Exception:
                logger.exception("Error in poll loop")

    def _poll_tick(self):
        with self._lock:
            has_active = any(s["refreshing"] for s in self._lib_state.values())
        if not has_active:
            return

        status = self.client.get_library_scan_status()
        if status is None:
            return

        now = time.time()
        to_fire = []

        with self._lock:
            for lib_id, state in self._lib_state.items():
                if not state["refreshing"]:
                    continue
                is_scanning = status.get(lib_id, False)
                hold_elapsed = (now - state["triggered_at"]) >= self._hold_seconds
                if is_scanning or not hold_elapsed:
                    continue
                if state["dirty"]:
                    state["dirty"] = False
                    state["triggered_at"] = now
                    to_fire.append((lib_id, state["name"]))
                    logger.info("[%s] Library scan complete — dirty, triggering rescan", state["name"])
                else:
                    state["refreshing"] = False
                    logger.info("[%s] Library scan complete, queue clear", state["name"])

        for lib_id, name in to_fire:
            ok = self.client.refresh_library(lib_id)
            if not ok:
                logger.error("[%s] Queued library rescan FAILED", name)
                with self._lock:
                    if lib_id in self._lib_state:
                        self._lib_state[lib_id]["refreshing"] = False

    def stop(self):
        self._stop.set()

    def get_status(self):
        with self._lock:
            return {
                "stats": dict(self._stats),
                "library_queue": {lid: dict(s) for lid, s in self._lib_state.items()},
            }


# -- Webhook payload parsing --

def _parent_folder_name(path):
    """Last component of the parent directory, lowercased."""
    norm = path.replace("\\", "/").rstrip("/")
    parent = norm.rsplit("/", 1)[0] if "/" in norm else norm
    return parent.rsplit("/", 1)[-1].lower()


def extract_media_info(payload):
    """Extract structured media info from a Sonarr or Radarr webhook.

    Returns a dict with: title, item_type, root_folder_name,
    and optionally tvdb_id, tmdb_id, imdb_id.
    """
    event_type = payload.get("eventType", "Unknown")
    info = {"event_type": event_type}

    # -- Sonarr --
    series = payload.get("series")
    if series:
        info["title"] = series.get("title", "")
        info["item_type"] = "Series"
        info["tvdb_id"] = series.get("tvdbId")
        info["imdb_id"] = series.get("imdbId")
        info["tmdb_id"] = series.get("tmdbId")

        path = series.get("path")
        if path:
            info["root_folder_name"] = _parent_folder_name(path)
            info["media_path"] = path
            logger.info("Sonarr [%s] '%s' path=%s root=%s tvdb=%s",
                        event_type, info["title"], path,
                        info.get("root_folder_name"), info.get("tvdb_id"))
        return info

    # -- Radarr --
    movie = payload.get("movie")
    if movie:
        info["title"] = movie.get("title", "")
        info["item_type"] = "Movie"
        info["tmdb_id"] = movie.get("tmdbId")
        info["imdb_id"] = movie.get("imdbId")

        path = movie.get("folderPath") or movie.get("path")
        if path:
            info["root_folder_name"] = _parent_folder_name(path)
            info["media_path"] = path
            logger.info("Radarr [%s] '%s' path=%s root=%s tmdb=%s",
                        event_type, info["title"], path,
                        info.get("root_folder_name"), info.get("tmdb_id"))
        return info

    # -- Fallback: derive from file path --
    for key in ("episodeFile", "movieFile"):
        file_obj = payload.get(key)
        if file_obj:
            fp = file_obj.get("path")
            if fp:
                media_dir = fp.replace("\\", "/").rstrip("/").rsplit("/", 1)[0]
                info["root_folder_name"] = _parent_folder_name(media_dir)
                info["media_path"] = media_dir
                info.setdefault("item_type", "Movie" if key == "movieFile" else "Series")
                logger.info("Derived media info from %s.path: root=%s", key, info["root_folder_name"])
                return info

    dest = payload.get("destinationPath")
    if dest:
        info["root_folder_name"] = _parent_folder_name(dest)
        info["media_path"] = dest
        logger.info("Root folder from destinationPath: %s", info["root_folder_name"])
        return info

    logger.warning("Could not extract media info from %s webhook", event_type)
    return None


# -- HTTP handler --

class WebhookHandler(http.server.BaseHTTPRequestHandler):

    jellyfin: JellyfinClient
    manager: RefreshManager

    def log_message(self, fmt, *args):
        logger.info("%s - %s", self.client_address[0], fmt % args)

    def do_GET(self):
        try:
            self._handle_get()
        except Exception:
            logger.exception("Error handling GET")
            try:
                self._respond("Internal server error.", HTTPStatus.INTERNAL_SERVER_ERROR)
            except Exception:
                pass

    def _handle_get(self):
        path = self.path.lower().rstrip("/")

        if path in ("/libraries", "/status"):
            self.jellyfin.fetch_libraries()
            body = json.dumps(
                {"libraries": self.jellyfin.get_libraries(), "manager": self.manager.get_status()},
                indent=2, default=str,
            )
            self._respond(body, HTTPStatus.OK, "application/json")

        elif "jellyfin" in path:
            ok = self.jellyfin.refresh_all()
            msg = f"Full library refresh {'triggered' if ok else 'FAILED'}."
            self._respond(msg, HTTPStatus.OK)

        else:
            self._respond(
                "Endpoints:\n"
                "  POST /          — Sonarr / Radarr webhook (targeted item refresh)\n"
                "  GET  /status    — show library mapping + manager status\n"
                "  GET  /jellyfin  — trigger a full library refresh\n",
                HTTPStatus.OK,
            )

    def do_POST(self):
        try:
            self._handle_post()
        except Exception:
            logger.exception("Error handling POST")
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

        media_info = extract_media_info(payload)
        if not media_info:
            self._respond("Acknowledged (could not extract media info).", HTTPStatus.OK)
            return

        if not media_info.get("title") and not media_info.get("root_folder_name"):
            self._respond("Acknowledged (no title or root folder).", HTTPStatus.OK)
            return

        result = self.manager.request_targeted_refresh(media_info)
        self._respond(result, HTTPStatus.OK)

    def _respond(self, body, status, content_type="text/plain"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))


def _run_service(config):
    jellyfin = JellyfinClient(config.address, config.apikey)
    manager = RefreshManager(jellyfin, config.hold_delay, config.poll_interval,
                             path_maps=getattr(config, "path_maps", []))

    logger.info("Connecting to Jellyfin at %s ...", config.address)
    if not jellyfin.fetch_libraries():
        raise ConnectionError("Could not reach Jellyfin — check --address and --apikey")

    # Startup precheck: force-set LibraryScanFanoutConcurrency
    jellyfin.precheck_configuration()

    WebhookHandler.jellyfin = jellyfin
    WebhookHandler.manager = manager

    logger.info("--- Configuration ---")
    logger.info("Jellyfin : %s", config.address)
    logger.info("Listen   : %s:%d", config.host, config.port)
    logger.info("Poll     : every %ds", config.poll_interval)
    logger.info("Strategy : targeted item refresh -> library fallback for new items")
    if config.path_maps:
        logger.info("Path maps: %d configured (for NFS cache invalidation)", len(config.path_maps))
        for remote, local in config.path_maps:
            logger.info("  %s -> %s", remote, local)
    else:
        logger.info("Path maps: none (NFS invalidation uses webhook paths as-is)")
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
    while True:
        try:
            _run_service(config)
            return
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
                "item-level refreshes.  Bypasses Jellyfin's slow FileRefresher "
                "debounce chain by calling POST /Items/{id}/Refresh directly on "
                "the specific Series or Movie item.  Falls back to library-level "
                "refresh for items not yet known to Jellyfin."
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
        parser.add_argument("-m", "--path-map", action="append", default=[],
                            metavar="REMOTE=LOCAL",
                            help="Map webhook paths to local NFS mount paths for cache "
                                 "invalidation. Can be specified multiple times. "
                                 "Example: -m /data/tv=/shares/video/tv "
                                 "-m /data/movies=/shares/video/movies")

        config = parser.parse_args()
        config.path_maps = []
        for mapping in config.path_map:
            if "=" not in mapping:
                parser.error(f"Invalid --path-map format: {mapping!r} (expected REMOTE=LOCAL)")
            remote, local = mapping.split("=", 1)
            config.path_maps.append((remote.rstrip("/"), local.rstrip("/")))
            logger.info("Path map: %s -> %s", remote, local)
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
