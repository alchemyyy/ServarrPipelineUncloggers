import http.server
import json
import logging
import os
import socketserver
import sys
from http import HTTPStatus

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ServarrForceImporter")


# Config
DEFAULT_CONFIG = {
    "listen_host": "0.0.0.0",
    "listen_port": 9099,
    "instances": [
        {
            "name": "Sonarr",
            "type": "sonarr",
            "url": "http://192.168.1.20:8989",
            "api_key": "YOUR_SONARR_API_KEY",
        },
        {
            "name": "Radarr",
            "type": "radarr",
            "url": "http://192.168.1.20:7878",
            "api_key": "YOUR_RADARR_API_KEY",
        },
    ],
}


def load_config(path):
    try:
        with open(path, "r") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.info("Config file not found, generating default: %s", path)
        with open(path, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        logger.info("Edit the config file with your instance URLs and API keys, then restart.")
        sys.exit(0)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in config file: %s", e)
        sys.exit(1)

    instances = []
    for i, inst in enumerate(raw.get("instances", [])):
        name = inst.get("name", f"instance-{i}")
        inst_type = inst.get("type", "").lower()
        url = inst.get("url", "").rstrip("/")
        api_key = inst.get("api_key", "")

        if inst_type not in ("sonarr", "radarr"):
            logger.error("Instance '%s' has invalid type '%s' (must be sonarr or radarr)", name, inst_type)
            sys.exit(1)
        if not url or not api_key:
            logger.error("Instance '%s' is missing url or api_key", name)
            sys.exit(1)

        instances.append({
            "name": name,
            "type": inst_type,
            "url": url,
            "api_key": api_key,
            "headers": {"X-Api-Key": api_key},
        })

    if not instances:
        logger.error("No instances defined in config")
        sys.exit(1)

    return {
        "listen_host": raw.get("listen_host", "0.0.0.0"),
        "listen_port": raw.get("listen_port", 9099),
        "instances": instances,
    }


# Instance lookup
def find_instance_by_url(instances, application_url):
    if not application_url:
        return None
    norm = application_url.rstrip("/").lower()
    for inst in instances:
        if inst["url"].lower() == norm:
            return inst
    return None


def find_instance_by_payload(instances, payload):
    """Fallback: infer instance from payload shape when applicationUrl doesn't match."""
    is_sonarr = "series" in payload
    is_radarr = "movie" in payload and "series" not in payload
    target_type = "sonarr" if is_sonarr else "radarr" if is_radarr else None
    if not target_type:
        return None
    candidates = [i for i in instances if i["type"] == target_type]
    if len(candidates) == 1:
        return candidates[0]
    return None


# Servarr API helpers
def get_queue(instance):
    resp = requests.get(
        f"{instance['url']}/api/v3/queue",
        headers=instance["headers"],
        params={"pageSize": 10000},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("records", [])


def get_manual_import_files(instance, download_id):
    resp = requests.get(
        f"{instance['url']}/api/v3/manualimport",
        headers=instance["headers"],
        params={"downloadId": download_id, "filterExistingFiles": "false"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def send_manual_import_command(instance, prepared_files):
    resp = requests.post(
        f"{instance['url']}/api/v3/command",
        headers=instance["headers"],
        json={"name": "ManualImport", "files": prepared_files, "importMode": "auto"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp


# File preparation (Sonarr vs Radarr)
def prepare_sonarr_files(available_files):
    prepared = []
    for f in available_files:
        series_id = f.get("series", {}).get("id", 0)
        episode_ids = [ep["id"] for ep in f.get("episodes", [])]
        if not series_id or not episode_ids:
            logger.warning("  Skipping (missing seriesId or episodeIds): %s", f.get("path"))
            continue
        prepared.append({
            "path": f["path"],
            "folderName": f.get("folderName", ""),
            "seriesId": series_id,
            "episodeIds": episode_ids,
            "quality": f["quality"],
            "languages": f.get("languages", [{"id": 1, "name": "English"}]),
            "releaseGroup": f.get("releaseGroup", ""),
            "indexerFlags": f.get("indexerFlags", 0),
            "releaseType": f.get("releaseType", "singleEpisode"),
            "downloadId": f.get("downloadId", ""),
        })
    return prepared

def prepare_radarr_files(available_files):
    prepared = []
    for f in available_files:
        movie_id = f.get("movie", {}).get("id", 0)
        if not movie_id:
            logger.warning("  Skipping (missing movieId): %s", f.get("path"))
            continue
        prepared.append({
            "path": f["path"],
            "folderName": f.get("folderName", ""),
            "movieId": movie_id,
            "quality": f["quality"],
            "languages": f.get("languages", [{"id": 1, "name": "English"}]),
            "releaseGroup": f.get("releaseGroup", ""),
            "indexerFlags": f.get("indexerFlags", 0),
            "downloadId": f.get("downloadId", ""),
        })
    return prepared


# Core import logic (shared by webhook handler and startup scan)
def handle_manual_import(instance, download_id, label):
    inst_name = instance["name"]

    try:
        available_files = get_manual_import_files(instance, download_id)
    except requests.RequestException as e:
        logger.error("[%s] Failed to get manual import files for %s: %s", inst_name, download_id, e)
        return False

    if not available_files:
        logger.info("[%s] No importable files for downloadId=%s (%s)", inst_name, download_id, label)
        return False

    logger.info("[%s] %d file(s) available for import (%s):", inst_name, len(available_files), label)
    for f in available_files:
        logger.info("[%s]   %s", inst_name, f.get("path"))

    if instance["type"] == "sonarr":
        prepared = prepare_sonarr_files(available_files)
    else:
        prepared = prepare_radarr_files(available_files)

    if not prepared:
        logger.warning("[%s] All files skipped (missing required IDs) for downloadId=%s", inst_name, download_id)
        return False

    try:
        resp = send_manual_import_command(instance, prepared)
        logger.info("[%s] ManualImport command accepted (HTTP %d) for downloadId=%s", inst_name, resp.status_code, download_id)
        return True
    except requests.RequestException as e:
        logger.error("[%s] Failed to send ManualImport command for downloadId=%s: %s", inst_name, download_id, e)
        return False


# Startup scan — catch items already stuck before the webhook listener started
def startup_scan(instances):
    for instance in instances:
        inst_name = instance["name"]
        logger.info("[%s] Running startup queue scan...", inst_name)

        try:
            records = get_queue(instance)
        except requests.RequestException as e:
            logger.error("[%s] Startup scan failed to fetch queue: %s", inst_name, e)
            continue

        completed = [r for r in records if r.get("status") == "completed"]
        logger.info("[%s] %d completed item(s) in queue (out of %d total)", inst_name, len(completed), len(records))

        imported = 0
        for item in completed:
            download_id = item.get("downloadId")
            if not download_id:
                continue
            title = item.get("title", download_id)
            if handle_manual_import(instance, download_id, title):
                imported += 1

        logger.info("[%s] Startup scan complete: imported %d item(s)", inst_name, imported)


class WebhookHandler(http.server.BaseHTTPRequestHandler):
    # Set by main() before the server starts
    instances = []

    def log_message(self, fmt, *args):
        logger.debug("HTTP %s - %s", self.client_address[0], fmt % args)

    def do_GET(self):
        self._respond(
            "ServarrForceImporter is running.\n"
            "POST ManualInteractionRequired webhooks to this endpoint.\n",
            HTTPStatus.OK,
        )

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self._respond("Empty body.", HTTPStatus.BAD_REQUEST)
            return

        try:
            raw = self.rfile.read(content_length)
            payload = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error("Invalid webhook body: %s", e)
            self._respond(f"Invalid JSON: {e}", HTTPStatus.BAD_REQUEST)
            return

        event_type = payload.get("eventType", "Unknown")
        logger.info("Webhook received: eventType=%s", event_type)

        if event_type.lower() == "test":
            application_url = payload.get("applicationUrl", "")
            instance = find_instance_by_url(self.instances, application_url)
            if instance is None:
                instance = find_instance_by_payload(self.instances, payload)
            if instance:
                logger.info("Test webhook OK — matched instance '%s'", instance["name"])
            else:
                logger.warning("Test webhook OK — but no matching instance found for applicationUrl='%s'", application_url)
            self._respond("Test webhook received.", HTTPStatus.OK)
            return

        if event_type != "ManualInteractionRequired":
            self._respond(f"Ignored eventType={event_type}.", HTTPStatus.OK)
            return

        # Identify the originating instance
        application_url = payload.get("applicationUrl", "")
        instance = find_instance_by_url(self.instances, application_url)

        if instance is None:
            instance = find_instance_by_payload(self.instances, payload)
            if instance:
                logger.warning(
                    "applicationUrl '%s' not matched; inferred instance '%s' by payload shape",
                    application_url, instance["name"],
                )
            else:
                logger.error(
                    "Cannot identify instance for applicationUrl='%s'. "
                    "Ensure the url in your config matches the Application URL in Servarr settings.",
                    application_url,
                )
                self._respond("Unknown instance.", HTTPStatus.OK)
                return

        download_id = payload.get("downloadId", "")
        if not download_id:
            logger.warning("Webhook has no downloadId, ignoring")
            self._respond("No downloadId.", HTTPStatus.OK)
            return

        # Build a human-readable label for logging
        if instance["type"] == "sonarr":
            series = payload.get("series", {})
            episodes = payload.get("episodes", [])
            if episodes:
                label = f"{series.get('title', '?')} S{episodes[0].get('seasonNumber', '?'):02}E{episodes[0].get('episodeNumber', '?'):02}"
            else:
                label = series.get("title", download_id)
        else:
            movie = payload.get("movie", {})
            label = f"{movie.get('title', '?')} ({movie.get('year', '?')})"

        logger.info("[%s] ManualInteractionRequired: %s (downloadId=%s)", instance["name"], label, download_id)
        handle_manual_import(instance, download_id, label)
        self._respond("OK", HTTPStatus.OK)

    def _respond(self, body, status, content_type="text/plain"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Listens for Sonarr/Radarr ManualInteractionRequired webhooks and force-imports stuck downloads."
    )
    parser.add_argument(
        "--config",
        default=os.path.join(
            os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__)),
            "ServarrForceImporter.json",
        ),
        help="Path to JSON config file (default: ServarrForceImporter.json next to the exe/script)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    instances = config["instances"]
    host = config["listen_host"]
    port = config["listen_port"]

    logger.info("--- ServarrForceImporter ---")
    for inst in instances:
        logger.info("  Instance: %s (%s) at %s", inst["name"], inst["type"], inst["url"])
    logger.info("  Listening on: %s:%d", host, port)
    logger.info("--------------------------")

    # Validate connectivity
    for inst in instances:
        try:
            resp = requests.get(f"{inst['url']}/api/v3/system/status", headers=inst["headers"], timeout=5)
            resp.raise_for_status()
            logger.info("[%s] Connected OK", inst["name"])
        except requests.RequestException as e:
            logger.error("[%s] Cannot connect to %s: %s", inst["name"], inst["url"], e)
            sys.exit(1)

    # Catch already-stuck items before we start listening
    startup_scan(instances)

    # Start webhook server
    WebhookHandler.instances = instances

    logger.info("Webhook server starting on %s:%d", host, port)
    logger.info("Configure Sonarr/Radarr webhooks to: http://<this-host>:%d/", port)

    try:
        with socketserver.TCPServer((host, port), WebhookHandler) as httpd:
            httpd.allow_reuse_address = True
            httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    except OSError as e:
        logger.error("Failed to bind to %s:%d - %s", host, port, e)
        sys.exit(1)


if __name__ == "__main__":
    main()
