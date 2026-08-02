import concurrent.futures
import json
import logging
import logging.handlers
import math
import os
import sys
import threading
import time
from datetime import datetime, timezone

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SonarrMissingEpisodeSearcher")

CONNECTION_FAILURE_REMINDER_SECONDS = 15 * 60
CONNECTION_RETRY_SECONDS = 60
_connection_failure_log_times = {}
_connection_failure_lock = threading.Lock()


def _is_connection_failure(exception):
    return isinstance(exception, (requests.ConnectionError, requests.Timeout))


def _describe_request_failure(exception):
    if isinstance(exception, requests.Timeout):
        return "Request timed out"
    if isinstance(exception, requests.ConnectionError):
        message = str(exception).casefold()
        if "refused" in message or "10061" in message:
            return "Connection refused"
        if "getaddrinfo" in message or "name or service not known" in message or "11001" in message:
            return "Host name could not be resolved"
        return "Connection failed"
    if isinstance(exception, requests.HTTPError) and exception.response is not None:
        return f"HTTP {exception.response.status_code}"
    return exception.__class__.__name__


def _handle_request_failure(instance, operation, exception):
    description = _describe_request_failure(exception)
    if not _is_connection_failure(exception):
        logger.error("[%s] %s failed: %s", instance["name"], operation, description)
        return False

    instance_key = instance["url"]
    current_time = time.monotonic()
    with _connection_failure_lock:
        last_log_time = _connection_failure_log_times.get(instance_key)
        should_log = (
            last_log_time is None
            or current_time - last_log_time >= CONNECTION_FAILURE_REMINDER_SECONDS
        )
        if should_log:
            _connection_failure_log_times[instance_key] = current_time

    if should_log:
        logger.warning(
            "[%s] %s while attempting to %s at %s; will retry",
            instance["name"],
            description,
            operation,
            instance["url"],
        )
    return True


def _mark_connection_available(instance):
    with _connection_failure_lock:
        was_unavailable = _connection_failure_log_times.pop(instance["url"], None) is not None

    if was_unavailable:
        logger.info("[%s] Connection restored: %s", instance["name"], instance["url"])


def _is_connection_unavailable(instance):
    with _connection_failure_lock:
        return instance["url"] in _connection_failure_log_times

try:
    _log_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
    _fh = logging.handlers.RotatingFileHandler(
        os.path.join(_log_dir, "SonarrMissingEpisodeSearcher.log"),
        maxBytes=5 * 1024 * 1024, backupCount=3,
    )
    _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logging.getLogger().addHandler(_fh)
except Exception:
    pass


# Config
DEFAULT_CONFIG = {
    "instances": [
        {
            "name": "Sonarr",
            "url": "http://192.168.1.20:8989",
            "api_key": "YOUR_SONARR_API_KEY",
            "min_age_hours": 2,
            "max_age_hours": 24,
            "interval_minutes": 15,
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
        name = inst.get("name", f"sonarr-{i}")
        url = inst.get("url", "").rstrip("/")
        api_key = inst.get("api_key", "")
        min_age_hours = inst.get("min_age_hours", 2)
        max_age_hours = inst.get("max_age_hours", 24)
        interval_minutes = inst.get("interval_minutes", 15)

        if not url or not api_key:
            logger.error("Instance '%s' is missing url or api_key", name)
            sys.exit(1)

        settings = {
            "min_age_hours": min_age_hours,
            "max_age_hours": max_age_hours,
            "interval_minutes": interval_minutes,
        }
        for setting_name, setting_value in settings.items():
            if isinstance(setting_value, bool) or not isinstance(setting_value, (int, float)) or not math.isfinite(setting_value):
                logger.error("Instance '%s' has invalid %s; expected a finite number", name, setting_name)
                sys.exit(1)

        if min_age_hours < 0:
            logger.error("Instance '%s' has invalid min_age_hours; expected zero or greater", name)
            sys.exit(1)
        if max_age_hours < min_age_hours:
            logger.error("Instance '%s' has invalid age window; max_age_hours must be at least min_age_hours", name)
            sys.exit(1)
        if interval_minutes <= 0:
            logger.error("Instance '%s' has invalid interval_minutes; expected a value greater than zero", name)
            sys.exit(1)

        instances.append({
            "name": name,
            "url": url,
            "api_key": api_key,
            "headers": {"X-Api-Key": api_key},
            "min_age_hours": min_age_hours,
            "max_age_hours": max_age_hours,
            "interval_minutes": interval_minutes,
        })

    if not instances:
        logger.error("No instances defined in config")
        sys.exit(1)

    return {"instances": instances}


def get_missing_episodes(instance, page=1, page_size=1000):
    resp = requests.get(
        f"{instance['url']}/api/v3/wanted/missing",
        headers=instance["headers"],
        params={
            "pageSize": page_size,
            "page": page,
            "sortKey": "episodes.airDateUtc",
            "sortDirection": "descending",
            "includeSeries": "true",
        },
        timeout=15,
    )
    resp.raise_for_status()
    _mark_connection_available(instance)
    return resp.json()


def search_episode(instance, episode_id):
    resp = requests.post(
        f"{instance['url']}/api/v3/command",
        headers=instance["headers"],
        json={"name": "EpisodeSearch", "episodeIds": [episode_id]},
        timeout=15,
    )
    resp.raise_for_status()
    _mark_connection_available(instance)
    return resp


def get_episode_age_hours(episode):
    air_date_utc = episode.get("airDateUtc")
    if not air_date_utc:
        return None
    try:
        aired = datetime.fromisoformat(air_date_utc.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - aired
        return delta.total_seconds() / 3600.0
    except (ValueError, TypeError):
        return None


def scan_missing(instance, min_age_hours, max_age_hours, interval_seconds, stop_event=None):
    inst_name = instance["name"]
    if not _is_connection_unavailable(instance):
        logger.info("[%s] Scanning for missing episodes (age window: %.1f–%.1f hours)...", inst_name, min_age_hours, max_age_hours)

    try:
        data = get_missing_episodes(instance)
    except requests.RequestException as e:
        _handle_request_failure(instance, "fetch missing episodes", e)
        return False

    records = data.get("records", [])
    total = data.get("totalRecords", len(records))
    logger.info("[%s] %d missing episode(s) (showing first %d)", inst_name, total, len(records))

    # Collect eligible episodes first so we can compute stagger delay
    eligible = []
    for ep in records:
        age_hours = get_episode_age_hours(ep)
        if age_hours is None:
            continue
        if age_hours > max_age_hours:
            break
        if age_hours < min_age_hours:
            continue

        series_title = ep.get("series", {}).get("title", "?")
        season = ep.get("seasonNumber", 0)
        episode_num = ep.get("episodeNumber", 0)
        ep_title = ep.get("title", "?")
        ep_id = ep.get("id")
        label = f"{series_title} S{season:02}E{episode_num:02} - {ep_title}"
        eligible.append({"id": ep_id, "label": label, "age_hours": age_hours})

    if not eligible:
        logger.info("[%s] No episodes in age window, nothing to search", inst_name)
        return True

    delay = interval_seconds / len(eligible)
    logger.info("[%s] %d episode(s) to search, staggering %.1fs apart", inst_name, len(eligible), delay)

    searched = 0
    for i, ep in enumerate(eligible):
        logger.info("[%s] %s — aired %.1f hours ago, searching...", inst_name, ep["label"], ep["age_hours"])

        try:
            resp = search_episode(instance, ep["id"])
            logger.info("[%s] Search command accepted (HTTP %d) for %s", inst_name, resp.status_code, ep["label"])
            searched += 1
        except requests.RequestException as e:
            connection_failed = _handle_request_failure(instance, f"search for {ep['label']}", e)
            if connection_failed:
                return True

        # Stagger: sleep between searches (skip after the last one)
        if i < len(eligible) - 1:
            if stop_event is None:
                time.sleep(delay)
            elif stop_event.wait(delay):
                logger.info("[%s] Scan interrupted after triggering %d search(es)", inst_name, searched)
                return True

    logger.info("[%s] Scan complete: triggered search for %d episode(s)", inst_name, searched)
    return True


def _run_instance_service(instance, stop_event):
    min_age = instance["min_age_hours"]
    max_age = instance["max_age_hours"]
    interval_seconds = instance["interval_minutes"] * 60

    while not stop_event.is_set():
        scan_start = time.monotonic()
        scan_successful = scan_missing(instance, min_age, max_age, interval_seconds, stop_event)
        if not scan_successful and _is_connection_unavailable(instance):
            retry_seconds = min(CONNECTION_RETRY_SECONDS, interval_seconds)
            stop_event.wait(retry_seconds)
            continue

        elapsed = time.monotonic() - scan_start
        remaining = max(0, interval_seconds - elapsed)
        if remaining > 0 and not stop_event.is_set():
            logger.info("[%s] Next scan in %.0f seconds...", instance["name"], remaining)
            stop_event.wait(remaining)


def _run_service(config):
    instances = config["instances"]

    logger.info("--- SonarrMissingEpisodeSearcher ---")
    for inst in instances:
        logger.info("  Instance: %s at %s", inst["name"], inst["url"])
        logger.info("    Age window: %.1f–%.1f hours", inst["min_age_hours"], inst["max_age_hours"])
        logger.info("    Scan interval: %.1f minutes", inst["interval_minutes"])
    logger.info("------------------------------------")

    # Validate connectivity
    for inst in instances:
        try:
            resp = requests.get(f"{inst['url']}/api/v3/system/status", headers=inst["headers"], timeout=5)
            resp.raise_for_status()
            _mark_connection_available(inst)
            logger.info("[%s] Connected OK", inst["name"])
        except requests.RequestException as e:
            _handle_request_failure(inst, "check system status", e)

    # Give each instance an independent schedule so stagger delays cannot block others
    stop_event = threading.Event()
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=len(instances),
        thread_name_prefix="sonarr-instance",
    )
    futures = [executor.submit(_run_instance_service, inst, stop_event) for inst in instances]

    try:
        while True:
            completed, _ = concurrent.futures.wait(
                futures,
                timeout=1,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            if not completed:
                continue

            for future in completed:
                future.result()
            raise RuntimeError("An instance worker stopped unexpectedly")
    finally:
        stop_event.set()
        executor.shutdown(wait=True, cancel_futures=True)


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
        import argparse

        parser = argparse.ArgumentParser(
            description="Scans Sonarr for missing episodes within an age window and triggers searches for them."
        )
        parser.add_argument(
            "--config",
            default=os.path.join(
                os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__)),
                "SonarrMissingEpisodeSearcher.json",
            ),
            help="Path to JSON config file (default: SonarrMissingEpisodeSearcher.json next to the exe/script)",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run a single scan and exit (no loop)",
        )
        args = parser.parse_args()

        config = load_config(args.config)

        if args.once:
            connection_failed = False
            for inst in config["instances"]:
                try:
                    resp = requests.get(f"{inst['url']}/api/v3/system/status", headers=inst["headers"], timeout=5)
                    resp.raise_for_status()
                    _mark_connection_available(inst)
                except requests.RequestException as e:
                    _handle_request_failure(inst, "check system status", e)
                    connection_failed = True
                    continue
                scan_successful = scan_missing(
                    inst,
                    inst["min_age_hours"],
                    inst["max_age_hours"],
                    inst["interval_minutes"] * 60,
                )
                if not scan_successful:
                    connection_failed = True
            if connection_failed:
                sys.exit(1)
        else:
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
