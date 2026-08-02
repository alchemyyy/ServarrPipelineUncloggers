import argparse
import concurrent.futures
import json
import logging
import logging.handlers
import math
import os
import sys
import threading
import time
from datetime import date, datetime, timezone
from typing import Any, Optional, TypedDict

import requests


SCRIPT_NAME = "SonarrTBAEpisodeRefresher"
CONFIG_FILENAME = f"{SCRIPT_NAME}.json"
LOG_FILENAME = f"{SCRIPT_NAME}.log"
DEFAULT_INTERVAL_MINUTES = 6 * 60
HTTP_REQUEST_TIMEOUT_SECONDS = 15
HTTP_STATUS_TIMEOUT_SECONDS = 5
SERVICE_RESTART_DELAY_SECONDS = 10
CONNECTION_FAILURE_REMINDER_SECONDS = 15 * 60
CONNECTION_RETRY_SECONDS = 60
TBA_TITLE_CASEFOLDED = "tba"

_connection_failure_log_times: dict[str, float] = {}
_connection_failure_lock = threading.Lock()


class SonarrInstance(TypedDict):
    name: str
    url: str
    api_key: str
    headers: dict[str, str]
    interval_minutes: float


class ServiceConfig(TypedDict):
    instances: list[SonarrInstance]


DEFAULT_CONFIG: dict[str, object] = {
    "instances": [
        {
            "name": "Sonarr",
            "url": "http://192.168.1.20:8989",
            "api_key": "YOUR_SONARR_API_KEY",
            "interval_minutes": DEFAULT_INTERVAL_MINUTES,
        },
    ],
}


APPLICATION_DIRECTORY = (
    os.path.dirname(sys.executable)
    if getattr(sys, "frozen", False)
    else os.path.dirname(os.path.abspath(__file__))
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(SCRIPT_NAME)

try:
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(APPLICATION_DIRECTORY, LOG_FILENAME),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.getLogger().addHandler(file_handler)
except Exception:
    pass


def _is_connection_failure(exception: requests.RequestException) -> bool:
    return isinstance(exception, (requests.ConnectionError, requests.Timeout))


def _describe_request_failure(exception: requests.RequestException) -> str:
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


def _handle_request_failure(
    instance: SonarrInstance,
    operation: str,
    exception: requests.RequestException,
) -> bool:
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


def _mark_connection_available(instance: SonarrInstance) -> None:
    with _connection_failure_lock:
        was_unavailable = _connection_failure_log_times.pop(instance["url"], None) is not None

    if was_unavailable:
        logger.info("[%s] Connection restored: %s", instance["name"], instance["url"])


def _is_connection_unavailable(instance: SonarrInstance) -> bool:
    with _connection_failure_lock:
        return instance["url"] in _connection_failure_log_times


def load_config(path: str) -> ServiceConfig:
    """Load and validate the JSON service configuration."""
    try:
        with open(path, "r", encoding="utf-8") as config_file:
            raw_config: Any = json.load(config_file)
    except FileNotFoundError:
        logger.info("Config file not found, generating default: %s", path)
        with open(path, "w", encoding="utf-8") as config_file:
            json.dump(DEFAULT_CONFIG, config_file, indent=2)
        logger.info("Edit the config file with your instance URLs and API keys, then restart.")
        sys.exit(0)
    except json.JSONDecodeError as exception:
        logger.error("Invalid JSON in config file: %s", exception)
        sys.exit(1)

    if not isinstance(raw_config, dict):
        logger.error("Config root must be a JSON object")
        sys.exit(1)

    raw_instances: Any = raw_config.get("instances", [])
    if not isinstance(raw_instances, list):
        logger.error("Config field 'instances' must be a JSON array")
        sys.exit(1)

    instances: list[SonarrInstance] = []
    for instance_index, raw_instance in enumerate(raw_instances):
        if not isinstance(raw_instance, dict):
            logger.error("Instance at index %d must be a JSON object", instance_index)
            sys.exit(1)

        name_value: Any = raw_instance.get("name", f"sonarr-{instance_index + 1}")
        URL_value: Any = raw_instance.get("url", "")
        API_key_value: Any = raw_instance.get("api_key", "")
        interval_value: Any = raw_instance.get("interval_minutes", DEFAULT_INTERVAL_MINUTES)

        if not isinstance(name_value, str) or not name_value.strip():
            logger.error("Instance at index %d has an invalid name", instance_index)
            sys.exit(1)
        instance_name = name_value.strip()

        if not isinstance(URL_value, str) or not URL_value.strip():
            logger.error("Instance '%s' is missing url", instance_name)
            sys.exit(1)
        instance_URL = URL_value.strip().rstrip("/")

        if not isinstance(API_key_value, str) or not API_key_value.strip():
            logger.error("Instance '%s' is missing api_key", instance_name)
            sys.exit(1)
        API_key = API_key_value.strip()

        if (
            isinstance(interval_value, bool)
            or not isinstance(interval_value, (int, float))
            or not math.isfinite(interval_value)
            or interval_value <= 0
        ):
            logger.error(
                "Instance '%s' has invalid interval_minutes; expected a finite number greater than zero",
                instance_name,
            )
            sys.exit(1)

        instances.append(
            {
                "name": instance_name,
                "url": instance_URL,
                "api_key": API_key,
                "headers": {"X-Api-Key": API_key},
                "interval_minutes": float(interval_value),
            }
        )

    if not instances:
        logger.error("No instances defined in config")
        sys.exit(1)

    return {"instances": instances}


def parse_record_list(payload: Any, endpoint_name: str) -> list[dict[str, Any]]:
    """Validate a Sonarr response containing a JSON array of objects."""
    if not isinstance(payload, list):
        raise ValueError(f"Sonarr {endpoint_name} response was not a JSON array")

    records: list[dict[str, Any]] = []
    for record in payload:
        if not isinstance(record, dict):
            raise ValueError(f"Sonarr {endpoint_name} response contained a non-object record")
        records.append(record)
    return records


def get_series(instance: SonarrInstance) -> list[dict[str, Any]]:
    """Fetch every series configured in a Sonarr instance."""
    response = requests.get(
        f"{instance['url']}/api/v3/series",
        headers=instance["headers"],
        timeout=HTTP_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    _mark_connection_available(instance)
    return parse_record_list(response.json(), "series")


def get_episodes(instance: SonarrInstance, series_ID: int) -> list[dict[str, Any]]:
    """Fetch every episode for one Sonarr series."""
    response = requests.get(
        f"{instance['url']}/api/v3/episode",
        headers=instance["headers"],
        params={"seriesId": series_ID},
        timeout=HTTP_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    _mark_connection_available(instance)
    return parse_record_list(response.json(), "episode")


def refresh_series_metadata(
    instance: SonarrInstance,
    series_IDs: list[int],
) -> requests.Response:
    """Queue one Sonarr metadata refresh command for the specified series."""
    response = requests.post(
        f"{instance['url']}/api/v3/command",
        headers=instance["headers"],
        json={"name": "RefreshSeries", "seriesIds": series_IDs},
        timeout=HTTP_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    _mark_connection_available(instance)
    return response


def get_episode_air_date(episode: dict[str, Any]) -> Optional[date]:
    """Return the episode's date-only air date, with UTC timestamp fallback."""
    air_date_value: Any = episode.get("airDate")
    if isinstance(air_date_value, str) and air_date_value:
        try:
            return date.fromisoformat(air_date_value)
        except ValueError:
            pass

    air_date_UTC_value: Any = episode.get("airDateUtc")
    if not isinstance(air_date_UTC_value, str) or not air_date_UTC_value:
        return None

    try:
        air_date_UTC = datetime.fromisoformat(air_date_UTC_value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if air_date_UTC.tzinfo is None:
        air_date_UTC = air_date_UTC.replace(tzinfo=timezone.utc)
    return air_date_UTC.astimezone().date()


def has_TBA_title(episode: dict[str, Any]) -> bool:
    """Return whether an episode title is exactly TBA, ignoring case and whitespace."""
    title_value: Any = episode.get("title")
    return isinstance(title_value, str) and title_value.strip().casefold() == TBA_TITLE_CASEFOLDED


def format_episode_code(episode: dict[str, Any]) -> str:
    """Format an episode's season and episode numbers for logging."""
    season_number: Any = episode.get("seasonNumber")
    episode_number: Any = episode.get("episodeNumber")
    if (
        isinstance(season_number, int)
        and not isinstance(season_number, bool)
        and isinstance(episode_number, int)
        and not isinstance(episode_number, bool)
    ):
        return f"S{season_number:02d}E{episode_number:02d}"
    return "S??E??"


def scan_TBA_episodes(
    instance: SonarrInstance,
    stop_event: Optional[threading.Event] = None,
) -> bool:
    """Find aired TBA episodes and refresh metadata for their series."""
    instance_name = instance["name"]
    current_date = datetime.now().astimezone().date()
    if not _is_connection_unavailable(instance):
        logger.info(
            "[%s] Scanning for TBA episodes with air dates on or before %s...",
            instance_name,
            current_date.isoformat(),
        )

    try:
        series_records = get_series(instance)
    except (requests.RequestException, ValueError) as exception:
        if isinstance(exception, requests.RequestException):
            _handle_request_failure(instance, "fetch series", exception)
        else:
            logger.error("[%s] Fetch series failed: %s", instance_name, exception)
        return False

    logger.info("[%s] Checking %d series", instance_name, len(series_records))
    matching_series_IDs: set[int] = set()
    matching_episode_count = 0
    scan_successful = True

    for series_record in series_records:
        if stop_event is not None and stop_event.is_set():
            logger.info("[%s] Scan interrupted", instance_name)
            return False

        series_ID_value: Any = series_record.get("id")
        if not isinstance(series_ID_value, int) or isinstance(series_ID_value, bool):
            logger.warning("[%s] Skipping series with an invalid ID", instance_name)
            scan_successful = False
            continue
        series_ID = series_ID_value

        series_title_value: Any = series_record.get("title")
        series_title = (
            series_title_value.strip()
            if isinstance(series_title_value, str) and series_title_value.strip()
            else f"Series {series_ID}"
        )

        try:
            episodes = get_episodes(instance, series_ID)
        except (requests.RequestException, ValueError) as exception:
            if isinstance(exception, requests.RequestException):
                connection_failed = _handle_request_failure(
                    instance,
                    f"fetch episodes for {series_title}",
                    exception,
                )
                if connection_failed:
                    return False
            else:
                logger.error(
                    "[%s] Fetch episodes for %s failed: %s",
                    instance_name,
                    series_title,
                    exception,
                )
            scan_successful = False
            continue

        series_match_count = 0
        for episode in episodes:
            if not has_TBA_title(episode):
                continue

            episode_air_date = get_episode_air_date(episode)
            if episode_air_date is None:
                logger.warning(
                    "[%s] Skipping %s %s because its air date is missing or invalid",
                    instance_name,
                    series_title,
                    format_episode_code(episode),
                )
                continue
            if episode_air_date > current_date:
                continue

            logger.info(
                "[%s] Found TBA episode: %s %s (air date %s)",
                instance_name,
                series_title,
                format_episode_code(episode),
                episode_air_date.isoformat(),
            )
            series_match_count += 1
            matching_episode_count += 1

        if series_match_count > 0:
            matching_series_IDs.add(series_ID)

    if not matching_series_IDs:
        logger.info("[%s] No eligible TBA episodes found", instance_name)
        return scan_successful

    sorted_series_IDs = sorted(matching_series_IDs)
    logger.info(
        "[%s] Found %d eligible TBA episode(s) across %d series; queuing metadata refresh",
        instance_name,
        matching_episode_count,
        len(sorted_series_IDs),
    )

    try:
        response = refresh_series_metadata(instance, sorted_series_IDs)
    except requests.RequestException as exception:
        connection_failed = _handle_request_failure(instance, "queue metadata refresh", exception)
        return not connection_failed

    logger.info(
        "[%s] Metadata refresh command accepted (HTTP %d) for %d series",
        instance_name,
        response.status_code,
        len(sorted_series_IDs),
    )
    return scan_successful


def _run_instance_service(instance: SonarrInstance, stop_event: threading.Event) -> None:
    """Run one Sonarr instance on its independent scan schedule."""
    interval_seconds = instance["interval_minutes"] * 60

    while not stop_event.is_set():
        scan_start = time.monotonic()
        scan_successful = scan_TBA_episodes(instance, stop_event)
        if not scan_successful and _is_connection_unavailable(instance):
            retry_seconds = min(CONNECTION_RETRY_SECONDS, interval_seconds)
            stop_event.wait(retry_seconds)
            continue

        elapsed_seconds = time.monotonic() - scan_start
        remaining_seconds = max(0.0, interval_seconds - elapsed_seconds)
        if remaining_seconds > 0 and not stop_event.is_set():
            logger.info(
                "[%s] Next scan in %.0f seconds...",
                instance["name"],
                remaining_seconds,
            )
            stop_event.wait(remaining_seconds)


def _run_service(config: ServiceConfig) -> None:
    """Start and supervise all configured Sonarr instance workers."""
    instances = config["instances"]

    logger.info("--- SonarrTBAEpisodeRefresher ---")
    for instance in instances:
        logger.info("  Instance: %s at %s", instance["name"], instance["url"])
        logger.info("    Scan interval: %.1f minutes", instance["interval_minutes"])
    logger.info("---------------------------------")

    for instance in instances:
        try:
            response = requests.get(
                f"{instance['url']}/api/v3/system/status",
                headers=instance["headers"],
                timeout=HTTP_STATUS_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            _mark_connection_available(instance)
            logger.info("[%s] Connected OK", instance["name"])
        except requests.RequestException as exception:
            _handle_request_failure(instance, "check system status", exception)

    stop_event = threading.Event()
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=len(instances),
        thread_name_prefix="sonarr-TBA-instance",
    )
    futures: list[concurrent.futures.Future[None]] = [
        executor.submit(_run_instance_service, instance, stop_event)
        for instance in instances
    ]

    try:
        while True:
            wait_result = concurrent.futures.wait(
                futures,
                timeout=1,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            completed_futures = wait_result.done
            if not completed_futures:
                continue

            for completed_future in completed_futures:
                completed_future.result()
            raise RuntimeError("An instance worker stopped unexpectedly")
    finally:
        stop_event.set()
        executor.shutdown(wait=True, cancel_futures=True)


def _run_forever(config: ServiceConfig) -> None:
    """Restart the service after unexpected failures until interrupted."""
    while True:
        try:
            _run_service(config)
            return
        except KeyboardInterrupt:
            logger.info("Shutting down.")
            return
        except Exception:
            logger.exception(
                "Service error; restarting in %ds",
                SERVICE_RESTART_DELAY_SECONDS,
            )
            try:
                time.sleep(SERVICE_RESTART_DELAY_SECONDS)
            except KeyboardInterrupt:
                logger.info("Shutting down.")
                return


def main() -> None:
    """Run the command-line service entry point."""
    try:
        parser = argparse.ArgumentParser(
            description=(
                "Scans Sonarr for aired episodes titled TBA and refreshes metadata "
                "for their series."
            )
        )
        parser.add_argument(
            "--config",
            default=os.path.join(APPLICATION_DIRECTORY, CONFIG_FILENAME),
            help=f"Path to JSON config file (default: {CONFIG_FILENAME} next to the exe/script)",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run a single scan and exit",
        )
        arguments = parser.parse_args()

        config = load_config(arguments.config)

        if arguments.once:
            all_scans_successful = True
            for instance in config["instances"]:
                try:
                    response = requests.get(
                        f"{instance['url']}/api/v3/system/status",
                        headers=instance["headers"],
                        timeout=HTTP_STATUS_TIMEOUT_SECONDS,
                    )
                    response.raise_for_status()
                    _mark_connection_available(instance)
                except requests.RequestException as exception:
                    _handle_request_failure(instance, "check system status", exception)
                    all_scans_successful = False
                    continue

                if not scan_TBA_episodes(instance):
                    all_scans_successful = False

            if not all_scans_successful:
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
