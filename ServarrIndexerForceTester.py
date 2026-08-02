import argparse
import json
import logging
import os
import sys
import threading
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ServarrIndexerForceTester")

CONNECTION_FAILURE_REMINDER_SECONDS = 15 * 60
_connection_failure_log_times = {}
_connection_failure_lock = threading.Lock()
_indexer_issue_states = {}


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


# Config
DEFAULT_CONFIG = {
    "test_interval_seconds": 60,
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
        "test_interval_seconds": raw.get("test_interval_seconds", 60),
        "instances": instances,
    }


# Servarr API helpers
def get_health(instance):
    resp = requests.get(
        f"{instance['url']}/api/v3/health",
        headers=instance["headers"],
        timeout=15,
    )
    resp.raise_for_status()
    _mark_connection_available(instance)
    return resp.json()


def get_all_indexers(instance):
    resp = requests.get(
        f"{instance['url']}/api/v3/indexer",
        headers=instance["headers"],
        timeout=15,
    )
    resp.raise_for_status()
    _mark_connection_available(instance)
    return resp.json()


def test_indexer(instance, indexer_resource):
    resp = requests.post(
        f"{instance['url']}/api/v3/indexer/test",
        headers=instance["headers"],
        params={"forceTest": "true"},
        json=indexer_resource,
        timeout=30,
    )
    _mark_connection_available(instance)
    return resp


def parse_blocked_indexer_names(health_results):
    """Extract blocked indexer names from IndexerStatusCheck health warnings.

    Returns a set of names, or None if all indexers are reported unavailable
    (meaning we should test everything).
    """
    names = set()
    for entry in health_results:
        if entry.get("source") != "IndexerStatusCheck":
            continue
        message = entry.get("message", "")
        # The message contains indexer names after the last ": " separator.
        # e.g. "Indexers unavailable due to failures: NZBgeek, DrunkenSlug"
        # When ALL indexers are down, the message has no names (no colon).
        if ": " in message:
            names_part = message.rsplit(": ", 1)[1]
            for name in names_part.split(", "):
                stripped = name.strip()
                if stripped:
                    names.add(stripped)
        else:
            return None  # all unavailable
    return names if names else None


# Core test logic
def check_and_test_indexers(instance):
    inst_name = instance["name"]

    # Check health for blocked indexers
    try:
        health = get_health(instance)
    except requests.RequestException as e:
        _handle_request_failure(instance, "fetch health", e)
        return

    blocked_names = parse_blocked_indexer_names(health)
    has_indexer_issues = any(h.get("source") == "IndexerStatusCheck" for h in health)

    if not has_indexer_issues:
        if _indexer_issue_states.get(instance["url"]) is not False:
            logger.info("[%s] All indexers healthy", inst_name)
        _indexer_issue_states[instance["url"]] = False
        return

    _indexer_issue_states[instance["url"]] = True

    # Fetch indexers to find the ones that need testing
    try:
        indexers = get_all_indexers(instance)
    except requests.RequestException as e:
        _handle_request_failure(instance, "fetch indexers", e)
        return

    enabled = {idx["name"]: idx for idx in indexers if idx.get("enable", False)}

    if blocked_names is not None:
        # Test only the specific blocked indexers
        to_test = [enabled[n] for n in blocked_names if n in enabled]
        if not to_test:
            # Names didn't match (locale mismatch, etc.) — fall back to all enabled
            logger.warning("[%s] Could not match blocked indexer names, testing all %d enabled", inst_name, len(enabled))
            to_test = list(enabled.values())
        else:
            logger.info("[%s] %d blocked indexer(s): %s", inst_name, len(to_test), ", ".join(blocked_names))
    else:
        # All indexers reported unavailable
        to_test = list(enabled.values())
        logger.info("[%s] All indexers reported unavailable, testing all %d enabled", inst_name, len(to_test))

    if not to_test:
        return

    passed = 0
    failed = 0
    errors = 0

    for indexer in to_test:
        idx_name = indexer.get("name", f"id-{indexer.get('id', '?')}")
        try:
            resp = test_indexer(instance, indexer)
            if resp.ok:
                logger.info("[%s] PASS: %s", inst_name, idx_name)
                passed += 1
            else:
                logger.warning("[%s] FAIL: %s (HTTP %d)", inst_name, idx_name, resp.status_code)
                failed += 1
        except requests.RequestException as e:
            _handle_request_failure(instance, f"test indexer '{idx_name}'", e)
            errors += 1

    logger.info(
        "[%s] Test cycle complete: %d passed, %d failed, %d errors (of %d tested)",
        inst_name, passed, failed, errors, len(to_test),
    )


def _check_initial_connectivity(instances):
    for instance in instances:
        try:
            resp = requests.get(
                f"{instance['url']}/api/v3/system/status",
                headers=instance["headers"],
                timeout=5,
            )
            resp.raise_for_status()
            _mark_connection_available(instance)
            logger.info("[%s] Connected OK", instance["name"])
        except requests.RequestException as e:
            _handle_request_failure(instance, "check system status", e)


def main():
    parser = argparse.ArgumentParser(
        description="Periodically force-tests all indexers in Sonarr/Radarr to bypass backoff penalties."
    )
    parser.add_argument(
        "--config",
        default=os.path.join(
            os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__)),
            "ServarrIndexerForceTester.json",
        ),
        help="Path to JSON config file (default: ServarrIndexerForceTester.json next to the exe/script)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    instances = config["instances"]
    interval = config["test_interval_seconds"]

    logger.info("--- ServarrIndexerForceTester ---")
    for inst in instances:
        logger.info("  Instance: %s (%s) at %s", inst["name"], inst["type"], inst["url"])
    logger.info("  Test interval: %d seconds", interval)
    logger.info("--------------------------------")

    # Check initial connectivity without blocking healthy instances
    _check_initial_connectivity(instances)

    # Main polling loop
    logger.info("Starting test loop (every %d seconds)...", interval)
    try:
        while True:
            for inst in instances:
                check_and_test_indexers(inst)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Shutting down.")


if __name__ == "__main__":
    main()
