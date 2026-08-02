import types
import unittest
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import requests

import ServarrForceImporter as force_importer
import ServarrIndexerForceTester as indexer_tester
import SonarrMissingEpisodeSearcher as missing_searcher
import SonarrTBAEpisodeRefresher as TBA_refresher


CONNECTION_MODULES: list[types.ModuleType] = [
    force_importer,
    indexer_tester,
    missing_searcher,
    TBA_refresher,
]


def create_instance(name: str, URL: str) -> dict[str, Any]:
    return {
        "name": name,
        "type": "sonarr",
        "url": URL,
        "api_key": "test-key",
        "headers": {"X-Api-Key": "test-key"},
        "min_age_hours": 1,
        "max_age_hours": 24,
        "interval_minutes": 10.0,
    }


class StopAfterFirstWait:
    def __init__(self) -> None:
        self.stopped = False
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        self.stopped = True
        return True


class StopAfterOneRetry:
    def __init__(self) -> None:
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return len(self.waits) > 1

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        return len(self.waits) > 1


class ConnectionLoggingTests(unittest.TestCase):
    def tearDown(self) -> None:
        for module in CONNECTION_MODULES:
            module._connection_failure_log_times.clear()
        force_importer._pending_scan_instances.clear()
        indexer_tester._indexer_issue_states.clear()

    def test_refusals_are_condensed_rate_limited_and_followed_by_one_recovery(self) -> None:
        refusal = requests.ConnectionError(
            "HTTPConnectionPool: Max retries exceeded because the target actively refused it [WinError 10061]"
        )

        for module in CONNECTION_MODULES:
            with self.subTest(module=module.__name__):
                instance = create_instance("Offline Sonarr", f"http://{module.__name__}:8989")
                module._connection_failure_log_times.clear()

                with (
                    patch.object(
                        module.time,
                        "monotonic",
                        side_effect=[
                            100.0,
                            101.0,
                            100.0 + module.CONNECTION_FAILURE_REMINDER_SECONDS,
                        ],
                    ),
                    patch.object(module.logger, "warning") as warning_mock,
                    patch.object(module.logger, "info") as info_mock,
                ):
                    self.assertTrue(module._handle_request_failure(instance, "fetch health", refusal))
                    self.assertTrue(module._handle_request_failure(instance, "fetch health", refusal))
                    self.assertTrue(module._handle_request_failure(instance, "fetch health", refusal))
                    module._mark_connection_available(instance)
                    module._mark_connection_available(instance)

                self.assertEqual(2, warning_mock.call_count)
                self.assertNotIn("HTTPConnectionPool", str(warning_mock.call_args_list))
                info_mock.assert_called_once_with(
                    "[%s] Connection restored: %s",
                    instance["name"],
                    instance["url"],
                )


class RetryBehaviorTests(unittest.TestCase):
    def tearDown(self) -> None:
        for module in CONNECTION_MODULES:
            module._connection_failure_log_times.clear()
        force_importer._pending_scan_instances.clear()
        indexer_tester._indexer_issue_states.clear()

    def test_indexer_startup_continues_to_healthy_instances_after_refusal(self) -> None:
        offline_instance = create_instance("Offline", "http://offline:8989")
        online_instance = create_instance("Online", "http://online:8989")
        healthy_response = Mock()
        healthy_response.raise_for_status.return_value = None

        with (
            patch.object(
                indexer_tester.requests,
                "get",
                side_effect=[requests.ConnectionError("connection refused"), healthy_response],
            ) as get_mock,
            patch.object(indexer_tester.logger, "warning"),
            patch.object(indexer_tester.logger, "info"),
        ):
            indexer_tester._check_initial_connectivity([offline_instance, online_instance])

        self.assertEqual(2, get_mock.call_count)
        self.assertIn(offline_instance["url"], indexer_tester._connection_failure_log_times)
        self.assertNotIn(online_instance["url"], indexer_tester._connection_failure_log_times)

    def test_indexer_healthy_status_is_logged_only_on_state_change(self) -> None:
        instance = create_instance("Healthy", "http://healthy:8989")

        with (
            patch.object(indexer_tester, "get_health", return_value=[]),
            patch.object(indexer_tester.logger, "info") as info_mock,
        ):
            indexer_tester.check_and_test_indexers(instance)
            indexer_tester.check_and_test_indexers(instance)

        info_mock.assert_called_once_with("[%s] All indexers healthy", instance["name"])

    def test_sonarr_workers_retry_connection_failures_after_one_minute(self) -> None:
        missing_instance = create_instance("Missing", "http://missing:8989")
        missing_stop_event = StopAfterFirstWait()
        with (
            patch.object(missing_searcher, "scan_missing", return_value=False),
            patch.object(missing_searcher, "_is_connection_unavailable", return_value=True),
        ):
            missing_searcher._run_instance_service(missing_instance, missing_stop_event)

        TBA_instance = create_instance("TBA", "http://tba:8989")
        TBA_stop_event = StopAfterFirstWait()
        with (
            patch.object(TBA_refresher, "scan_TBA_episodes", return_value=False),
            patch.object(TBA_refresher, "_is_connection_unavailable", return_value=True),
        ):
            TBA_refresher._run_instance_service(TBA_instance, TBA_stop_event)

        self.assertEqual([60], missing_stop_event.waits)
        self.assertEqual([60], TBA_stop_event.waits)

    def test_importer_retries_failed_startup_scans_until_they_succeed(self) -> None:
        instance = create_instance("Importer", "http://importer:8989")
        stop_event = StopAfterOneRetry()
        force_importer._queue_startup_scan_retry(instance)

        with (
            patch.object(
                force_importer,
                "_startup_scan_instance",
                return_value=True,
            ) as scan_mock,
            patch.object(force_importer.logger, "info"),
        ):
            force_importer._retry_startup_scans(stop_event)

        scan_mock.assert_called_once_with(instance)
        self.assertEqual([60, 60], stop_event.waits)
        self.assertFalse(force_importer._pending_scan_instances)

    def test_importer_starts_webhook_server_while_instance_is_offline(self) -> None:
        instance = create_instance("Importer", "http://importer:8989")
        server_context = MagicMock()
        server_context.__enter__.return_value.serve_forever.side_effect = KeyboardInterrupt()
        retry_thread = Mock()

        with (
            patch.object(
                force_importer.requests,
                "get",
                side_effect=requests.ConnectionError("connection refused"),
            ),
            patch.object(force_importer.threading, "Thread", return_value=retry_thread),
            patch.object(force_importer.socketserver, "TCPServer", return_value=server_context) as server_mock,
            patch.object(force_importer.logger, "warning"),
            patch.object(force_importer.logger, "info"),
        ):
            force_importer._run_service(
                {
                    "instances": [instance],
                    "listen_host": "127.0.0.1",
                    "listen_port": 9099,
                }
            )

        server_mock.assert_called_once_with(("127.0.0.1", 9099), force_importer.WebhookHandler)
        retry_thread.start.assert_called_once_with()
        retry_thread.join.assert_called_once_with(
            timeout=force_importer.THREAD_SHUTDOWN_TIMEOUT_SECONDS
        )


if __name__ == "__main__":
    unittest.main()
