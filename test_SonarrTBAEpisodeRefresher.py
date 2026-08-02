import io
import json
import unittest
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import Mock, patch

import SonarrTBAEpisodeRefresher as refresher


def create_instance() -> refresher.SonarrInstance:
    return {
        "name": "Test Sonarr",
        "url": "http://sonarr.test:8989",
        "api_key": "test-key",
        "headers": {"X-Api-Key": "test-key"},
        "interval_minutes": 360.0,
    }


class ConfigTests(unittest.TestCase):
    def test_loads_instance_specific_intervals(self) -> None:
        raw_config: dict[str, Any] = {
            "instances": [
                {
                    "name": "Anime",
                    "url": "http://anime:8989/",
                    "api_key": "anime-key",
                    "interval_minutes": 120,
                },
                {
                    "name": "Television",
                    "url": "http://television:8989",
                    "api_key": "television-key",
                    "interval_minutes": 480,
                },
            ]
        }

        with patch("builtins.open", return_value=io.StringIO(json.dumps(raw_config))):
            config = refresher.load_config("unused.json")

        self.assertEqual(2, len(config["instances"]))
        self.assertEqual(120.0, config["instances"][0]["interval_minutes"])
        self.assertEqual(480.0, config["instances"][1]["interval_minutes"])
        self.assertEqual("http://anime:8989", config["instances"][0]["url"])

    def test_defaults_interval_to_six_hours(self) -> None:
        raw_config: dict[str, Any] = {
            "instances": [
                {
                    "name": "Sonarr",
                    "url": "http://sonarr:8989",
                    "api_key": "test-key",
                }
            ]
        }

        with patch("builtins.open", return_value=io.StringIO(json.dumps(raw_config))):
            config = refresher.load_config("unused.json")

        self.assertEqual(360.0, config["instances"][0]["interval_minutes"])


class EpisodeMatchingTests(unittest.TestCase):
    def test_air_date_prefers_date_value_and_falls_back_to_UTC(self) -> None:
        explicit_date = refresher.get_episode_air_date(
            {
                "airDate": "2026-07-31",
                "airDateUtc": "2026-08-01T06:00:00Z",
            }
        )
        fallback_timestamp = "2026-08-01T06:00:00Z"
        fallback_date = refresher.get_episode_air_date({"airDateUtc": fallback_timestamp})
        expected_fallback_date = (
            datetime.fromisoformat(fallback_timestamp.replace("Z", "+00:00"))
            .astimezone()
            .date()
        )

        self.assertEqual("2026-07-31", explicit_date.isoformat())
        self.assertEqual(expected_fallback_date, fallback_date)
        self.assertIsNone(refresher.get_episode_air_date({"airDate": "invalid"}))

    def test_scan_refreshes_matching_series_once_and_skips_future_air_dates(self) -> None:
        instance = create_instance()
        current_date = datetime.now().astimezone().date()
        previous_date = current_date - timedelta(days=1)
        future_date = current_date + timedelta(days=1)
        series_records = [
            {"id": 20, "title": "Current Series"},
            {"id": 10, "title": "Past Series"},
            {"id": 30, "title": "Future Series"},
        ]
        episodes_by_series: dict[int, list[dict[str, Any]]] = {
            20: [
                {
                    "title": " tba ",
                    "airDate": current_date.isoformat(),
                    "seasonNumber": 1,
                    "episodeNumber": 2,
                },
                {
                    "title": "TBA",
                    "airDate": previous_date.isoformat(),
                    "seasonNumber": 1,
                    "episodeNumber": 1,
                },
            ],
            10: [
                {
                    "title": "TBA",
                    "airDate": previous_date.isoformat(),
                    "seasonNumber": 2,
                    "episodeNumber": 3,
                }
            ],
            30: [
                {
                    "title": "TBA",
                    "airDate": future_date.isoformat(),
                    "seasonNumber": 1,
                    "episodeNumber": 1,
                },
                {
                    "title": "Published Title",
                    "airDate": previous_date.isoformat(),
                    "seasonNumber": 1,
                    "episodeNumber": 2,
                },
            ],
        }
        accepted_response = Mock(status_code=201)

        def get_test_episodes(
            unused_instance: refresher.SonarrInstance,
            series_ID: int,
        ) -> list[dict[str, Any]]:
            return episodes_by_series[series_ID]

        with (
            patch.object(refresher, "get_series", return_value=series_records),
            patch.object(refresher, "get_episodes", side_effect=get_test_episodes),
            patch.object(
                refresher,
                "refresh_series_metadata",
                return_value=accepted_response,
            ) as refresh_mock,
        ):
            scan_successful = refresher.scan_TBA_episodes(instance)

        self.assertTrue(scan_successful)
        refresh_mock.assert_called_once_with(instance, [10, 20])

    def test_refresh_command_uses_series_IDs(self) -> None:
        instance = create_instance()
        accepted_response = Mock()
        accepted_response.raise_for_status.return_value = None

        with patch.object(refresher.requests, "post", return_value=accepted_response) as post_mock:
            response = refresher.refresh_series_metadata(instance, [10, 20])

        self.assertIs(accepted_response, response)
        post_mock.assert_called_once_with(
            "http://sonarr.test:8989/api/v3/command",
            headers={"X-Api-Key": "test-key"},
            json={"name": "RefreshSeries", "seriesIds": [10, 20]},
            timeout=refresher.HTTP_REQUEST_TIMEOUT_SECONDS,
        )


if __name__ == "__main__":
    unittest.main()
