"""Tests for ingestion.glue_crawler_trigger.GlueCrawlerTrigger."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from ingestion.glue_crawler_trigger import (
    CrawlerFailedError,
    GlueCrawlerTrigger,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_glue_client(states: list[str]) -> MagicMock:
    """Return a mock Glue client that cycles through the given crawler states."""
    client = MagicMock()

    # Build successive get_crawler responses
    responses = [
        {"Crawler": {"State": state, "LastCrawl": {"Status": state}}}
        for state in states
    ]
    client.get_crawler.side_effect = responses

    # start_crawler succeeds by default
    client.start_crawler.return_value = {}

    # Expose the exception class the real client would have
    client.exceptions.CrawlerRunningException = Exception

    return client


# ── CrawlerFailedError ────────────────────────────────────────────────────────

class TestCrawlerFailedError:
    def test_message_contains_crawler_name(self):
        err = CrawlerFailedError("my-crawler", {"ErrorMessage": "S3 access denied"})
        assert "my-crawler" in str(err)

    def test_message_contains_error_message(self):
        err = CrawlerFailedError("my-crawler", {"ErrorMessage": "S3 access denied"})
        assert "S3 access denied" in str(err)

    def test_stores_last_crawl_info(self):
        info = {"ErrorMessage": "oops", "Status": "FAILED"}
        err = CrawlerFailedError("my-crawler", info)
        assert err.last_crawl_info == info

    def test_stores_crawler_name(self):
        err = CrawlerFailedError("my-crawler", {})
        assert err.crawler_name == "my-crawler"

    def test_no_error_message_key_does_not_raise(self):
        # last_crawl_info may be empty on some failures
        err = CrawlerFailedError("c", {})
        assert "No error message" in str(err)


# ── GlueCrawlerTrigger init ───────────────────────────────────────────────────

class TestInit:
    def test_stores_crawler_name(self):
        client = _make_glue_client(["READY"])
        trigger = GlueCrawlerTrigger("my-crawler", glue_client=client)
        assert trigger.crawler_name == "my-crawler"

    def test_default_poll_interval(self):
        client = _make_glue_client(["READY"])
        trigger = GlueCrawlerTrigger("x", glue_client=client)
        assert trigger.poll_interval_seconds == 15

    def test_custom_poll_interval(self):
        client = _make_glue_client(["READY"])
        trigger = GlueCrawlerTrigger("x", glue_client=client, poll_interval_seconds=5)
        assert trigger.poll_interval_seconds == 5

    def test_default_timeout(self):
        client = _make_glue_client(["READY"])
        trigger = GlueCrawlerTrigger("x", glue_client=client)
        assert trigger.timeout_seconds == 900


# ── run() — happy paths ───────────────────────────────────────────────────────

class TestRun:
    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_returns_last_crawl_on_success(self, mock_sleep):
        last_crawl = {"Status": "SUCCEEDED", "Summary": "10 tables crawled"}
        client = MagicMock()
        client.start_crawler.return_value = {}
        client.get_crawler.return_value = {
            "Crawler": {"State": "READY", "LastCrawl": last_crawl}
        }
        client.exceptions.CrawlerRunningException = Exception

        trigger = GlueCrawlerTrigger("c", glue_client=client)
        result = trigger.run()

        assert result == last_crawl

    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_calls_start_crawler_once(self, mock_sleep):
        client = _make_glue_client(["READY"])
        trigger = GlueCrawlerTrigger("my-crawler", glue_client=client)
        trigger.run()

        client.start_crawler.assert_called_once_with(Name="my-crawler")

    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_polls_until_terminal_state(self, mock_sleep):
        """Crawler goes RUNNING → RUNNING → READY; should poll 3 times."""
        client = _make_glue_client(["RUNNING", "RUNNING", "READY"])
        trigger = GlueCrawlerTrigger("c", glue_client=client, poll_interval_seconds=1)
        trigger.run()

        assert client.get_crawler.call_count == 3

    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_sleeps_between_polls(self, mock_sleep):
        client = _make_glue_client(["RUNNING", "READY"])
        trigger = GlueCrawlerTrigger("c", glue_client=client, poll_interval_seconds=7)
        trigger.run()

        mock_sleep.assert_called_with(7)

    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_already_running_exception_is_swallowed(self, mock_sleep):
        """If start_crawler raises CrawlerRunningException, we continue polling."""
        client = MagicMock()
        client.exceptions.CrawlerRunningException = RuntimeError
        client.start_crawler.side_effect = RuntimeError("already running")
        client.get_crawler.return_value = {
            "Crawler": {"State": "READY", "LastCrawl": {}}
        }

        trigger = GlueCrawlerTrigger("c", glue_client=client)
        result = trigger.run()  # should not raise

        assert result == {}


# ── run() — failure paths ─────────────────────────────────────────────────────

class TestRunFailures:
    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_raises_crawler_failed_error_on_failed_state(self, mock_sleep):
        client = MagicMock()
        client.start_crawler.return_value = {}
        client.exceptions.CrawlerRunningException = Exception
        client.get_crawler.return_value = {
            "Crawler": {
                "State": "FAILED",
                "LastCrawl": {"Status": "FAILED", "ErrorMessage": "Out of memory"},
            }
        }

        trigger = GlueCrawlerTrigger("c", glue_client=client)

        with pytest.raises(CrawlerFailedError) as exc_info:
            trigger.run()

        assert exc_info.value.crawler_name == "c"
        assert "Out of memory" in str(exc_info.value)

    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_raises_timeout_error_when_crawler_hangs(self, mock_sleep):
        """Crawler stays RUNNING forever; timeout should fire."""
        client = MagicMock()
        client.start_crawler.return_value = {}
        client.exceptions.CrawlerRunningException = Exception
        # Always returns RUNNING
        client.get_crawler.return_value = {
            "Crawler": {"State": "RUNNING", "LastCrawl": {}}
        }

        trigger = GlueCrawlerTrigger(
            "c",
            glue_client=client,
            poll_interval_seconds=10,
            timeout_seconds=25,  # expires after 3 polls (30s > 25s)
        )

        with pytest.raises(TimeoutError) as exc_info:
            trigger.run()

        assert "c" in str(exc_info.value)
        assert "RUNNING" in str(exc_info.value)

    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_timeout_zero_disables_timeout(self, mock_sleep):
        """timeout_seconds=0 means unlimited; crawler eventually succeeds."""
        # First 5 polls return RUNNING, then READY
        states = ["RUNNING"] * 5 + ["READY"]
        client = _make_glue_client(states)

        trigger = GlueCrawlerTrigger(
            "c",
            glue_client=client,
            poll_interval_seconds=1,
            timeout_seconds=0,
        )
        result = trigger.run()

        assert client.get_crawler.call_count == 6

    @patch("ingestion.glue_crawler_trigger.time.sleep")
    def test_failed_error_has_full_last_crawl_info(self, mock_sleep):
        last_crawl = {"Status": "FAILED", "ErrorMessage": "disk full", "DPUHour": 0.5}
        client = MagicMock()
        client.start_crawler.return_value = {}
        client.exceptions.CrawlerRunningException = Exception
        client.get_crawler.return_value = {
            "Crawler": {"State": "FAILED", "LastCrawl": last_crawl}
        }

        trigger = GlueCrawlerTrigger("c", glue_client=client)

        with pytest.raises(CrawlerFailedError) as exc_info:
            trigger.run()

        assert exc_info.value.last_crawl_info == last_crawl
