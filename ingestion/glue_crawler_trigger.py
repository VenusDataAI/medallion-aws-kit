"""GlueCrawlerTrigger — starts a Glue crawler and polls until completion."""

from __future__ import annotations

import logging
import time
from typing import Optional

import boto3
from botocore.client import BaseClient

logger = logging.getLogger(__name__)

# Terminal states returned by Glue
_TERMINAL_STATES = {"READY", "FAILED"}
_SUCCESS_STATE = "READY"
_FAILED_STATE = "FAILED"


class CrawlerFailedError(Exception):
    """Raised when a Glue crawler ends in the FAILED state."""

    def __init__(self, crawler_name: str, last_crawl_info: dict) -> None:
        self.crawler_name = crawler_name
        self.last_crawl_info = last_crawl_info
        error_message = last_crawl_info.get("ErrorMessage", "No error message available.")
        super().__init__(
            f"Crawler '{crawler_name}' finished in FAILED state: {error_message}"
        )


class GlueCrawlerTrigger:
    """Start a named Glue crawler and poll until it reaches a terminal state.

    Args:
        crawler_name: Name of the Glue crawler to trigger.
        glue_client: Optional pre-configured boto3 Glue client. A new client
            using the default credential chain is created when not provided.
        poll_interval_seconds: Seconds between status polls (default: 15).
        timeout_seconds: Maximum seconds to wait before raising TimeoutError
            (default: 900 — 15 minutes). Set to 0 to disable.
    """

    def __init__(
        self,
        crawler_name: str,
        glue_client: Optional[BaseClient] = None,
        poll_interval_seconds: int = 15,
        timeout_seconds: int = 900,
    ) -> None:
        self.crawler_name = crawler_name
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds
        self._glue: BaseClient = glue_client or boto3.client("glue")

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> dict:
        """Start the crawler and block until it finishes.

        Returns:
            The ``LastCrawl`` dict from the final ``GetCrawler`` response.

        Raises:
            CrawlerFailedError: If the crawler ends in FAILED state.
            TimeoutError: If the crawler has not finished within ``timeout_seconds``.
        """
        self._start_crawler()
        return self._poll_until_done()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _start_crawler(self) -> None:
        """Issue StartCrawler; ignore AlreadyRunningException gracefully."""
        try:
            self._glue.start_crawler(Name=self.crawler_name)
            logger.info("Started crawler '%s'.", self.crawler_name)
        except self._glue.exceptions.CrawlerRunningException:
            logger.info(
                "Crawler '%s' is already running — will poll for completion.",
                self.crawler_name,
            )

    def _poll_until_done(self) -> dict:
        """Poll GetCrawler until the crawler reaches a terminal state."""
        elapsed = 0.0

        while True:
            response = self._glue.get_crawler(Name=self.crawler_name)
            crawler = response["Crawler"]
            state: str = crawler.get("State", "UNKNOWN")
            last_crawl: dict = crawler.get("LastCrawl", {})

            logger.debug("Crawler '%s' state: %s", self.crawler_name, state)

            if state in _TERMINAL_STATES:
                if state == _FAILED_STATE:
                    raise CrawlerFailedError(self.crawler_name, last_crawl)
                logger.info(
                    "Crawler '%s' completed successfully.", self.crawler_name
                )
                return last_crawl

            # Not done yet — wait before next poll
            if self.timeout_seconds > 0 and elapsed >= self.timeout_seconds:
                raise TimeoutError(
                    f"Crawler '{self.crawler_name}' did not finish within "
                    f"{self.timeout_seconds}s (current state: {state})."
                )

            time.sleep(self.poll_interval_seconds)
            elapsed += self.poll_interval_seconds
