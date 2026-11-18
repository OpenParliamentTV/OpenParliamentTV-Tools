"""Polite HTTP for the scrapers: spaced requests, retried with exponential backoff."""

from __future__ import annotations

import logging
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")


class PoliteHTTP:
    """GET and form POST against one site.

    Requests are spaced at least ``polite_delay`` seconds apart. A failed request (HTTP error, network error, timeout) is retried up to ``retry_count`` times, waiting ``base_delay`` seconds and doubling up to a minute; after the last attempt a ``RuntimeError`` is raised. With ``none_on_404`` a 404 answer returns ``None`` at once, for callers that probe whether a page exists.
    """

    def __init__(self, *, polite_delay: float = 0.5, none_on_404: bool = False):
        self.polite_delay = polite_delay
        self.none_on_404 = none_on_404
        self._last_request_at = 0.0

    def request(self, req: Request, *, retry_count: int, timeout: float,
                base_delay: float) -> bytes | None:
        delay = base_delay
        last_err: Exception | None = None
        for attempt in range(1, max(retry_count, 1) + 1):
            wait = self._last_request_at + self.polite_delay - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                with urlopen(req, timeout=timeout) as resp:
                    self._last_request_at = time.monotonic()
                    return resp.read()
            except (HTTPError, URLError, TimeoutError) as e:
                self._last_request_at = time.monotonic()
                if self.none_on_404 and isinstance(e, HTTPError) and e.code == 404:
                    return None
                last_err = e
                if attempt >= retry_count:
                    break
                logger.warning(f"HTTP retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
        raise RuntimeError(f"HTTP failed after {retry_count} attempts: {last_err}")

    def fetch(self, url: str, *, retry_count: int = 10, timeout: float = 60.0,
              base_delay: float = 1.0) -> bytes | None:
        """GET a URL."""
        req = Request(url, headers={"User-Agent": USER_AGENT})
        return self.request(req, retry_count=retry_count, timeout=timeout, base_delay=base_delay)

    def fetch_text(self, url: str, *, retry_count: int = 10, timeout: float = 60.0,
                   encoding: str = "utf-8") -> str | None:
        body = self.fetch(url, retry_count=retry_count, timeout=timeout)
        return None if body is None else body.decode(encoding, errors="replace")

    def post(self, url: str, data: dict[str, str], *, retry_count: int = 10,
             timeout: float = 60.0, base_delay: float = 1.0) -> bytes | None:
        """POST ``data`` as x-www-form-urlencoded, the way a page's own XHR does."""
        req = Request(
            url,
            data=urlencode(data).encode("utf-8"),
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "text/html, */*",
            },
        )
        return self.request(req, retry_count=retry_count, timeout=timeout, base_delay=base_delay)

    def post_text(self, url: str, data: dict[str, str], *, retry_count: int = 10,
                  timeout: float = 60.0, encoding: str = "utf-8") -> str | None:
        body = self.post(url, data, retry_count=retry_count, timeout=timeout)
        return None if body is None else body.decode(encoding, errors="replace")
