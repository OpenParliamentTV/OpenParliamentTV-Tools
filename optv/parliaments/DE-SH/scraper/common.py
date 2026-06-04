"""Shared HTTP helpers for the DE-SH scrapers.

The m7k mediathek is plain nginx + PHP with no anti-bot. Selector
endpoints and ``result.php`` are jQuery ``.load()`` calls (POST,
``application/x-www-form-urlencoded``). The Plenarprotokoll listing is
GET HTML on ``landtag.ltsh.de``. We use stdlib urllib with a polite
User-Agent + a small retry/backoff loop.
"""

from __future__ import annotations

import logging
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

M7K_BASE = "https://m7k.ltsh.de"
LANDTAG_BASE = "https://www.landtag.ltsh.de"

# Internal WP IDs used by the m7k selectors. The displayed "WP18/19/20"
# in the UI maps to ``wp=4/5/6`` on the wire. Update when a new WP rolls
# over.
WP_INTERNAL_ID: dict[int, int] = {
    18: 4,
    19: 5,
    20: 6,
}


_LAST_FETCH_AT = 0.0
# Seconds between successive fetches. m7k has not been observed to rate-limit
# at 0.5s but we stay conservative to be a polite citizen of a Landtag IT
# department's shared infra.
POLITE_DELAY = 0.5


def _request(req: Request, *, retry_count: int, timeout: float,
             base_delay: float) -> bytes:
    global _LAST_FETCH_AT
    delay = base_delay
    last_err: Exception | None = None
    for attempt in range(1, max(retry_count, 1) + 1):
        wait = _LAST_FETCH_AT + POLITE_DELAY - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            with urlopen(req, timeout=timeout) as resp:
                _LAST_FETCH_AT = time.monotonic()
                return resp.read()
        except (HTTPError, URLError, TimeoutError) as e:
            _LAST_FETCH_AT = time.monotonic()
            last_err = e
            if attempt >= retry_count:
                break
            logger.warning(f"HTTP retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError(f"HTTP failed after {retry_count} attempts: {last_err}")


def fetch(url: str, *, retry_count: int = 10, timeout: float = 60.0,
          base_delay: float = 1.0) -> bytes:
    """GET a URL with retry/backoff."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    return _request(req, retry_count=retry_count, timeout=timeout, base_delay=base_delay)


def fetch_text(url: str, *, retry_count: int = 10, timeout: float = 60.0,
               encoding: str = "utf-8") -> str:
    return fetch(url, retry_count=retry_count, timeout=timeout).decode(encoding, errors="replace")


def post(url: str, data: dict[str, str], *, retry_count: int = 10,
         timeout: float = 60.0, base_delay: float = 1.0) -> bytes:
    """POST x-www-form-urlencoded with retry/backoff."""
    body = urlencode(data).encode("utf-8")
    req = Request(
        url,
        data=body,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html, */*",
        },
    )
    return _request(req, retry_count=retry_count, timeout=timeout, base_delay=base_delay)


def post_text(url: str, data: dict[str, str], *, retry_count: int = 10,
              timeout: float = 60.0, encoding: str = "utf-8") -> str:
    return post(url, data, retry_count=retry_count, timeout=timeout).decode(encoding, errors="replace")
