"""Shared HTTP helpers for the DE-ST scrapers.

The Landtag portal is plain nginx + TYPO3 with no anti-bot — we use stdlib
urllib with a polite User-Agent and a small retry/backoff loop.
"""

from __future__ import annotations

import logging
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

LANDTAG_BASE = "https://www.landtag.sachsen-anhalt.de"


_LAST_FETCH_AT = 0.0
# Seconds between successive fetches. Empirically calibrated: at 0.25 s
# (~4 req/s) the Landtag's nginx blocks our IP after ~60 successful fetches
# within a minute; at 1.5 s (~0.7 req/s) it sustains indefinitely. Fast
# enough to finish a 241-speech Sitzungsperiode merge in ~6 minutes.
POLITE_DELAY = 1.5


def fetch(url: str, *, retry_count: int = 10, timeout: float = 60.0,
          base_delay: float = 1.0) -> bytes:
    """GET a URL with retry/backoff. Returns response body bytes.

    Enforces a global ``POLITE_DELAY`` between successive fetches — the
    Landtag's nginx will start refusing connections under sustained pressure
    (we hit ECONNREFUSED after a burst of unthrottled AJAX calls during
    initial testing). When connections are refused, backoff doubles up to
    60s to give the IP block time to clear.
    """
    global _LAST_FETCH_AT
    req = Request(url, headers={"User-Agent": USER_AGENT})
    delay = base_delay
    last_err: Exception | None = None
    for attempt in range(1, max(retry_count, 1) + 1):
        # Throttle.
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
            logger.warning(f"GET {url} retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError(f"GET {url} failed after {retry_count} attempts: {last_err}")


def fetch_text(url: str, *, retry_count: int = 10, timeout: float = 60.0,
               encoding: str = "utf-8") -> str:
    return fetch(url, retry_count=retry_count, timeout=timeout).decode(encoding, errors="replace")
