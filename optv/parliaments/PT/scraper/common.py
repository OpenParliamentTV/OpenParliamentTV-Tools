#! /usr/bin/env python3
"""Shared HTTP helpers for the PT scrapers.

av.parlamento.pt (Metatheke) and debates.parlamento.pt are public and
unauthenticated but occasionally rate-limit / return transient 5xx, so all
fetches go through :func:`http_get` which retries with exponential backoff and a
polite User-Agent.
"""

from __future__ import annotations

import logging
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")


def http_get(url: str, *, timeout: float = 60.0, retry_count: int = 10,
             retry_delay_max: float = 10.0, binary: bool = False):
    """GET ``url`` with retries. Returns ``bytes`` if ``binary`` else ``str``.

    Retries on 429/5xx/transient network errors with exponential backoff capped
    at ``retry_delay_max``. Raises the last error if all attempts fail.
    """
    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=timeout) as resp:
                payload = resp.read()
            return payload if binary else payload.decode("utf-8", errors="replace")
        except HTTPError as e:
            last_err = e
            if e.code not in (429, 500, 502, 503, 504):
                raise
            logger.warning(f"GET {url} → HTTP {e.code} (attempt {attempt}/{retry_count})")
        except (URLError, TimeoutError, OSError) as e:
            last_err = e
            logger.warning(f"GET {url} failed: {e} (attempt {attempt}/{retry_count})")
        if attempt < retry_count:
            time.sleep(min(delay, retry_delay_max))
            delay *= 2
    raise RuntimeError(f"GET {url} failed after {retry_count} attempts: {last_err}")
