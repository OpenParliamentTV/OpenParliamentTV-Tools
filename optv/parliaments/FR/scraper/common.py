#! /usr/bin/env python3
"""Shared HTTP helpers for the FR scrapers.

The assemblee-nationale.fr web front rate-limits aggressive access (429s), so
all fetches go through :func:`http_get` / :func:`http_status`, which retry with
exponential backoff and a polite User-Agent. The open-data hosts
(``data.assemblee-nationale.fr``, ``www.assemblee-nationale.fr/dyn/opendata``)
are friendlier than the dynamic ``/dyn/`` web pages, but we treat them
uniformly.
"""

from __future__ import annotations

import logging
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from optv.shared.http import USER_AGENT

logger = logging.getLogger(__name__)


_RETRY_CODES = (429, 500, 502, 503, 504)


def _request(url: str, *, timeout: float, retry_count: int, retry_delay_max: float,
             return_client_errors: bool) -> tuple[int, bytes]:
    """GET ``url`` (redirects followed) with retries; return ``(status, body)``.

    Retries on 429/5xx/transient network errors with exponential backoff capped
    at ``retry_delay_max``. Any other HTTP error is raised, or returned as
    ``(code, b"")`` when ``return_client_errors`` is set. Raises the last error
    if all attempts fail.
    """
    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except HTTPError as e:
            last_err = e
            if e.code not in _RETRY_CODES:
                if return_client_errors:
                    return e.code, b""
                raise
            logger.warning(f"GET {url} → HTTP {e.code} (attempt {attempt}/{retry_count})")
        except (URLError, TimeoutError, OSError) as e:
            last_err = e
            logger.warning(f"GET {url} failed: {e} (attempt {attempt}/{retry_count})")
        if attempt < retry_count:
            time.sleep(min(delay, retry_delay_max))
            delay *= 2
    raise RuntimeError(f"GET {url} failed after {retry_count} attempts: {last_err}")


def http_get(url: str, *, timeout: float = 60.0, retry_count: int = 10,
             retry_delay_max: float = 10.0, binary: bool = False):
    """GET ``url`` with retries. Returns ``bytes`` if ``binary`` else ``str``."""
    _status, payload = _request(url, timeout=timeout, retry_count=retry_count,
                                retry_delay_max=retry_delay_max,
                                return_client_errors=False)
    return payload if binary else payload.decode("utf-8", errors="replace")


def http_status(url: str, *, timeout: float = 30.0, retry_count: int = 10,
                retry_delay_max: float = 10.0) -> int:
    """GET ``url`` with retries and return the final HTTP status.

    A non-retryable error status (e.g. 404 for a recording that went offline)
    is returned rather than raised, so callers can tell a missing resource from
    a transient failure.
    """
    status, _payload = _request(url, timeout=timeout, retry_count=retry_count,
                                retry_delay_max=retry_delay_max,
                                return_client_errors=True)
    return status
