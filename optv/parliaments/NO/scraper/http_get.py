"""HTTP GET helper shared by the NO scrapers.

data.stortinget.no allows 100 API calls per minute and answers above that with
``429 Too Many Requests`` (no ``Retry-After`` documented). Every NO fetch goes
through :func:`fetch_bytes`, which paces calls per host (``data.stortinget.no``
at most one call per 0.7 s, i.e. below the limit) and retries 429, 5xx and
transient network errors. A 429 waits for ``Retry-After`` when the server sends
one, otherwise a full minute so the per-minute window has cleared.
"""

from __future__ import annotations

import logging
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from optv.shared.http import USER_AGENT

logger = logging.getLogger(__name__)


# Minimum seconds between two requests to the same host.
MIN_INTERVAL = {
    "data.stortinget.no": 0.7,
}
DEFAULT_MIN_INTERVAL = 0.3
RATE_LIMIT_WAIT = 60.0

_last_request: dict[str, float] = {}


def strip_bom(b: bytes) -> bytes:
    return b[3:] if b.startswith(b"\xef\xbb\xbf") else b


def _throttle(host: str) -> None:
    interval = MIN_INTERVAL.get(host, DEFAULT_MIN_INTERVAL)
    last = _last_request.get(host)
    if last is not None:
        wait = interval - (time.monotonic() - last)
        if wait > 0:
            time.sleep(wait)
    _last_request[host] = time.monotonic()


def _rate_limit_wait(e: HTTPError) -> float:
    headers = getattr(e, "headers", None)
    retry_after = headers.get("Retry-After") if headers is not None else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            pass
    return RATE_LIMIT_WAIT


def fetch_bytes(url: str, *, accept: str = "*/*", timeout: float = 60.0,
                retry_count: int = 10, retry_delay_max: float = 10.0) -> bytes:
    """GET ``url`` and return the body; retry 429, 5xx and network errors."""
    host = urlsplit(url).hostname or ""
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        _throttle(host)
        req = Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": accept,
            "Accept-Language": "nb,en;q=0.9",
        })
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except HTTPError as e:
            retryable = e.code == 429 or 500 <= e.code < 600
            if not retryable or attempt >= retry_count:
                raise
            if e.code == 429:
                wait = _rate_limit_wait(e)
            else:
                wait = min(delay, retry_delay_max)
                delay = min(delay * 2, retry_delay_max)
            logger.warning(f"HTTP {e.code} on {url}, retry {attempt}/{retry_count} after {wait:.1f}s")
            time.sleep(wait)
            last_exc = e
        except (URLError, TimeoutError, ConnectionError) as e:
            if attempt >= retry_count:
                raise
            wait = min(delay, retry_delay_max)
            delay = min(delay * 2, retry_delay_max)
            logger.warning(f"{type(e).__name__} on {url}: {e}, retry {attempt}/{retry_count} after {wait:.1f}s")
            time.sleep(wait)
            last_exc = e
    raise RuntimeError(f"Exhausted {retry_count} attempts for {url}: {last_exc}")
