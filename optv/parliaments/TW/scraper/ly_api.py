"""Legislative Yuan API client (ly.govapi.tw/v2).

Operated by openfunltd (Taiwanese civic-tech NPO); aggregates the official
data.ly.gov.tw datasets and the IVOD service into a single JSON API, plus
adds per-speech AI transcripts (whisperx) and speaker diarization (pyannote).
Used in production by ``billy3321/ivod_transcript_db``.

Endpoint surface we rely on:

* ``GET /v2/ivods`` — list IVODs with filter (term/session/meeting code/date)
  and aggregation (``agg=...`` returns bucket counts instead of records).
* ``GET /v2/ivods/{ivod_id}`` — single IVOD with ``transcript.whisperx``,
  ``transcript.pyannote``, ``video_url`` (HLS), and meeting metadata.
* ``GET /v2/legislators`` — legislator roster for NEL bootstrap.

Filter parameters use Chinese identifiers (``屆``, ``會期``, ``會議資料.會議代碼``,
``日期``) — the API URL-encodes them, but we pass them via ``urlencode`` which
takes care of that automatically.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import deque
from pathlib import Path
from typing import Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

API_BASE = "https://ly.govapi.tw/v2"


class LYApiClient:
    """Thin urllib wrapper for ly.govapi.tw v2 with throttling + on-disk cache.

    No documented rate limit on the upstream — we pace at ~0.4s between calls
    with a 10-call burst so a one-plenary download (~30 GETs) stays under a
    polite ceiling without crawling.
    """

    def __init__(
        self,
        user_agent: str = "OpenParliamentTV-Tools/0.1 (+https://github.com/OpenParliamentTV)",
        *,
        cache_dir: Path | None = None,
        min_interval: float = 0.4,
        burst: int = 10,
        timeout: float = 60.0,
        retry_count: int = 5,
        retry_delay_max: float = 10.0,
    ):
        self.user_agent = user_agent
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.min_interval = min_interval
        self.burst = max(1, burst)
        self.timeout = timeout
        self.retry_count = max(1, retry_count)
        self.retry_delay_max = retry_delay_max
        self._stamps: deque[float] = deque(maxlen=self.burst)

    # ---- low-level ----

    def _throttle(self) -> None:
        now = time.monotonic()
        if len(self._stamps) == self.burst:
            wait = self.min_interval * self.burst - (now - self._stamps[0])
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
        elif self._stamps:
            wait = self.min_interval - (now - self._stamps[-1])
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
        self._stamps.append(now)

    def _cache_path(self, url: str) -> Path | None:
        if self.cache_dir is None:
            return None
        h = hashlib.sha1(url.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{h}.json"

    def get_json(self, path: str, params: dict | None = None,
                 *, use_cache: bool = True) -> dict:
        """GET an API path and return the parsed JSON payload."""
        if path.startswith("http"):
            url = path
        else:
            url = API_BASE + "/" + path.lstrip("/")
        if params:
            url = url + ("&" if "?" in url else "?") + urlencode(params)

        cache_path = self._cache_path(url) if use_cache else None
        if cache_path is not None and cache_path.exists():
            logger.debug("LY API cache hit: %s", url)
            return json.loads(cache_path.read_text())

        delay = 1.0
        last_exc: Exception | None = None
        for attempt in range(1, self.retry_count + 1):
            self._throttle()
            req = Request(url, headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            })
            logger.debug("LY API GET %s (attempt %d)", url, attempt)
            try:
                with urlopen(req, timeout=self.timeout) as resp:
                    body = resp.read()
                if not body:
                    return {"ivods": [], "legislators": [], "total": 0}
                payload = json.loads(body)
                if cache_path is not None:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(body)
                return payload
            except HTTPError as e:
                if 500 <= e.code < 600 and attempt < self.retry_count:
                    wait = min(delay, self.retry_delay_max)
                    logger.warning("HTTP %d on %s, retry %d/%d after %.1fs",
                                   e.code, url, attempt, self.retry_count, wait)
                    time.sleep(wait)
                    delay = min(delay * 2, self.retry_delay_max)
                    last_exc = e
                    continue
                raise
            except (URLError, TimeoutError, ConnectionError) as e:
                if attempt < self.retry_count:
                    wait = min(delay, self.retry_delay_max)
                    logger.warning("%s on %s: %s, retry %d/%d after %.1fs",
                                   type(e).__name__, url, e,
                                   attempt, self.retry_count, wait)
                    time.sleep(wait)
                    delay = min(delay * 2, self.retry_delay_max)
                    last_exc = e
                    continue
                raise
        raise RuntimeError(f"Exhausted {self.retry_count} attempts for {url}: {last_exc}")

    # ---- high-level helpers ----

    def list_ivods_for_meeting(self, meeting_code: str, *,
                               limit: int = 500) -> list[dict]:
        """All IVODs for one meeting (e.g. ``院會-11-5-11``).

        Returns the bare ``ivods`` list, sorted by ``開始時間`` (chronological).
        """
        payload = self.get_json("ivods", {
            "會議資料.會議代碼": meeting_code,
            "limit": limit,
            "sort": "開始時間",
        })
        return list(payload.get("ivods") or [])

    def list_plenary_meeting_codes(self, term: int, session_period: int) -> list[str]:
        """All plenary meeting codes for one (term, session_period).

        Aggregates by ``會議資料.會議代碼`` then filters to those starting with
        ``院會-``. Committee meetings (``委員會-*``) are intentionally excluded.
        """
        payload = self.get_json("ivods", {
            "屆": term,
            "會期": session_period,
            "agg": "會議資料.會議代碼",
            "limit": 0,
        })
        codes: list[str] = []
        for agg in payload.get("aggs") or []:
            for bucket in agg.get("buckets") or []:
                code = bucket.get("會議資料.會議代碼") or ""
                if code.startswith("院會-"):
                    codes.append(code)
        return sorted(set(codes))

    def list_session_periods(self, term: int) -> list[int]:
        """Distinct 會期 values that have IVODs recorded for a given term."""
        payload = self.get_json("ivods", {
            "屆": term,
            "agg": "會議資料.會期",
            "limit": 0,
        })
        periods: set[int] = set()
        for agg in payload.get("aggs") or []:
            for bucket in agg.get("buckets") or []:
                v = bucket.get("會議資料.會期")
                if v is None:
                    continue
                try:
                    periods.add(int(v))
                except (TypeError, ValueError):
                    pass
        return sorted(periods)

    def get_ivod(self, ivod_id: int | str) -> dict:
        """Fetch one IVOD with full ``transcript`` block. Returns the ``data`` dict."""
        payload = self.get_json(f"ivods/{ivod_id}")
        data = payload.get("data")
        if not data:
            raise LookupError(f"No IVOD detail for {ivod_id!r}")
        return data

    def iter_legislators(self, term: int, *, page_size: int = 500) -> Iterator[dict]:
        """Iterate legislators of one term, paginating until exhausted."""
        page = 1
        while True:
            payload = self.get_json("legislators", {
                "屆": term, "limit": page_size, "page": page,
            })
            rows = payload.get("legislators") or []
            if not rows:
                return
            for row in rows:
                yield row
            total_page = payload.get("total_page") or 0
            if page >= total_page:
                return
            page += 1
