"""European Parliament Open Data Portal API client (data.europarl.europa.eu).

Replaces the AWS-WAF-bypassing CRE HTML scraper. The API serves the same
verbatim plenary debates as structured JSON-LD with an embedded XML fragment
per speech — including the EN translation, speaker person ref, faction, and
millisecond-precision timing.

The bulk speech listing returns full ``xml_fragment`` payloads inline, so one
plenary day (~459 speeches) needs roughly 6 paginated calls + 1 meeting call
+ a handful of agenda-item calls — well under the 500-req/5min cap.

Rate limit (per EP docs): 500 requests / 5 min per endpoint. We pace at ~0.7s
between calls (with a 10-call burst budget) to stay comfortably below it.

See <https://data.europarl.europa.eu/api/v2/> for the live OpenAPI spec.
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

API_BASE = "https://data.europarl.europa.eu/api/v2"

# Activity-type discriminators used by the EP API.
PLENARY_SITTING = "PLENARY_SITTING"
PLENARY_DEBATE_SPEECH = "PLENARY_DEBATE_SPEECH"


class EPApiClient:
    """Thin urllib wrapper for the EP Open Data Portal API.

    The ``User-Agent`` header is required by the service and should identify
    the OpenParliamentTV deployment for traffic-source attribution.
    """

    def __init__(
        self,
        user_agent: str = "OpenParliamentTV-Tools/0.1 (+https://github.com/OpenParliamentTV)",
        *,
        cache_dir: Path | None = None,
        min_interval: float = 0.7,
        burst: int = 10,
        timeout: float = 60.0,
    ):
        self.user_agent = user_agent
        self.cache_dir = cache_dir
        self.min_interval = min_interval
        self.burst = max(1, burst)
        self.timeout = timeout
        # Token-bucket: deque of recent request timestamps.
        self._stamps: deque[float] = deque(maxlen=self.burst)

    # ---- low-level request ----

    def _throttle(self) -> None:
        now = time.monotonic()
        if len(self._stamps) == self.burst:
            oldest = self._stamps[0]
            wait = self.min_interval * self.burst - (now - oldest)
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

    def get_json(self, path: str, params: dict | None = None) -> dict:
        """GET an API path and return the parsed JSON-LD payload."""
        if path.startswith("http"):
            url = path
        else:
            url = API_BASE + "/" + path.lstrip("/")
        if params:
            url = url + ("&" if "?" in url else "?") + urlencode(params)

        cache_path = self._cache_path(url)
        if cache_path is not None and cache_path.exists():
            logger.debug("EP API cache hit: %s", url)
            return json.loads(cache_path.read_text())

        self._throttle()
        req = Request(url, headers={
            "Accept": "application/ld+json",
            "User-Agent": self.user_agent,
        })
        logger.debug("EP API GET %s", url)
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                body = resp.read()
        except HTTPError as e:
            if e.code == 204:
                return {"data": []}
            raise
        if not body:
            return {"data": []}
        payload = json.loads(body)
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(body)
        return payload

    # ---- pagination ----

    def _iter_pages(self, path: str, params: dict) -> Iterator[dict]:
        """Yield ``data`` items across all pages, following hydra/searchResults links."""
        offset = 0
        limit = int(params.get("limit", 100))
        params = dict(params)
        while True:
            params["offset"] = offset
            params["limit"] = limit
            page = self.get_json(path, params)
            items = page.get("data") or []
            if not items:
                return
            for it in items:
                yield it
            total = (page.get("meta") or {}).get("total")
            if total is None or offset + len(items) >= int(total):
                return
            offset += len(items)

    # ---- high-level helpers ----

    def list_plenary_sittings(self, year: int) -> list[dict]:
        """Return plenary sittings for the given calendar year (filtered to
        ``had_activity_type == PLENARY_SITTING``, sorted by date)."""
        items = []
        for it in self._iter_pages("meetings", {"year": year, "limit": 100}):
            if _activity_type(it) == PLENARY_SITTING:
                items.append(it)
        items.sort(key=lambda x: x.get("activity_date") or "")
        return items

    def get_meeting(self, meeting_id: str) -> dict:
        """Return a single plenary meeting record (the day envelope)."""
        page = self.get_json(f"meetings/{meeting_id}")
        items = page.get("data") or []
        if not items:
            raise LookupError(f"No meeting found for {meeting_id}")
        return items[0]

    def list_agenda_items(self, meeting_id: str) -> dict[str, dict]:
        """Return the agenda items of a meeting keyed by their ITM id.

        Walks the meeting's ``consists_of`` for ``PVCRE-ITM-*`` refs and fetches
        each via ``/events/{id}`` to get the multilingual ``activity_label``.
        """
        meeting = self.get_meeting(meeting_id)
        consists = meeting.get("consists_of") or []
        items: dict[str, dict] = {}
        for ref in consists:
            ref_id = ref.rsplit("/", 1)[-1] if isinstance(ref, str) else None
            if not ref_id or "PVCRE-ITM-" not in ref_id:
                continue
            try:
                event = self.get_event(ref_id)
            except (HTTPError, URLError, LookupError) as e:
                logger.warning("agenda-item fetch failed for %s: %s", ref_id, e)
                continue
            items[ref_id] = event
        return items

    def get_event(self, event_id: str) -> dict:
        page = self.get_json(f"events/{event_id}")
        items = page.get("data") or []
        if not items:
            raise LookupError(f"No event found for {event_id}")
        return items[0]

    def iter_day_speeches(self, date_yyyymmdd: str) -> Iterator[dict]:
        """Iterate every speech of one plenary day with full ``xml_fragment``
        payloads inline. ``date_yyyymmdd`` is YYYYMMDD."""
        date_dashed = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
        params = {
            "sitting-date": date_dashed,
            "sitting-date-end": date_dashed,
            "include-output": "xml_fragment",
            "limit": 100,
        }
        yield from self._iter_pages("speeches", params)


# ---- pure helpers (no client state) ----

def _activity_type(item: dict) -> str:
    """Extract the bare activity-type identifier, dropping the EP IRI prefix."""
    t = item.get("had_activity_type") or ""
    return t.rsplit("/", 1)[-1] if t else ""


def strip_iri_prefix(value: str | None, prefix_marker: str = "person/") -> str | None:
    """Return the suffix after ``prefix_marker`` in an EP IRI, or None."""
    if not value:
        return None
    if prefix_marker in value:
        return value.split(prefix_marker, 1)[1]
    return value


def ref_to_id(value: str | None) -> str | None:
    """Last segment of a slashed EP reference (e.g. ``eli/dl/event/MTG-...`` → ``MTG-...``)."""
    if not value:
        return None
    return value.rsplit("/", 1)[-1]
