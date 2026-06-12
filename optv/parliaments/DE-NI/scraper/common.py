"""Shared HTTP helpers + Plenar-TV REST API client for the DE-NI scrapers.

Niedersachsen's Plenar-TV (``plenartv.de``) is a SvelteKit single-page app, but
it is backed by a **public, unauthenticated** typed REST API at
``https://api.plenartv.de`` — so we ignore the HTML entirely and call the API
directly (no headless browser, no scraping). The research doc's "no
machine-readable access / no official API" is wrong.

API surface we use (all GET, no auth for reads):

- ``/session/periode/{wp}/session/{tagungsabschnitt}`` → one Tagungsabschnitt
  (Plenar-TV's "session") with its ``meetings[]`` (each a Sitzung: ``id``,
  ``meetingDate``, ``meetingNumber``).
- ``/subject/date/{meetingDate}`` → the agenda items (TOPs, "subjects") of that
  Sitzung — but **without** ``speakerTimings``.
- ``/subject/{subject_id}`` → one subject **with** ``speakerTimings[]`` (the
  per-speech spine: ``abg_id``, ``surname``, ``name``, ``fraktion``,
  ``speechType``, ``startTimeInStreamSecs``, ``stopTimeInStreamSecs``).
- ``/vtt/{subject_id}`` → ``text/vtt`` time-aligned subtitles (when present; not
  used in v1).

Per-speech video is a server-side-clipped HLS playlist (verified, no auth):
``{VOD}/stream/{streamFileName}/index.m3u8?start={sec}&end={sec}`` where the
offsets are the speaker timing's stream seconds (+ the subject's ``video.offset``,
usually 0). The clip URL *is* the speech — no media fragment needed.

We use stdlib urllib with a polite User-Agent + a small retry/backoff loop.
"""

from __future__ import annotations

import json
import logging
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

API_BASE = "https://api.plenartv.de"
VOD_BASE = "https://vod.plenartv.de"
WEB_BASE = "https://plenartv.de"

_LAST_FETCH_AT = 0.0
POLITE_DELAY = 0.3


def _request(req: Request, *, retry_count: int, timeout: float,
             base_delay: float) -> bytes | None:
    """GET with retry/backoff. Returns ``None`` on a 404 (used by the prober)."""
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
        except HTTPError as e:
            _LAST_FETCH_AT = time.monotonic()
            if e.code in (404, 422):
                return None
            last_err = e
            if attempt >= retry_count:
                break
            logger.warning(f"HTTP retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
        except (URLError, TimeoutError) as e:
            _LAST_FETCH_AT = time.monotonic()
            last_err = e
            if attempt >= retry_count:
                break
            logger.warning(f"HTTP retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError(f"HTTP failed after {retry_count} attempts: {last_err}")


def fetch(url: str, *, retry_count: int = 10, timeout: float = 60.0,
          base_delay: float = 1.0) -> bytes | None:
    """GET a URL with retry/backoff. ``None`` on 404/422."""
    req = Request(url, headers={
        "User-Agent": USER_AGENT,
        "Origin": WEB_BASE,
        "Accept": "application/json",
    })
    return _request(req, retry_count=retry_count, timeout=timeout, base_delay=base_delay)


def get_json(path: str, *, retry_count: int = 10):
    """GET ``{API_BASE}{path}`` and parse JSON. ``None`` on 404/422."""
    body = fetch(f"{API_BASE}{path}", retry_count=retry_count)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


# ---------------------------------------------------------------------------
# Typed endpoint helpers
# ---------------------------------------------------------------------------

def get_session(period: int, tagungsabschnitt: int, *, retry_count: int = 10):
    """One Tagungsabschnitt with its ``meetings[]`` (or ``None`` if absent)."""
    return get_json(
        f"/session/periode/{period}/session/{tagungsabschnitt}",
        retry_count=retry_count)


def get_subjects_by_date(meeting_date: str, *, retry_count: int = 10) -> list:
    """The subjects (agenda items) of the Sitzung held on ``meeting_date``.

    Note: these carry agenda metadata + stream offsets but **not**
    ``speakerTimings`` — call :func:`get_subject` per subject for those.
    """
    return get_json(f"/subject/date/{meeting_date}", retry_count=retry_count) or []


def get_subject(subject_id: str, *, retry_count: int = 10):
    """One subject **with** ``speakerTimings[]`` (or ``None``)."""
    return get_json(f"/subject/{subject_id}", retry_count=retry_count)


def session_key(period: int, sitzung: int) -> str:
    """``(19, 80)`` → ``"19080"`` — the flat per-Sitzung Stage 2 session key."""
    return f"{period:02d}{sitzung:03d}"


def session_page_url(period: int, tagungsabschnitt: int, sitzung: int) -> str:
    """The public Plenar-TV web page for a Sitzung (used as ``sourcePage`` base)."""
    return f"{WEB_BASE}/tagungsabschnitt/{period}-{tagungsabschnitt}?sitzung={sitzung}"


def video_clip_uri(stream_file_name: str, start_sec, end_sec) -> str:
    """Per-speech server-side-clipped HLS playlist URL (verified, no auth).

    ``{VOD}/stream/{streamFileName}/index.m3u8?start={sec}&end={sec}`` — the clip
    boundaries are integer stream seconds. Returns ``""`` if the stream file or
    bounds are missing.
    """
    if not stream_file_name or start_sec is None or end_sec is None:
        return ""
    return (f"{VOD_BASE}/stream/{stream_file_name}/index.m3u8"
            f"?start={int(round(float(start_sec)))}&end={int(round(float(end_sec)))}")


def vtt_uri(subject_id: str) -> str:
    """Per-subject WebVTT subtitle URL (time-aligned text; parsed by vtt2json)."""
    return f"{API_BASE}/vtt/{subject_id}" if subject_id else ""
