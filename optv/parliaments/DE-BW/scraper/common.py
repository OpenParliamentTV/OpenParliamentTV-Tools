"""Shared HTTP + HTML-parsing helpers for the DE-BW scrapers.

The Landtag BW mediathek (``www.landtag-bw.de``, TYPO3) is plain public HTML
with no anti-bot. Two endpoints matter:

- the **filterlist** AJAX widget
  (``/ajax/filterlist/de/mediathek/videos/videos-508226``) returns the most
  recent session video cards as JSON-with-embedded-HTML; we scrape the
  ``/de/mediathek/videos/{slug}`` links from it (incremental-update path), and
- each **session video page** (``/de/mediathek/videos/{nr}-sitzung-vom-…-{id}``)
  carries, in *static* HTML, the session MP4 URL plus a ``e-chapterList``: one
  ``e-accordion`` per Tagesordnungspunkt, each with one ``<li>`` per speech
  holding ``changeTimestamp(seconds)`` (start offset into the session MP4), the
  speaker (in ``Lastname Firstname`` order), a ``| Role | Faction`` meta string
  and a ``HH:MM:SS`` display time.

There is no per-speech MP4 — DE-BW is the SE/DE-SH per-speech-offset model (one
session recording, per-speech windows addressed by ``#t=start,end``). We use
stdlib urllib with a polite User-Agent + a small retry/backoff loop.
"""

from __future__ import annotations

import logging
import re
import time
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

BASE = "https://www.landtag-bw.de"
# The video list is rendered by a filterList widget bound to this endpoint.
# A plain GET returns the newest page (PAGE_SIZE cards) + a date-facet block.
# The widget's "load more" (APPEND) sets ``offset`` to the number of items
# already shown, so the full archive is walkable via ``?offset=N`` GET requests
# stepping by PAGE_SIZE (``noStaticItems=true`` drops the pinned/static cards
# so the windows tile cleanly). See fetch_session_urls().
FILTERLIST_URL = f"{BASE}/ajax/filterlist/de/mediathek/videos/videos-508226"
PAGE_SIZE = 12

_LAST_FETCH_AT = 0.0
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


# ---------------------------------------------------------------------------
# Filterlist (recent session enumeration)
# ---------------------------------------------------------------------------

# /de/mediathek/videos/118-sitzung-vom-13-maerz-2025-563198
_VIDEO_LINK_RE = re.compile(
    r'/de/mediathek/videos/(?P<slug>\d+-sitzung-vom-[a-z0-9-]+-(?P<pageid>\d+))')


def parse_filterlist(html: str) -> list[str]:
    """Return absolute session-video page URLs found in a filterlist response.

    Deduplicated, preserving first-seen order. Only ``…-sitzung-vom-…`` slugs
    are kept (skips ``oea-…``, ``videos-…`` and other non-plenary cards).
    """
    seen: dict[str, None] = {}
    for m in _VIDEO_LINK_RE.finditer(html):
        seen.setdefault(f"{BASE}/de/mediathek/videos/{m.group('slug')}", None)
    return list(seen)


_RESULTS_TOTAL_RE = re.compile(r'"numberOfResults"\s*:\s*(\d+)')


def fetch_session_urls(*, retry_count: int = 20, max_results: int | None = None) -> list[str]:
    """Walk the whole video archive via ``?offset=`` pagination.

    Returns all session-video page URLs (deduplicated, newest first). The first
    page reports ``numberOfResults``; we then step ``offset`` by ``PAGE_SIZE``
    until that total is covered (or ``max_results`` is hit). ~88 requests cover
    the full ~1054-item BW archive; the result is cached by fetch_archive so
    this only runs on a fresh/forced build.
    """
    seen: dict[str, None] = {}

    def page(offset: int) -> str:
        return fetch_text(f"{FILTERLIST_URL}?offset={offset}&noStaticItems=true",
                          retry_count=retry_count)

    first = page(0)
    for u in parse_filterlist(first):
        seen.setdefault(u, None)
    m = _RESULTS_TOTAL_RE.search(first)
    total = int(m.group(1)) if m else len(seen)
    if max_results is not None:
        total = min(total, max_results)
    logger.info(f"Filterlist reports {total} result(s); paginating by {PAGE_SIZE}…")

    offset = PAGE_SIZE
    while offset < total:
        links = parse_filterlist(page(offset))
        if not links:
            logger.warning(f"Empty page at offset={offset}; stopping early")
            break
        for u in links:
            seen.setdefault(u, None)
        offset += PAGE_SIZE
    logger.info(f"Discovered {len(seen)} distinct session URL(s)")
    return list(seen)


# ``118-sitzung-vom-13-maerz-2025-563198`` → sitzung 118 + date. The date is
# also read authoritatively from the MP4 URL once the page is fetched; the slug
# gives a provisional Sitzung number + date used for period-scoping beforehand.
_SLUG_SITZUNG_RE = re.compile(r'/videos/(?P<sitzung>\d+)-sitzung-vom-')
_SLUG_DATE_RE = re.compile(
    r'/videos/\d+-sitzung-vom-(?P<day>\d+)-(?P<month>[a-zäöü]+)-(?P<year>\d{4})-\d+')
_MONTHS = {
    "januar": 1, "februar": 2, "maerz": 3, "märz": 3, "april": 4, "mai": 5,
    "juni": 6, "juli": 7, "august": 8, "september": 9, "oktober": 10,
    "november": 11, "dezember": 12,
}


def slug_sitzung(url: str) -> int | None:
    m = _SLUG_SITZUNG_RE.search(url)
    return int(m.group("sitzung")) if m else None


def slug_date(url: str) -> str | None:
    """``…-13-maerz-2025-…`` → ``"2025-03-13"`` (None if unparseable)."""
    m = _SLUG_DATE_RE.search(url)
    if not m:
        return None
    month = _MONTHS.get(m.group("month").lower())
    if not month:
        return None
    return f"{int(m.group('year')):04d}-{month:02d}-{int(m.group('day')):02d}"


# ---------------------------------------------------------------------------
# Session video page (per-speech chapter list)
# ---------------------------------------------------------------------------

# https://ltbw-stream.babiel.com/wahlperiode17/2025/sitzung118_20250313/Aufzeichnung_118_1.mp4
_MP4_RE = re.compile(
    r'https://ltbw-stream\.babiel\.com/wahlperiode(?P<wp>\d+)/(?P<year>\d{4})/'
    r'sitzung(?P<sitzung>\d+)_(?P<ymd>\d{8})/Aufzeichnung_(?P<nr>\d+)_(?P<part>\d+)\.mp4')

# One TOP block. Each Tagesordnungspunkt is an ``e-accordion`` whose header
# carries the title; we slice the chapter list at each title.
_ACCORDION_TITLE_RE = re.compile(
    r'<h3 class="e-accordion__title">(?P<title>.*?)</h3>', re.S)
# The description sits in the first ``e-accordion__rte`` after the title.
_DESC_RE = re.compile(
    r'class="e-accordion__rte[^"]*">(?P<desc>.*?)</div>', re.S)
# Per-speech list items.
_LI_RE = re.compile(r'<li>(?P<li>.*?)</li>', re.S)
_OFFSET_RE = re.compile(r'changeTimestamp\((?P<sec>\d+)\)')
# Speaker name span: class ends exactly at "-strong" (the time span is
# "-strong -small", so the trailing quote disambiguates).
_NAME_RE = re.compile(r'timestampText -strong">(?P<name>[^<]*)</span>')
_META_RE = re.compile(r'timestampText">(?P<meta>[^<]*)</span>')
_TIME_RE = re.compile(r'timestampText -strong -small">(?P<time>[^<]*)</span>')
_TAG_RE = re.compile(r'<[^>]+>')


def _text(fragment: str) -> str:
    """Strip tags + collapse whitespace from an HTML fragment."""
    return re.sub(r'\s+', ' ', unescape(_TAG_RE.sub(' ', fragment))).strip()


def _parse_speech(li_html: str) -> dict | None:
    off = _OFFSET_RE.search(li_html)
    name = _NAME_RE.search(li_html)
    if not off or not name:
        return None
    meta = _META_RE.search(li_html)
    tm = _TIME_RE.search(li_html)
    return {
        "name_raw": _text(name.group("name")),
        "meta_raw": _text(meta.group("meta")) if meta else "",
        "start_offset": int(off.group("sec")),
        "clock": _text(tm.group("time")) if tm else "",
    }


def parse_video_page(html: str) -> dict | None:
    """Parse a session video page into MP4 metadata + per-TOP speech list.

    Returns ``None`` if the page carries no babiel MP4 (e.g. a livestream
    placeholder or an unexpected layout).
    """
    mp4 = _MP4_RE.search(html)
    if not mp4:
        logger.warning("No babiel MP4 URL on page — skipping")
        return None
    ymd = mp4.group("ymd")
    iso_date = f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

    # Restrict to the chapter list so layout chrome can't leak in.
    cl = re.search(r'<div class="e-chapterList".*', html, re.S)
    chapter_html = cl.group(0) if cl else html

    titles = list(_ACCORDION_TITLE_RE.finditer(chapter_html))
    tops: list[dict] = []
    for idx, tm in enumerate(titles):
        start = tm.end()
        end = titles[idx + 1].start() if idx + 1 < len(titles) else len(chapter_html)
        block = chapter_html[start:end]
        desc_m = _DESC_RE.search(block)
        speeches = [s for li in _LI_RE.finditer(block)
                    if (s := _parse_speech(li.group("li")))]
        tops.append({
            "index": idx,
            "title": _text(tm.group("title")),
            "description": _text(desc_m.group("desc")) if desc_m else "",
            "speeches": speeches,
        })

    return {
        "mp4_url": mp4.group(0),
        "wp": int(mp4.group("wp")),
        "sitzung": int(mp4.group("sitzung")),
        "part": int(mp4.group("part")),
        "year": int(mp4.group("year")),
        "date": iso_date,
        "tops": tops,
    }
