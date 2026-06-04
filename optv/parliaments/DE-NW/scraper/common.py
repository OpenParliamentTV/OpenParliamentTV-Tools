"""Shared HTTP + HTML-parsing helpers for the DE-NW scrapers.

The Landtag NRW mediathek (``www.landtag.nrw.de``) serves each plenary session
as **static, server-rendered HTML** at
``/home/mediathek/video.html?kid={session-UUID}`` (the research doc's "headless
browser required / site blocks crawlers" is wrong — a plain UA-spoofed GET
works). One page carries the full per-speech spine:

- the session number sits in an ``<h2>117. Plenarsitzung</h2>`` header and the
  session start in a ``<time datetime="2026-01-30T10:00:00+02:00">`` element;
- one **HLS stream per session** at ``/videos/{kid}/playlist.m3u8`` (master →
  360p/720p/1080p) — there is no per-speech or per-TOP file, so per-speech
  windows are offsets into that one stream (the SE/DE-SH ``#t=start,end`` model);
- one **speech** per ``<!-- TEST-REDNER: Redner{mdlId=…, funktionId=…, name=…,
  fraktion=…, funktion=…, topNr=…, speechNr=…, parentTitle=…} -->`` debug comment
  immediately followed by ``<a href="?kid={UUID}&top-redner-id={id}">``. ``mdlId``
  is the parliament-native MdL id (a person identifier, like DE-NI's ``abg_id``);
  chair/government speakers carry a ``funktionId`` (``18PC1``, ``18MUG1``) +
  ``funktion`` (``Präsident``/``Minister``/``Ministerin``) instead, with a null
  ``mdlId``. ``fraktion`` is the party; ``topNr`` groups speeches into agenda
  items (the TOP title is the nearest preceding ``<h3 class="e-top__title">``).

The base page only shows minute-resolution display times. The **precise**
per-speech start offset (seconds) is rendered by the player only when a single
speech is selected: requesting ``…&top-redner-id={id}`` emits
``player.offset({ start: 70, end: 4278, … })``. We therefore fetch one extra
page per speech (``parse_offset``) for the precise ``start``; the rendered
``end`` is unreliable for speeches that double as a TOP "full length" link, so
the merger synthesises each window's end from the next speech's start (the
DE-BW approach).

We use stdlib urllib with a polite User-Agent + a small retry/backoff loop.
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

BASE = "https://www.landtag.nrw.de"

_LAST_FETCH_AT = 0.0
POLITE_DELAY = 0.5


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
            if e.code == 404:
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
    """GET a URL with retry/backoff. ``None`` on 404."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    return _request(req, retry_count=retry_count, timeout=timeout, base_delay=base_delay)


def fetch_text(url: str, *, retry_count: int = 10, timeout: float = 60.0,
               encoding: str = "utf-8") -> str | None:
    body = fetch(url, retry_count=retry_count, timeout=timeout)
    return None if body is None else body.decode(encoding, errors="replace")


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

def video_page_url(kid: str, redner_id: str | int | None = None) -> str:
    url = f"{BASE}/home/mediathek/video.html?kid={kid}"
    if redner_id is not None:
        url += f"&top-redner-id={redner_id}"
    return url


def archive_page_url(page: int) -> str:
    return (f"{BASE}/home/mediathek/archivierte-aufzeichnungen.html"
            f"?art=plenarsitzung&page={page}")


def hls_url(kid: str) -> str:
    return f"{BASE}/videos/{kid}/playlist.m3u8"


_KID_RE = re.compile(r'kid=(?P<kid>[0-9a-fA-F-]{36})')
_UUID_RE = re.compile(
    r'\b(?P<kid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\b')


def parse_kid(url_or_text: str) -> str | None:
    """Extract a session UUID from a ``…kid={uuid}`` URL **or** a bare UUID."""
    s = url_or_text or ""
    m = _KID_RE.search(s) or _UUID_RE.search(s)
    return m.group("kid") if m else None


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r'<[^>]+>')


def _text(fragment: str) -> str:
    """Strip tags + collapse whitespace from an HTML fragment."""
    return re.sub(r'\s+', ' ', unescape(_TAG_RE.sub(' ', fragment))).strip()


# ---------------------------------------------------------------------------
# Archive listing page
# ---------------------------------------------------------------------------

_ARCHIVE_CARD_RE = re.compile(
    r'<a\s+href="[^"]*\?kid=(?P<kid>[0-9a-fA-F-]{36})"[^>]*>(?P<body>.*?)</a>',
    re.S)
_SITZUNG_TXT_RE = re.compile(r'(?P<nr>\d{1,3})\.\s*(?:Plenar)?[Ss]itzung')
_DATE_TXT_RE = re.compile(r'(?P<d>\d{2})\.(?P<m>\d{2})\.(?P<y>\d{4})')
_PAGE_REF_RE = re.compile(r'[?&]page=(\d+)')


def archive_max_page(html: str) -> int | None:
    """Highest ``?page=N`` referenced on an archive listing page."""
    pages = [int(m.group(1)) for m in _PAGE_REF_RE.finditer(html or "")]
    return max(pages) if pages else None


def parse_archive_page(html: str) -> list[dict]:
    """Parse one archive listing page into ``[{kid, sitzung, date}]``.

    ``sitzung``/``date`` are best-effort (read from the teaser card text and
    used only for WP scoping + logging); ``fetch_media`` re-derives the
    authoritative Sitzung number and date from each session's own video page.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for m in _ARCHIVE_CARD_RE.finditer(html or ""):
        kid = m.group("kid")
        if kid in seen:
            continue
        seen.add(kid)
        body = _text(m.group("body"))
        sm = _SITZUNG_TXT_RE.search(body)
        dm = _DATE_TXT_RE.search(body)
        out.append({
            "kid": kid,
            "sitzung": int(sm.group("nr")) if sm else None,
            "date": f"{dm.group('y')}-{dm.group('m')}-{dm.group('d')}" if dm else None,
        })
    return out


# ---------------------------------------------------------------------------
# Session video page
# ---------------------------------------------------------------------------

# The session start (full ISO datetime, with the source's TZ stamp).
_SESSION_DT_RE = re.compile(
    r'<time[^>]*datetime="(?P<dt>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})"')
# "117. Plenarsitzung" (may be followed by ", TOP 1" on a redner-selected page).
_SITZUNG_HDR_RE = re.compile(r'(?P<nr>\d{1,3})\.\s*Plenarsitzung')
# The per-speech debug comment. ``[^}]*`` is safe — the struct has no nested braces.
_REDNER_RE = re.compile(r'TEST-REDNER:\s*Redner\{(?P<body>[^}]*)\}')
_FIELD_RE = re.compile(r"(\w+)='([^']*)'")
_REDNER_ID_RE = re.compile(r'top-redner-id=(\d+)')
# Agenda-item title block.
_TOP_TITLE_RE = re.compile(r'<h3 class="[^"]*e-top__title[^"]*"[^>]*>(?P<t>.*?)</h3>', re.S)
# Per-speech display time (minute resolution), used only as an offset fallback.
_DISPLAY_TIME_RE = re.compile(
    r'class="[^"]*item__time"[^>]*>\s*(?:<!--.*?-->)?\s*(?P<t>\d{1,2}:\d{2})\s*</time>', re.S)
# The precise per-speech offset, rendered only on a redner-selected page.
_OFFSET_RE = re.compile(r'player\.offset\(\{\s*start:\s*(?P<start>\d+)\s*,\s*end:\s*(?P<end>\d+)')


def _null(value: str | None) -> str:
    """Normalise the source's ``'null'`` / empty markers to an empty string."""
    v = (value or "").strip()
    return "" if v.lower() == "null" else v


def parse_offset(html: str) -> tuple[int | None, int | None]:
    """``(start, end)`` seconds from a redner-selected page's ``player.offset``."""
    m = _OFFSET_RE.search(html or "")
    if not m:
        return None, None
    return int(m.group("start")), int(m.group("end"))


def parse_video_page(html: str) -> dict | None:
    """Parse a Landtag NRW session video page into session meta + speeches.

    Returns ``None`` when the page carries no ``TEST-REDNER`` speech spine.
    Each speech dict carries its raw struct fields plus the resolved
    ``top_redner_id`` (the seek link) and ``top_title`` (nearest preceding
    ``e-top__title`` header). Speeches are returned in document order, which is
    chronological (it matches the increasing offsets).
    """
    redner = list(_REDNER_RE.finditer(html))
    if not redner:
        return None

    dt_m = _SESSION_DT_RE.search(html)
    session_start_iso = dt_m.group("dt") if dt_m else None
    sz_m = _SITZUNG_HDR_RE.search(html)
    sitzung = int(sz_m.group("nr")) if sz_m else None

    titles = [(m.start(), _text(m.group("t"))) for m in _TOP_TITLE_RE.finditer(html)]
    times = [(m.start(), m.group("t")) for m in _DISPLAY_TIME_RE.finditer(html)]

    def _nearest_before(pairs, pos):
        out = None
        for p, v in pairs:
            if p < pos:
                out = v
            else:
                break
        return out

    speeches: list[dict] = []
    for idx, m in enumerate(redner):
        fields = dict(_FIELD_RE.findall(m.group("body")))
        rid_m = _REDNER_ID_RE.search(html, m.end())
        top_redner_id = rid_m.group(1) if rid_m else None
        speeches.append({
            "index": idx,
            "top_redner_id": top_redner_id,
            "mdl_id": _null(fields.get("mdlId")),
            "funktion_id": _null(fields.get("funktionId")),
            "name": _null(fields.get("name")),
            "fraktion": _null(fields.get("fraktion")),
            "funktion": _null(fields.get("funktion")),
            "top_nr": _null(fields.get("topNr")),
            "speech_nr": _null(fields.get("speechNr")),
            "parent_title": _null(fields.get("parentTitle")),
            "top_title": _nearest_before(titles, m.start()) or "",
            "display_time": _nearest_before(times, m.start()),
        })

    return {
        "session_start_iso": session_start_iso,
        "sitzung": sitzung,
        "speeches": speeches,
    }
