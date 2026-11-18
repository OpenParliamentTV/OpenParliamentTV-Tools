"""Shared HTTP + HTML-parsing helpers for the DE-HH scrapers.

The Hamburg mediathek (``mediathek.buergerschaft-hh.de``, built and run by
Intermediate Engineering, im-en.com) serves each plenary session at a
predictable URL ``/sitzung/{WP}/{Sitzung}/`` (e.g. ``/sitzung/23/18/``), so
discovery is a ``1..max`` enumeration. The upper bound for the current term is
the highest sitting linked from the plenary list ``/sitzungen/plenar`` (the most
recent sittings only).

The session page is **static HTML** carrying a full per-speech spine:

- one **agenda item** (TOP) per ``<video id="sessionitem-{UUID}">``: the
  ``data-cleanStreamUrls`` attribute holds a Python-dict-syntax list with a
  server-side-clipped HLS master URL
  (``/hls/clipFrom/{ms}/clipTo/{ms}/{date}/clean_{UUID}/…/master.m3u8``, the
  offsets in ms into the day's recording); a sibling ``data-signStreamUrls``
  carries the sign-language variant (its own clip range). The TOP number sits in
  a ``div.agendaItemNumber`` ("TOP 38"), the title in an
  ``<h3 class="… topheader" aria-label="…">``. The whole-sitting video
  (``<video id="session-{UUID}">``) is not an agenda item and is skipped.
- one **speech** per ``<div class="speech" …>`` inside that item's
  ``<ul class="speeches">``: ``data-speechPk`` (UUID), ``data-speechLegacyId``
  (numeric in WP 22, ``None`` in WP 23), ``data-start`` + ``data-duration``
  (seconds, relative to the item's HLS clip), ``data-sessionItemId`` (the owning
  TOP), ``data-speakerNameWithoutFunction`` and ``data-speakerFunction``
  (faction, or a government role). Each speech ``<li>`` also carries a
  ``video-download/?start={unix}&stop={unix}`` link whose timestamps are the
  **real wall-clock** speech start/stop.

The server cuts any range of the day's recording: the item's clip URL with
``clipFrom``/``clipTo`` narrowed to a speech (``speech_clip_url``) is that
speech's own HLS clip. From 2022-06-15 the clip URL's urlset also lists a WebVTT
file (``lang/deu/{date}_clean.vtt``), and the master then carries a subtitle
rendition whose cue times count from the clip start (``has_subtitles``).

We use stdlib urllib with a polite User-Agent + a small retry/backoff loop.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from html import unescape
from zoneinfo import ZoneInfo

from optv.shared.http import PoliteHTTP

logger = logging.getLogger(__name__)

BASE = "https://mediathek.buergerschaft-hh.de"

_http = PoliteHTTP(polite_delay=0.5, none_on_404=True)
fetch = _http.fetch
fetch_text = _http.fetch_text


def session_url(period: int, sitzung: int) -> str:
    return f"{BASE}/sitzung/{period}/{sitzung}/"


_SESSION_REF_RE = re.compile(r'/sitzung/(?P<wp>\d+)/(?P<nr>\d+)/?')


def parse_session_ref(url: str) -> tuple[int, int] | None:
    """``…/sitzung/23/18/`` → ``(23, 18)`` (Wahlperiode, Sitzung)."""
    m = _SESSION_REF_RE.search(url or "")
    return (int(m.group("wp")), int(m.group("nr"))) if m else None


PLENAR_LIST_URL = f"{BASE}/sitzungen/plenar"


def max_session_in(html: str, period: int) -> int | None:
    """Highest ``/sitzung/{period}/{n}/`` link in ``html``, or ``None``."""
    nums = [int(m.group("nr")) for m in _SESSION_REF_RE.finditer(html or "")
            if int(m.group("wp")) == period]
    return max(nums) if nums else None


def listing_max_session(period: int, *, retry_count: int = 20) -> int | None:
    """Read the highest ``/sitzung/{period}/{n}/`` link from the plenary list
    (``/sitzungen/plenar``, the most recent sittings). Used to bound the
    discovery enumeration for the *current* term."""
    html = fetch_text(PLENAR_LIST_URL, retry_count=retry_count)
    return max_session_in(html, period) if html else None


# ---------------------------------------------------------------------------
# Session page parsing
# ---------------------------------------------------------------------------

_VIDEO_RE = re.compile(
    r'<video\b[^>]*\bid="sessionitem-(?P<sid>[0-9a-fA-F-]+)"[^>]*>', re.S)
_CLEAN_SRC_RE = re.compile(r'data-cleanStreamUrls="(?P<v>[^"]*)"')
_SIGN_SRC_RE = re.compile(r'data-signStreamUrls="(?P<v>[^"]*)"')
# The attribute value is a Python-dict-style list (single quotes), not JSON:
# [{'src': '/hls/…/master.m3u8', 'type': 'application/x-mpegURL'}]
_SRC_IN_SOURCES_RE = re.compile(r"'src'\s*:\s*'(?P<src>[^']+)'")
# /hls/clipFrom/644000/clipTo/944000/2026-02-11/clean_…/…/master.m3u8
_DATE_IN_HLS_RE = re.compile(r'/clip(?:From|To)/\d+/clip(?:From|To)/\d+/(?P<date>\d{4}-\d{2}-\d{2})/')
_CLIP_RANGE_RE = re.compile(r'/clipFrom/(?P<from>\d+)/clipTo/(?P<to>\d+)/')
# A WebVTT file in the clip's urlset (",…,lang/deu/2026-02-11_clean.vtt,.urlset")
# means the HLS master carries a subtitle rendition.
_VTT_IN_URLSET_RE = re.compile(r',[^,/]*(?:/[^,/]+)*\.vtt,')

BERLIN = ZoneInfo("Europe/Berlin")


def speech_clip_url(item_hls: str, start: float, end: float) -> str:
    """The server-cut HLS clip of one speech.

    ``item_hls`` is an agenda item's clip URL (``/clipFrom/{a}/clipTo/{b}/``, ms
    into the day's recording); ``start``/``end`` are the speech's seconds
    relative to that clip (``data-start`` and ``data-start + data-duration``).
    The result narrows the range to ``a + start·1000 … a + end·1000``. A negative
    ``start`` (a speech beginning just before its TOP's clip) is kept: the server
    cuts from the day's recording. Returns ``""`` when ``item_hls`` carries no
    clip range.
    """
    m = _CLIP_RANGE_RE.search(item_hls or "")
    if not m:
        return ""
    base = int(m.group("from"))
    clip_from = max(base + round(start * 1000), 0)
    clip_to = base + round(end * 1000)
    return (item_hls[:m.start()]
            + f"/clipFrom/{clip_from}/clipTo/{clip_to}/"
            + item_hls[m.end():])


def has_subtitles(hls: str) -> bool:
    """Whether a clip URL's urlset lists a WebVTT file (subtitle rendition)."""
    return bool(_VTT_IN_URLSET_RE.search(hls or ""))

# Title + TOP number (both duplicated in the responsive layout; we take the
# last occurrence before each video, which is one of the identical copies).
_TITLE_RE = re.compile(
    r'<h3 class="[^"]*topheader[^"]*"[^>]*aria-label="(?P<aria>[^"]*)"[^>]*>(?P<text>.*?)</h3>',
    re.S)
_AGENDA_NO_RE = re.compile(
    r'<div class="agendaItemNumber[^"]*">(?P<txt>.*?)</div>', re.S)
_TOP_NO_RE = re.compile(r'TOP\s*(?P<no>\d+[a-z]?)', re.I)

# One speech <li> (no nested <li>); carries the data-* div + a video-download link.
_LI_RE = re.compile(r'<li class="[^"]*speechGroupElement[^"]*">(?P<li>.*?)</li>', re.S)
_SPEECH_TAG_RE = re.compile(r'<div class="speech [^>]*>', re.S)
_DL_RE = re.compile(r'video-download/\?start=(?P<start>\d+)&(?:amp;)?stop=(?P<stop>\d+)')
_SESSION_UUID_RE = re.compile(r'/sitzung/(?P<uuid>[0-9a-fA-F-]{36})/video-download/')

_TAG_RE = re.compile(r'<[^>]+>')


def _text(fragment: str) -> str:
    """Strip tags + collapse whitespace from an HTML fragment."""
    return re.sub(r'\s+', ' ', unescape(_TAG_RE.sub(' ', fragment))).strip()


def _attr(tag: str, name: str) -> str | None:
    m = re.search(rf'{name}="([^"]*)"', tag)
    return m.group(1) if m else None


def _clean_title(aria: str, text: str) -> str:
    """Prefer the aria-label (clean title up to the "; Dauer …" suffix); fall
    back to the visible text with a trailing "(M:SS)" duration stripped."""
    aria = re.sub(r'\s+', ' ', unescape(aria)).strip()
    if aria:
        return re.split(r';\s*Dauer', aria, maxsplit=1)[0].strip()
    txt = _text(text)
    return re.sub(r'\s*\(\d+:\d{2}(?::\d{2})?\)\s*$', '', txt).strip()


def _abs(src: str) -> str:
    if not src:
        return ""
    return src if src.startswith("http") else f"{BASE}{src}"


def _hls_src(sources_attr: str | None) -> str:
    if not sources_attr:
        return ""
    m = _SRC_IN_SOURCES_RE.search(unescape(sources_attr))
    return _abs(m.group("src")) if m else ""


def _parse_speech_li(li_html: str) -> dict | None:
    tag_m = _SPEECH_TAG_RE.search(li_html)
    if not tag_m:
        return None
    tag = tag_m.group(0)
    pk = _attr(tag, "data-speechPk")
    sid = _attr(tag, "data-sessionItemId")
    if not pk or not sid:
        return None
    try:
        start = float(_attr(tag, "data-start") or 0)
        duration = float(_attr(tag, "data-duration") or 0)
    except ValueError:
        start, duration = 0.0, 0.0
    dl = _DL_RE.search(li_html)
    legacy = (_attr(tag, "data-speechLegacyId") or "").strip()
    return {
        "speech_pk": pk,
        "legacy_id": int(legacy) if legacy.isdigit() else None,
        "session_item_id": sid,
        "start_offset": start,
        "duration": duration,
        "name_raw": unescape(_attr(tag, "data-speakerNameWithoutFunction") or "").strip(),
        "function": unescape(_attr(tag, "data-speakerFunction") or "").strip(),
        "download_start": int(dl.group("start")) if dl else None,
        "download_stop": int(dl.group("stop")) if dl else None,
    }


def parse_session_page(html: str) -> dict | None:
    """Parse a Hamburg session page into agenda items + per-speech records.

    Returns ``None`` if the page carries no agenda-item video (e.g. a livestream
    placeholder or an unexpected layout).
    """
    videos = list(_VIDEO_RE.finditer(html))
    if not videos:
        logger.warning("No sessionitem video on page — skipping")
        return None

    # Speeches grouped by their owning TOP (data-sessionItemId).
    speeches_by_sid: dict[str, list[dict]] = {}
    for li in _LI_RE.finditer(html):
        sp = _parse_speech_li(li.group("li"))
        if sp:
            speeches_by_sid.setdefault(sp["session_item_id"], []).append(sp)

    # Title + TOP number positions, for "last before each video" lookup.
    titles = [(m.start(), _clean_title(m.group("aria"), m.group("text")))
              for m in _TITLE_RE.finditer(html)]
    numbers = []
    for m in _AGENDA_NO_RE.finditer(html):
        mm = _TOP_NO_RE.search(_text(m.group("txt")))
        numbers.append((m.start(), mm.group("no") if mm else None))

    def _last_before(pairs, pos, lo):
        out = None
        for p, v in pairs:
            if lo < p < pos:
                out = v
        return out

    items: list[dict] = []
    prev = -1
    iso_date = ""
    session_uuid = ""
    for idx, vm in enumerate(videos):
        sid = vm.group("sid")
        tag = vm.group(0)
        clean = _hls_src(_CLEAN_SRC_RE.search(tag).group("v") if _CLEAN_SRC_RE.search(tag) else None)
        sign = _hls_src(_SIGN_SRC_RE.search(tag).group("v") if _SIGN_SRC_RE.search(tag) else None)
        if not iso_date and clean:
            dm = _DATE_IN_HLS_RE.search(clean)
            if dm:
                iso_date = dm.group("date")
        speeches = sorted(speeches_by_sid.get(sid, []),
                          key=lambda s: (s["start_offset"], s["speech_pk"]))
        items.append({
            "index": idx,
            "sid": sid,
            "top_number": _last_before(numbers, vm.start(), prev),
            "title": _last_before(titles, vm.start(), prev) or "",
            "clean_hls": clean,
            "sign_hls": sign,
            "speeches": speeches,
        })
        prev = vm.start()

    sm = _SESSION_UUID_RE.search(html)
    if sm:
        session_uuid = sm.group("uuid")

    if not iso_date:
        # No dated clip URL: fall back to the local date of the first speech's
        # wall-clock start.
        starts = [s["download_start"] for sps in speeches_by_sid.values()
                  for s in sps if s.get("download_start")]
        if starts:
            iso_date = datetime.fromtimestamp(min(starts), tz=BERLIN).date().isoformat()

    return {
        "date": iso_date,
        "session_uuid": session_uuid,
        "items": items,
    }
