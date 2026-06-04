"""Shared HTTP + HTML-parsing helpers for the DE-SN scraper.

The Sächsischer Landtag mediathek (``www.landtag.sachsen.de``, an ASP.NET/CMS
site) is plain public HTML with no anti-bot. The plenary-video archive is a
single paginated list:

    /de/mediathek-und-publikationen/videos/plenarvideos/index.cshtml
        ?electoral_term_id={8|7|alle}&start=N           (20 items per page)

and — crucially — **each list item is fully self-contained**: one
``<article class="xm_teaser …">`` per speech carrying the speaker, the faction
badge, the speech-time category, the TOP number + a short ``thema`` text, the
Sitzungsnummer, the date + wall-clock time, the daily HLS ``<source>`` URL
(``stream-o01.envia-tel.net/vod/smil:{YYYYMMDD}.smil/playlist.m3u8``) and the
per-speech ``data-component-options='{"startPosition":S,"endPosition":E}'``
offsets (seconds into that daily stream). So there is **no per-speech GET** —
one pagination pass yields every field the pipeline needs.

DE-SN is the SE/DE-SH/DE-BW per-speech-offset model (one daily recording,
per-speech windows addressed by ``#t=start,end``), but cleaner: both start and
end offsets are present (no end-synthesis), and the item's wall-clock time gives
a real ``dateStart``. We use stdlib urllib with a polite User-Agent + a small
retry/backoff loop.
"""

from __future__ import annotations

import json
import logging
import re
import time
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

BASE = "https://www.landtag.sachsen.de"
LIST_URL = (f"{BASE}/de/mediathek-und-publikationen/videos/plenarvideos/"
            "index.cshtml")
EINZELBEITRAG_URL = (f"{BASE}/de/mediathek-und-publikationen/videos/plenarvideos/"
                     "videoeinzelbeitrag")
PAGE_SIZE = 20

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


def list_page_url(electoral_term_id: str, start: int) -> str:
    return f"{LIST_URL}?electoral_term_id={electoral_term_id}&start={start}"


# ---------------------------------------------------------------------------
# List-item parsing (one <article class="xm_teaser …"> per speech)
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r'<[^>]+>')
# Split the page into per-speech article blocks. The class list varies a little
# ("xm_teaser xm_teaser--compact"), so anchor on the opening tag prefix.
_ARTICLE_SPLIT_RE = re.compile(r'<article\s+class="xm_teaser')
# h2 title:  "Dirk Panter <span class="badge badge-party-5">SPD</span> - Sonderredezeit"
_TITLE_RE = re.compile(
    r'<h2\s+class="xm_teaser__title[^"]*">(?P<inner>.*?)</h2>', re.S)
_BADGE_RE = re.compile(r'<span\s+class="badge\s+badge-party-\d+">(?P<faction>[^<]*)</span>', re.S)
# subtitle items: TOP n / Sitzungsnummer NN / Sitzung des Plenums vom DD.MM.YYYY HH:MM:SS UHR
_TOP_RE = re.compile(r'TOP\s+(?P<top>\d+[a-z]?)', re.I)
_SITZUNG_RE = re.compile(r'Sitzungsnummer\s+(?P<nr>\d+)', re.I)
_DATETIME_RE = re.compile(
    r'vom\s+(?P<d>\d{2})\.(?P<m>\d{2})\.(?P<y>\d{4})\s+'
    r'(?P<H>\d{2}):(?P<M>\d{2}):(?P<S>\d{2})', re.I)
# short per-speech thema text in the teaser body
_THEMA_RE = re.compile(
    r'<div\s+class="xm_teaser__text[^"]*"[^>]*>\s*<p>(?P<thema>.*?)</p>', re.S)
# modal id == einzelbeitrag id (YYYYMMDDHHMM)
_MODAL_ID_RE = re.compile(r'modal-content-(?P<id>\d{8,14})')
# player config + daily HLS source
_OPTS_RE = re.compile(
    r'data-component-options=\'(?P<json>\{[^\']*?"startPosition"[^\']*?\})\'', re.S)
_SOURCE_RE = re.compile(
    r'<source\s+src="(?P<src>[^"]*smil:\d+\.smil/playlist\.m3u8)"', re.S)
# Tagesordnung / session-calendar link (per session)
_TO_LINK_RE = re.compile(r'href="(?P<href>/de/aktuelles/sitzungskalender/sitzung/\d+)"')


def _text(fragment: str) -> str:
    """Strip tags + collapse whitespace + unescape entities."""
    return re.sub(r'\s+', ' ', unescape(_TAG_RE.sub(' ', fragment))).strip()


def _parse_title(inner_html: str) -> tuple[str, str, str]:
    """``"Dirk Panter <span class=badge…>SPD</span> - Sonderredezeit"`` →
    ``(speaker, faction, speech_type)``. Faction/speech_type may be empty."""
    badge = _BADGE_RE.search(inner_html)
    faction = unescape(badge.group("faction")).strip() if badge else ""
    if badge:
        speaker = _text(inner_html[:badge.start()])
        tail = _text(inner_html[badge.end():])
        speech_type = tail.lstrip("- ").strip()
    else:
        speaker = _text(inner_html)
        speech_type = ""
    return speaker, faction, speech_type


def parse_list_page(html: str, *, wp: int) -> list[dict]:
    """Parse one archive list page into per-speech raw records.

    Returns one dict per ``xm_teaser`` article that carries a video player.
    Items without a daily-HLS ``<source>`` or player offsets are skipped
    (livestream placeholders, "no video" rows).
    """
    blocks = _ARTICLE_SPLIT_RE.split(html)[1:]   # drop the pre-first-article chrome
    records: list[dict] = []
    for block in blocks:
        opts = _OPTS_RE.search(block)
        src = _SOURCE_RE.search(block)
        mid = _MODAL_ID_RE.search(block)
        if not (opts and src and mid):
            continue
        try:
            cfg = json.loads(opts.group("json"))
        except json.JSONDecodeError:
            continue
        start = cfg.get("startPosition")
        end = cfg.get("endPosition")
        if start is None:
            continue

        title_m = _TITLE_RE.search(block)
        speaker, faction, speech_type = (
            _parse_title(title_m.group("inner")) if title_m else ("", "", ""))

        sitz = _SITZUNG_RE.search(block)
        dt = _DATETIME_RE.search(block)
        top = _TOP_RE.search(block)
        thema = _THEMA_RE.search(block)
        to_link = _TO_LINK_RE.search(block)
        speech_id = mid.group("id")

        records.append({
            "id": speech_id,
            "wp": wp,
            "sitzung": int(sitz.group("nr")) if sitz else None,
            "date": (f"{dt.group('y')}-{dt.group('m')}-{dt.group('d')}" if dt else None),
            "time": (f"{dt.group('H')}:{dt.group('M')}:{dt.group('S')}" if dt else None),
            "top_no": top.group("top") if top else None,
            "thema": _text(thema.group("thema")) if thema else "",
            "speaker_raw": speaker,
            "faction_raw": faction,
            "speech_type": speech_type,
            "smil_url": src.group("src"),
            "start_offset": int(start),
            "end_offset": int(end) if end is not None else None,
            "source_page": f"{EINZELBEITRAG_URL}/{speech_id}",
            "tagesordnung_url": (BASE + to_link.group("href")) if to_link else "",
        })
    return records
