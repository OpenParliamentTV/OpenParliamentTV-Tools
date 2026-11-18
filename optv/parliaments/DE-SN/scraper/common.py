"""Shared HTTP + HTML-parsing helpers for the DE-SN scraper.

The Sächsischer Landtag mediathek (``www.landtag.sachsen.de``, an ASP.NET/CMS
site) is plain public HTML with no anti-bot. The plenary-video archive is a
single paginated list:

    /de/mediathek-und-publikationen/videos/plenarvideos/index.cshtml
        ?electoral_term_id={8|7|alle}&start=N           (20 items per page)

sorted by Sitzungsnummer (highest first) and chronologically within a sitting,
and — crucially — **each list item is fully self-contained**: one
``<article class="xm_teaser …">`` per speech carrying the speaker, the faction
badge, the speech-time category, the TOP number + the TOP title (``thema``), the
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
from html import unescape

from optv.shared.http import PoliteHTTP

logger = logging.getLogger(__name__)

BASE = "https://www.landtag.sachsen.de"
LIST_URL = (f"{BASE}/de/mediathek-und-publikationen/videos/plenarvideos/"
            "index.cshtml")
EINZELBEITRAG_URL = (f"{BASE}/de/mediathek-und-publikationen/videos/plenarvideos/"
                     "videoeinzelbeitrag")
PAGE_SIZE = 20

# First sitting day of each Wahlperiode (the constituent sitting). The list's
# ``electoral_term_id`` filter files WP 8's constituent sitting (2024-10-01)
# under WP 7, so an item's WP is derived from its date rather than taken from
# the filter. Add the next WP's start date when it is constituted.
WP_START_DATES = {7: "2019-10-01", 8: "2024-10-01"}

_http = PoliteHTTP(polite_delay=0.5)
fetch = _http.fetch
fetch_text = _http.fetch_text


def list_page_url(electoral_term_id: str, start: int) -> str:
    return f"{LIST_URL}?electoral_term_id={electoral_term_id}&start={start}"


def derive_wp(date: str | None, listed_wp: int) -> int:
    """The Wahlperiode an item belongs to, from its ``YYYY-MM-DD`` date.

    Returns the highest WP in ``WP_START_DATES`` that started on or before
    ``date``, but never lower than ``listed_wp`` (the list filter), so items of
    a WP missing from the table keep the filter's WP.
    """
    if not date:
        return listed_wp
    started = [wp for wp, start in WP_START_DATES.items() if start <= date]
    return max([listed_wp, *started]) if started else listed_wp


_PAGINATION_START_RE = re.compile(r'[?&;]start=(\d+)')


def last_page_start(html: str) -> int | None:
    """The ``start`` offset of the last list page, from the pagination links."""
    starts = [int(s) for s in _PAGINATION_START_RE.findall(html)]
    return max(starts) if starts else None


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
# the TOP title (thema) in the teaser body
_THEMA_RE = re.compile(
    r'<div\s+class="xm_teaser__text[^"]*"[^>]*>\s*<p>(?P<thema>.*?)</p>', re.S)
# modal id == einzelbeitrag id: YYYYMMDD + (startPosition − 10), e.g.
# 202609171127 for startPosition 1137 on 2026-09-17 (variable length)
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


def split_speaker_type(text: str) -> tuple[str, str]:
    """``"Unbekannter Redner - Standard"`` → ``("Unbekannter Redner", "Standard")``.

    Titles without a faction badge carry the speech type after the last
    ``" - "``. Hyphenated names (``Neuhaus-Wartenberg``) have no spaces around
    the hyphen and stay intact.
    """
    name, sep, speech_type = text.rpartition(" - ")
    if not sep or not name.strip():
        return text.strip(), ""
    return name.strip(), speech_type.strip()


def _parse_title(inner_html: str) -> tuple[str, str, str]:
    """``"Dirk Panter <span class=badge…>SPD</span> - Sonderredezeit"`` →
    ``(speaker, faction, speech_type)``; a title without a badge
    (``"Unbekannter Redner - Standard"``) splits at the last ``" - "``.
    Faction/speech_type may be empty."""
    badge = _BADGE_RE.search(inner_html)
    faction = unescape(badge.group("faction")).strip() if badge else ""
    if badge:
        speaker = _text(inner_html[:badge.start()])
        tail = _text(inner_html[badge.end():])
        speech_type = tail.lstrip("- ").strip()
    else:
        speaker, speech_type = split_speaker_type(_text(inner_html))
    return speaker, faction, speech_type


def parse_list_page(html: str, *, wp: int) -> list[dict]:
    """Parse one archive list page into per-speech raw records.

    Returns one dict per ``xm_teaser`` article that carries a video player.
    Items without a daily-HLS ``<source>`` or player offsets are skipped
    (livestream placeholders, "no video" rows). ``wp`` is the list filter's
    Wahlperiode; each record's ``wp`` is derived from its date
    (:func:`derive_wp`).
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
        date = f"{dt.group('y')}-{dt.group('m')}-{dt.group('d')}" if dt else None

        records.append({
            "id": speech_id,
            "wp": derive_wp(date, wp),
            "sitzung": int(sitz.group("nr")) if sitz else None,
            "date": date,
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
