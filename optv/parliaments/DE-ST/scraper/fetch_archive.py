#! /usr/bin/env python3
"""Crawl /archiv → list of Sitzungsperiode pages, build the Sitzung map.

The portal organises plenary work into Sitzungsperioden (one page each),
each containing 1–N Landtagssitzungen (a single calendar day each). The
archive keeps only the electoral period in progress: ``/archiv`` lists its
pages under a ``<h2>{N}. Wahlperiode</h2>`` heading. Page addresses are
editor-chosen slugs (``/1-sitzungsperiode``, ``/2-sitzungsperiode-1``), so
each page's href is taken from the archive as it is; the SP number comes
from the link text.

The Sitzung number is the canonical reference unit (Plenarprotokoll
citations, Drucksachen). Each day section of an SP page links that day's
protocol as ``/fileadmin/files/plenum/wp{P}/{NNN}stzg.pdf`` once it is
published; its file name carries the Sitzung number. A day without a protocol
yet gets the previous day's number + 1 (archive order).

The map is persisted to ``<data>/metadata/sitzung-map-{PP}.json`` so the
downstream stages can resolve a session key to its (SP, page URL, day
section, date, protocol URL).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

from lxml import html as lxml_html

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.scraper"

from .common import LANDTAG_BASE, fetch_text

logger = logging.getLogger(__name__)

ARCHIVE_URL = f"{LANDTAG_BASE}/archiv"

_PERIOD_HEADING_RE = re.compile(r"^(\d+)\.\s*Wahlperiode$")
_SP_LINK_TEXT_RE = re.compile(r"(\d+)\.\s*Sitzungsperiode")
_PROTOCOL_HREF_RE = re.compile(r"/plenum/wp(\d+)/(\d+)stzg\.pdf$")
_SECTION_ID_RE = re.compile(r"section-(\d+)$")


def absolute_url(href: str) -> str:
    """Join a portal href (``/2-sitzungsperiode-1?…``) to the portal base."""
    return urljoin(LANDTAG_BASE + "/", href)


def _sp_page_path(proceedings_dir: Path, period: int, sp_number: int) -> Path:
    return Path(proceedings_dir) / f"sp-{int(period):02d}-{int(sp_number):03d}.html"


def _map_path(metadata_dir: Path, period: int) -> Path:
    return Path(metadata_dir) / f"sitzung-map-{int(period):02d}.json"


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def parse_archive(archive_html: str) -> dict[int, list[tuple[int, str]]]:
    """Return ``{period: [(sp_number, absolute_url), ...]}`` from /archiv.

    Only links listed under a ``<h2>{N}. Wahlperiode</h2>`` heading count, so
    the page header's link to the upcoming Sitzungsperiode (no videos yet) is
    ignored.
    """
    tree = lxml_html.fromstring(archive_html)
    out: dict[int, list[tuple[int, str]]] = {}
    for h2 in tree.xpath("//h2"):
        m = _PERIOD_HEADING_RE.match(_normalize_ws(h2.text_content()))
        if not m:
            continue
        period = int(m.group(1))
        found: dict[int, str] = {}
        for sib in h2.itersiblings():
            if sib.tag == "h2":
                break
            links = [sib] if sib.tag == "a" else sib.xpath(".//a[@href]")
            for a in links:
                tm = _SP_LINK_TEXT_RE.search(_normalize_ws(a.text_content()))
                if tm and a.get("href"):
                    found.setdefault(int(tm.group(1)), absolute_url(a.get("href")))
        out[period] = sorted(found.items())
    return out


def _parse_iso_date(s: str) -> str | None:
    """Accept '28.01.2026' or '2026-01-28' style dates, return ISO 'YYYY-MM-DD'."""
    s = s.strip()
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", s)
    if m:
        d, mo, y = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def extract_section_dates(sp_html: str) -> list[tuple[int, str]]:
    """Return [(section_index, iso_date), ...] in DOM order.

    The day tabs render as ``<a href="#section-N" ...>Mittwoch, 28.01.2026</a>``.
    We extract the section index and the trailing DD.MM.YYYY date.
    """
    out: list[tuple[int, str]] = []
    seen: set[int] = set()
    for m in re.finditer(
        r'<a[^>]*href="#section-(\d+)"[^>]*>(.*?)</a>',
        sp_html, flags=re.DOTALL,
    ):
        idx = int(m.group(1))
        if idx in seen:
            continue
        text = re.sub(r"<[^>]+>", " ", m.group(2))
        text = re.sub(r"\s+", " ", text).strip()
        # Look for trailing DD.MM.YYYY
        dm = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4})", text)
        if not dm:
            continue
        date = _parse_iso_date(dm.group(1))
        if date is None:
            continue
        out.append((idx, date))
        seen.add(idx)
    out.sort()
    return out


def extract_section_protocols(sp_html: str, period: int) -> dict[int, tuple[str, int]]:
    """Return ``{section_index: (protocol_url, sitzung_number)}``.

    Each day section links its Plenarprotokoll (``{NNN}stzg.pdf``) once it is
    published; sections without one are absent from the result.
    """
    tree = lxml_html.fromstring(sp_html)
    out: dict[int, tuple[str, int]] = {}
    for sec in tree.xpath('//section[starts-with(@id, "section-")]'):
        m = _SECTION_ID_RE.match(sec.get("id", ""))
        if not m:
            continue  # section-inner-*, section-video-*
        for a in sec.xpath(".//a[@href]"):
            pm = _PROTOCOL_HREF_RE.search(a.get("href", ""))
            if pm and int(pm.group(1)) == int(period):
                out[int(m.group(1))] = (absolute_url(a.get("href")), int(pm.group(2)))
                break
    return out


def assign_sitzung_numbers(protocol_numbers: list[int | None]) -> list[tuple[int, str]]:
    """Number the sitting days of a period, given in archive order.

    A day with a protocol takes the protocol's number (``"protocol"``); a day
    without one takes the previous day's number + 1 (``"sequence"``). Days
    before the first protocol are counted back from it; with no protocol at
    all, numbering starts at 1.
    """
    first = next((i for i, n in enumerate(protocol_numbers) if n is not None), None)
    prev = protocol_numbers[first] - first - 1 if first is not None else 0
    out: list[tuple[int, str]] = []
    for i, known in enumerate(protocol_numbers):
        expected = prev + 1
        if known is not None:
            if known != expected:
                logger.warning(f"Sitting day {i + 1} of the archive: protocol says "
                               f"Sitzung {known}, the sequence gives {expected}")
            number, source = known, "protocol"
        else:
            number, source = expected, "sequence"
        out.append((number, source))
        prev = number
    return out


def _ensure_sp_page(period: int, sp: int, url: str, proceedings_dir: Path, *,
                    force: bool, retry_count: int) -> str:
    """Return the SP page HTML, fetching it unless a complete copy is cached.

    A cached page is refreshed while one of its days still lacks a protocol
    link, so the protocol (and any video added after the sitting) shows up.
    The file is rewritten only when its content changed.
    """
    path = _sp_page_path(proceedings_dir, period, sp)
    cached = path.read_text(encoding="utf-8", errors="replace") if path.exists() else None
    if cached is not None and not force:
        days = {idx for idx, _ in extract_section_dates(cached)}
        if days and days <= set(extract_section_protocols(cached, period)):
            return cached
    logger.info(f"Fetching {url}")
    try:
        html = fetch_text(url, retry_count=retry_count)
    except RuntimeError as e:
        if cached is None:
            raise
        logger.warning(f"SP {sp}: refresh failed, using the cached page ({e})")
        return cached
    if html != cached:
        path.write_text(html, encoding="utf-8")
    return html


def _write_json_if_changed(path: Path, payload: dict) -> bool:
    if path.exists():
        try:
            if json.loads(path.read_text()) == payload:
                return False
        except json.JSONDecodeError:
            pass
    with path.open("w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return True


def fetch_archive_and_build_sitzung_map(
    *,
    period: int,
    proceedings_dir: Path,
    metadata_dir: Path,
    force: bool = False,
    retry_count: int = 10,
) -> dict | None:
    """Crawl /archiv and produce the Sitzung map for ``period``.

    Returns the map and writes it to ``metadata/sitzung-map-{PP}.json`` when
    its content changed. Returns ``None`` (and leaves any stored map alone)
    when the archive does not list ``period`` — the portal keeps only the
    electoral period in progress online.

    Schema::

        {
          "period": 9,
          "sitzungsperioden": [
            {
              "sp": 2,
              "url": "https://www.landtag.sachsen-anhalt.de/2-sitzungsperiode-1",
              "sittings": [
                {"sitzung": 2, "date": "2026-10-14", "section": 0,
                 "protocolURL": "https://…/plenum/wp9/002stzg.pdf",
                 "numberSource": "protocol"},
                ...
              ]
            },
            ...
          ]
        }
    """
    period = int(period)
    proceedings_dir = Path(proceedings_dir)
    metadata_dir = Path(metadata_dir)
    proceedings_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    listed = parse_archive(fetch_text(ARCHIVE_URL, retry_count=retry_count))
    if period not in listed:
        logger.error(f"The archive lists electoral period(s) {sorted(listed)} but not "
                     f"{period}; its pages are offline — nothing to download")
        return None
    sps = listed[period]
    logger.info(f"Archive lists {len(sps)} Sitzungsperioden of WP {period}: "
                f"{[sp for sp, _ in sps]}")

    days: list[tuple[int, str, int, str, str | None, int | None]] = []
    for sp, url in sps:
        html = _ensure_sp_page(period, sp, url, proceedings_dir,
                               force=force, retry_count=retry_count)
        section_dates = extract_section_dates(html)
        if not section_dates:
            logger.warning(f"SP {sp}: no day sections found — skipping")
            continue
        protocols = extract_section_protocols(html, period)
        for section, date in section_dates:
            protocol_url, number = protocols.get(section, (None, None))
            days.append((sp, url, section, date, protocol_url, number))

    numbers = assign_sitzung_numbers([d[5] for d in days])
    out: list[dict] = []
    for (sp, url, section, date, protocol_url, _), (number, source) in zip(days, numbers):
        if not out or out[-1]["sp"] != sp:
            out.append({"sp": sp, "url": url, "sittings": []})
        out[-1]["sittings"].append({
            "sitzung": number,
            "date": date,
            "section": section,
            "protocolURL": protocol_url,
            "numberSource": source,
        })

    payload = {"period": period, "sitzungsperioden": out}
    map_path = _map_path(metadata_dir, period)
    if _write_json_if_changed(map_path, payload):
        last = out[-1]["sittings"][-1]["sitzung"] if out else 0
        logger.info(f"Wrote {map_path} ({len(out)} Sitzungsperioden; latest Sitzung = {last})")
    return payload


def load_sitzung_map(metadata_dir: Path, period: int) -> dict:
    return json.loads(_map_path(metadata_dir, period).read_text())


def session_id(period: int, sitzung: int) -> str:
    """Return the OPTV session key: ``{period:02d}{sitzung:03d}``."""
    return f"{int(period):02d}{int(sitzung):03d}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=9)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    fetch_archive_and_build_sitzung_map(
        period=args.period,
        proceedings_dir=args.data_dir / "original" / "proceedings",
        metadata_dir=args.data_dir / "metadata",
        force=args.force,
        retry_count=args.retry_count,
    )
