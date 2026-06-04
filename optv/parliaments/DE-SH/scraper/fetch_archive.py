#! /usr/bin/env python3
"""Discover the DE-SH archive structure for a given Wahlperiode.

Produces two artifacts that the downstream scrapers/parsers consume:

1. The list of ``(tagung_no, tagung_internal_id)`` pairs returned by
   ``tg-html-selector.php?wp=<internal>`` — used by ``fetch_media`` to
   iterate through ``result.php`` per Tagung while staying under the
   499-result cap.
2. A ``date → Sitzung-number`` map derived from the Plenarprotokoll
   listing page on ``landtag.ltsh.de``. Sitzung numbers are NOT exposed
   by the m7k AJAX feed (which knows only Tagung + date), but they are
   the canonical Plenarprotokoll citation and the natural Stage 2
   ``session.number``. The map is cached to
   ``metadata/sitzung_index.json`` so re-runs do not re-scrape it.

No PDF is downloaded — only the listing page is parsed to recover the
``date → NNN`` mapping from URLs of the form
``/export/sites/ltsh/infothek/wahl20/plenum/plenprot/{YYYY}/20-{NNN}_{MM}-{YY}.pdf``.
The protocol's month-year suffix combined with the Tagung-internal-id
date list lets us pin every NNN to an exact calendar date.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-SH.scraper"

from .common import (
    LANDTAG_BASE,
    M7K_BASE,
    WP_INTERNAL_ID,
    fetch_text,
    post_text,
)

logger = logging.getLogger(__name__)


@dataclass
class TagungEntry:
    tagung_no: int        # the displayed Tagung number, e.g. 39
    internal_id: int      # the AJAX-side id, e.g. 171
    sitzungen: list[tuple[int, str]]  # [(sitzung_internal_id, "DD.MM.YYYY"), ...]


@dataclass
class Archive:
    wp: int               # displayed Wahlperiode, e.g. 20
    wp_internal_id: int   # m7k-side id, e.g. 6
    tagungen: list[TagungEntry]
    # ``date_iso → sitzung_no`` (e.g. "2026-01-30" → 110), derived from the
    # Plenarprotokoll listing page on landtag.ltsh.de.
    sitzung_by_date: dict[str, int]

    def to_json(self) -> dict:
        return {
            "wp": self.wp,
            "wp_internal_id": self.wp_internal_id,
            "tagungen": [asdict(t) for t in self.tagungen],
            "sitzung_by_date": self.sitzung_by_date,
        }


# tg-html-selector.php returns one <option value="ID">N</option> per
# Tagung, with ``alle`` at the top.
_OPTION_RE = re.compile(
    r'<option\s+value="(?P<value>[^"]+)"(?:\s+selected="selected")?\s*>(?P<label>[^<]+)</option>',
    re.I,
)

# The Plenarprotokoll listing page carries one block per session of the form:
#   <div class="date_daily">110. Sitzung, 30. Januar 2026</div>
# followed by the PDF link. We parse the date_daily block directly because
# it gives us the full calendar date alongside the Sitzung number — much
# more reliable than inferring day-of-month from the PDF filename.
_DATE_DAILY_RE = re.compile(
    r'<div\s+class="date_daily">\s*(?P<sitzung>\d{1,3})\.\s*Sitzung,\s*'
    r'(?P<day>\d{1,2})\.\s*(?P<month>[A-Za-zÄÖÜäöüß]+)\s*(?P<year>\d{4})\s*</div>',
    re.I,
)

_DE_MONTH = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4, "Mai": 5, "Juni": 6,
    "Juli": 7, "August": 8, "September": 9, "Oktober": 10, "November": 11,
    "Dezember": 12,
}


def _parse_selector_options(html: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for m in _OPTION_RE.finditer(html):
        out.append((m.group("value"), m.group("label").strip()))
    return out


def _parse_date_de(s: str) -> str | None:
    """``"30.01.2026"`` → ``"2026-01-30"``; returns None on parse failure."""
    try:
        return datetime.strptime(s, "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def fetch_tagung_list(*, wp_internal: int, retry_count: int = 10) -> list[tuple[int, int]]:
    """Return ``[(tagung_no, internal_id), ...]`` sorted by Tagung number."""
    html = post_text(
        f"{M7K_BASE}/tg-html-selector.php",
        {"wp": str(wp_internal)},
        retry_count=retry_count,
    )
    out: list[tuple[int, int]] = []
    for value, label in _parse_selector_options(html):
        if value == "alle" or not label.isdigit():
            continue
        out.append((int(label), int(value)))
    out.sort(key=lambda x: x[0])
    return out


def fetch_sitzungen_for_tagung(*, tg_internal: int, retry_count: int = 10) -> list[tuple[int, str]]:
    """Return ``[(sitzung_internal_id, "DD.MM.YYYY"), ...]`` for one Tagung.

    The selector returns ``alle Tage`` as the first option followed by one
    entry per calendar day.
    """
    html = post_text(
        f"{M7K_BASE}/sg-html-selector.php",
        {"tg": str(tg_internal)},
        retry_count=retry_count,
    )
    out: list[tuple[int, str]] = []
    for value, label in _parse_selector_options(html):
        if value == "alle":
            continue
        try:
            sid = int(value)
        except ValueError:
            continue
        out.append((sid, label))
    return out


def fetch_protocol_listing(*, period: int, retry_count: int = 10) -> dict[str, int]:
    """Parse ``/infothek/wahl{N}/plenum/plenprot_seite/`` for date→Sitzung.

    The listing page renders one block per session of the form
    ``<div class="date_daily">110. Sitzung, 30. Januar 2026</div>``
    immediately above the PDF link. We parse the date_daily block to get
    both the Sitzung number and the full calendar date in one pass.

    Returns ``{date_iso: sitzung_no}`` (e.g. ``{"2026-01-30": 110}``).
    """
    url = f"{LANDTAG_BASE}/infothek/wahl{period}/plenum/plenprot_seite/"
    html = fetch_text(url, retry_count=retry_count)
    out: dict[str, int] = {}
    for m in _DATE_DAILY_RE.finditer(html):
        month_no = _DE_MONTH.get(m.group("month"))
        if month_no is None:
            logger.debug(f"Unknown German month {m.group('month')!r} in listing")
            continue
        iso = f"{int(m.group('year')):04d}-{month_no:02d}-{int(m.group('day')):02d}"
        out[iso] = int(m.group("sitzung"))
    if not out:
        logger.warning(f"No protocol date_daily blocks found in listing at {url}")
    return out


def fetch_archive(*, period: int, media_dir: Path, metadata_dir: Path,
                  force: bool = False, retry_count: int = 10) -> Archive:
    """Build the full archive descriptor for ``period`` and cache it.

    Cached at ``metadata/archive-wp{period}.json``. The cache is refreshed
    on every run when ``force`` is set; otherwise we rebuild only if the
    file is missing (the structure is small, ~20 KB, and rebuilding it
    every run keeps a live deployment in sync without explicit pokes).
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    media_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / f"archive-wp{period}.json"

    if period not in WP_INTERNAL_ID:
        raise SystemExit(
            f"DE-SH: period {period} has no known m7k internal id "
            f"(known: {sorted(WP_INTERNAL_ID)}). Update WP_INTERNAL_ID."
        )
    wp_internal = WP_INTERNAL_ID[period]

    # m7k selectors first (small, fast, definitive truth for the Tagung set).
    tagung_list = fetch_tagung_list(wp_internal=wp_internal, retry_count=retry_count)
    tagungen: list[TagungEntry] = []
    for tagung_no, tg_internal in tagung_list:
        sitzungen = fetch_sitzungen_for_tagung(
            tg_internal=tg_internal, retry_count=retry_count
        )
        tagungen.append(TagungEntry(
            tagung_no=tagung_no,
            internal_id=tg_internal,
            sitzungen=sitzungen,
        ))

    # Then the protocol listing for the date → Sitzung-NNN map.
    sitzung_by_date = fetch_protocol_listing(period=period, retry_count=retry_count)

    archive = Archive(
        wp=period,
        wp_internal_id=wp_internal,
        tagungen=tagungen,
        sitzung_by_date=sitzung_by_date,
    )
    out_path.write_text(json.dumps(archive.to_json(), indent=2, ensure_ascii=False))
    logger.info(
        f"Wrote {out_path.name}: {len(tagungen)} Tagungen, "
        f"{sum(len(t.sitzungen) for t in tagungen)} sitting days, "
        f"{len(sitzung_by_date)} day→Sitzung mappings"
    )
    return archive


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=20)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    fetch_archive(
        period=args.period,
        media_dir=args.data_dir / "original" / "media",
        metadata_dir=args.data_dir / "metadata",
        force=args.force,
        retry_count=args.retry_count,
    )
