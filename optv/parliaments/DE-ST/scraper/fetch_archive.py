#! /usr/bin/env python3
"""Crawl /archiv → list of Sitzungsperiode URLs, build cumulative Sitzung map.

The portal organises plenary work into Sitzungsperioden (URL boundary), each
containing 1–N Landtagssitzungen (a single calendar day each). The Sitzung
number is the canonical reference unit (used in Plenarprotokoll citations,
Drucksachen, etc.) but is NOT exposed as structured data anywhere on the
portal — only in PDF headers and transcript prose. We derive it structurally:

  - Walk /archiv → list of all SP URLs in WP order.
  - For each SP page, count ``<section id="section-N">`` tags. Each section
    is one Landtagssitzung; its tab anchor carries the date.
  - Cumulative sum across SPs (1..N-1) + 1 = Sitzung number for SP N's day 0.

Pure HTML parsing — no PDF, no transcript prose, no Tagesordnung dependency.

The map is persisted to ``<data>/metadata/sitzung-map.json`` so downstream
stages (scraper, parser, merger) can resolve a session key to a (SP, day,
date) triple cheaply.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.scraper"

from .common import LANDTAG_BASE, fetch_text

logger = logging.getLogger(__name__)

GERMAN_MONTHS = {
    "januar": 1, "februar": 2, "märz": 3, "marz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8, "september": 9,
    "oktober": 10, "november": 11, "dezember": 12,
}


def _sp_page_path(proceedings_dir: Path, sp_number: int) -> Path:
    return proceedings_dir / f"sp-{sp_number:03d}.html"


def list_sitzungsperioden(archive_html: str) -> list[int]:
    """Extract all `/N-sitzungsperiode` numbers from the archive index."""
    nums = sorted({int(m.group(1)) for m in re.finditer(
        r'href="/(\d+)-sitzungsperiode"', archive_html)})
    return nums


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


def _ensure_sp_page(sp: int, proceedings_dir: Path, *, force: bool, retry_count: int) -> str:
    path = _sp_page_path(proceedings_dir, sp)
    if path.exists() and not force:
        return path.read_text(encoding="utf-8", errors="replace")
    url = f"{LANDTAG_BASE}/{sp}-sitzungsperiode"
    logger.info(f"Fetching {url}")
    html = fetch_text(url, retry_count=retry_count)
    path.write_text(html, encoding="utf-8")
    return html


_FIRST_SPEECH_LINK_RE = re.compile(
    r'href="[^"]*tx_lsasessions_transcript%5Bspeaker%5D=(\d+)[^"]*cHash=([0-9a-f]+)',
)
_SITZUNG_NUMBER_RE = re.compile(r"die\s+(\d+)\.\s*Sitzung", re.IGNORECASE)


def _probe_sitzung_offset(*, sp: int, sp_html: str, expected_first_sitzung: int,
                          retry_count: int) -> int | None:
    """Fetch SP's first speech transcript, extract canonical Sitzung number.

    Returns ``canonical - expected``: how much to add to every cumulative
    count so the map matches the canonical Landtag Sitzung numbering.
    Returns ``None`` if probing fails (no transcript link, no number in
    transcript, or HTTP failure).
    """
    m = _FIRST_SPEECH_LINK_RE.search(sp_html)
    if not m:
        return None
    speaker_id, c_hash = m.group(1), m.group(2)
    url = (f"{LANDTAG_BASE}/{sp}-sitzungsperiode"
           f"?transcriptSessions=lsaSessionsAjax"
           f"&tx_lsasessions_transcript%5Bspeaker%5D={speaker_id}&cHash={c_hash}")
    try:
        body = fetch_text(url, retry_count=retry_count)
    except Exception as e:
        logger.warning(f"Sitzung offset probe failed (HTTP): {e}")
        return None
    sm = _SITZUNG_NUMBER_RE.search(body)
    if not sm:
        logger.warning(f"Sitzung offset probe found no 'NNN. Sitzung' in SP {sp} opening")
        return None
    canonical = int(sm.group(1))
    return canonical - expected_first_sitzung


def fetch_archive_and_build_sitzung_map(
    *,
    period: int,
    proceedings_dir: Path,
    metadata_dir: Path,
    force: bool = False,
    retry_count: int = 10,
) -> dict:
    """Crawl /archiv and produce the Sitzung map for the current WP.

    Returns the in-memory map and writes it to ``metadata/sitzung-map.json``.

    Schema::

        {
          "period": 8,
          "sitzungsperioden": [
            {
              "sp": 1,
              "sittings": [
                {"sitzung": 1, "date": "2021-07-06", "section": 0},
                ...
              ]
            },
            ...
          ]
        }
    """
    proceedings_dir = Path(proceedings_dir)
    metadata_dir = Path(metadata_dir)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    archive_html = fetch_text(f"{LANDTAG_BASE}/archiv", retry_count=retry_count)
    sps = list_sitzungsperioden(archive_html)
    logger.info(f"Archive lists {len(sps)} Sitzungsperioden: {sps[0]}-{sps[-1]} "
                f"(missing: {sorted(set(range(sps[0], sps[-1]+1)) - set(sps))})")

    cumulative = 0
    out: list[dict] = []
    sp_html_cache: dict[int, str] = {}
    for sp in sps:
        html = _ensure_sp_page(sp, proceedings_dir, force=force, retry_count=retry_count)
        sp_html_cache[sp] = html
        section_dates = extract_section_dates(html)
        if not section_dates:
            logger.warning(f"SP {sp}: no day sections found — skipping")
            continue
        sittings = []
        for day_idx, (section, date) in enumerate(section_dates):
            cumulative += 1
            sittings.append({
                "sitzung": cumulative,
                "date": date,
                "section": section,
            })
        out.append({"sp": sp, "sittings": sittings})

    # Cumulative count is wrong by the size of any SP that is missing from
    # the archive (SP 42 is genuinely absent in WP 8). Probe the latest SP
    # against the canonical Sitzung number in its opening transcript and
    # apply the offset uniformly. Cheap (one transcript fetch) and self-
    # correcting if more SPs go missing later.
    offset = 0
    if out:
        latest = out[-1]
        latest_first = latest["sittings"][0]["sitzung"]
        delta = _probe_sitzung_offset(
            sp=latest["sp"],
            sp_html=sp_html_cache[latest["sp"]],
            expected_first_sitzung=latest_first,
            retry_count=retry_count,
        )
        if delta:
            offset = delta
            logger.warning(f"Sitzung offset probe: cumulative count off by {delta} "
                           f"(likely missing SP(s) in archive — adjusting all "
                           f"Sitzungen by +{delta})")
            for entry in out:
                for sit in entry["sittings"]:
                    sit["sitzung"] += delta
        elif delta == 0:
            logger.info("Sitzung offset probe: cumulative count matches canonical numbering")

    payload = {
        "period": int(period),
        "sitzung_offset": offset,
        "sitzungsperioden": out,
    }
    map_path = metadata_dir / "sitzung-map.json"
    with map_path.open("w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    last_sitzung = out[-1]["sittings"][-1]["sitzung"] if out else 0
    logger.info(f"Wrote {map_path} ({len(out)} Sitzungsperioden; "
                f"latest Sitzung = {last_sitzung}; offset = {offset})")
    return payload


def load_sitzung_map(metadata_dir: Path) -> dict:
    return json.loads((Path(metadata_dir) / "sitzung-map.json").read_text())


def session_id(period: int, sitzung: int) -> str:
    """Return the OPTV session key: ``{period:02d}{sitzung:03d}``."""
    return f"{period:02d}{sitzung:03d}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=8)
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
