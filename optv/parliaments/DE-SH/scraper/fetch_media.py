#! /usr/bin/env python3
"""Download m7k ``result.php`` per Tagung and one ``iframe.php?b=…`` sample
per Sitzung-day for video URL probing.

The m7k AJAX endpoint ``result.php`` returns one ``<div class="result">``
block per speech. It is capped at **499 results per query**, so we
iterate one query per Tagung (~10–40 results each — well under the
cap), keyed by the displayed Tagung number. Each per-Tagung response
covers one or more calendar days; the parser splits by date.

Raw saved layout::

    original/media/
        result/{period}/tagung-{N:03d}.html      # full result.php response
        iframe/{date}/{speech_id}.html           # one per speech for video probe

The iframe responses are required because the MP4 filename inside them
encodes the *recorder* timestamp, not the sitting date — we cannot
construct the video URL purely from ``beginn`` / ``ende`` in
``result.php``. One iframe fetch per Sitzung-day is enough for the
parser to derive the per-Sitzung MP4 path (every speech on the same day
shares the same MP4); however to keep the merger robust against split-
MP4 days we fetch one per speech and let the parser dedup.

The MP4 path is the same for every speech that day — fetching all of
them is wasteful but cheap, and gives us per-speech robustness if a
future split changes that assumption.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-SH.scraper"

from .common import M7K_BASE, post_text, fetch_text
from .fetch_archive import Archive

logger = logging.getLogger(__name__)

# Per-speech ``<div class="result" id="NNNNN">`` carries the speech ID and
# the German date. We only need to enumerate speech IDs here so we know
# which iframe pages to fetch; full parsing happens in media2json.
_RESULT_HEADER_RE = re.compile(
    r'<div\s+class="result"\s+id="(?P<id>\d+)"\s*>'
    r'.*?<div\s+class="datum">(?P<datum>[^<]+)</div>',
    re.S | re.I,
)


def _parse_de_date(s: str) -> str | None:
    """``"30.01.2026"`` → ``"2026-01-30"``."""
    from datetime import datetime
    try:
        return datetime.strptime(s.strip(), "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def _fetch_result_for_tagung(*, wp_internal: int, tg_internal: int,
                             retry_count: int) -> str:
    return post_text(
        f"{M7K_BASE}/result.php",
        {
            "wp": str(wp_internal),
            "tg": str(tg_internal),
            "sg": "alle",
            "fn": "alle",
            "rd": "alle",
        },
        retry_count=retry_count,
    )


def _list_speech_ids(result_html: str) -> list[tuple[str, str]]:
    """Return ``[(speech_id, iso_date), ...]`` from a result.php response."""
    out: list[tuple[str, str]] = []
    for m in _RESULT_HEADER_RE.finditer(result_html):
        iso = _parse_de_date(m.group("datum"))
        if iso:
            out.append((m.group("id"), iso))
    return out


def fetch_media_for_archive(*, archive: Archive, media_dir: Path,
                            force: bool = False, retry_count: int = 20,
                            session_filter: str | None = None) -> None:
    """For every Tagung in ``archive``, save the raw ``result.php`` HTML and
    one ``iframe.php?b=…`` per speech.

    ``session_filter`` is a regex matched against the 5-digit Stage 2
    session key (``20{NNN}``); only Tagungen containing at least one
    matching Sitzung are fetched. Use it for smoke runs.
    """
    media_dir = Path(media_dir)
    result_dir = media_dir / "result" / f"wp{archive.wp}"
    iframe_dir = media_dir / "iframe"
    result_dir.mkdir(parents=True, exist_ok=True)
    iframe_dir.mkdir(parents=True, exist_ok=True)

    # Resolve session_filter to the set of sitting days we care about.
    target_dates: set[str] | None = None
    if session_filter:
        pattern = re.compile(session_filter)
        target_dates = {
            iso for iso, sit_no in archive.sitzung_by_date.items()
            if pattern.match(f"{archive.wp}{sit_no:03d}")
        }
        if not target_dates:
            logger.warning(
                f"session_filter {session_filter!r} matched no Sitzung in "
                f"the archive — nothing to fetch."
            )
            return
        logger.info(
            f"session_filter {session_filter!r} → {len(target_dates)} sitting days"
        )

    for tagung in archive.tagungen:
        tagung_dates = {iso for sid, label in tagung.sitzungen
                        if (iso := _parse_de_date(label)) is not None}
        if target_dates is not None and tagung_dates.isdisjoint(target_dates):
            continue

        result_path = result_dir / f"tagung-{tagung.tagung_no:03d}.html"
        if force or not result_path.exists():
            logger.info(
                f"Fetching result.php for WP{archive.wp} Tagung {tagung.tagung_no} "
                f"(internal {tagung.internal_id})"
            )
            html = _fetch_result_for_tagung(
                wp_internal=archive.wp_internal_id,
                tg_internal=tagung.internal_id,
                retry_count=retry_count,
            )
            result_path.write_text(html, encoding="utf-8")
        else:
            html = result_path.read_text(encoding="utf-8")

        speech_ids = _list_speech_ids(html)
        if not speech_ids:
            logger.warning(
                f"Tagung {tagung.tagung_no}: result.php returned no parseable "
                f"speech entries — check the response shape."
            )
            continue
        if len(speech_ids) >= 499:
            logger.warning(
                f"Tagung {tagung.tagung_no}: hit the m7k 499-result cap "
                f"({len(speech_ids)} entries). Some speeches may be missing."
            )

        for speech_id, iso in speech_ids:
            if target_dates is not None and iso not in target_dates:
                continue
            iframe_path = iframe_dir / iso / f"{speech_id}.html"
            if not force and iframe_path.exists():
                continue
            iframe_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                body = fetch_text(
                    f"{M7K_BASE}/iframe.php?b={speech_id}",
                    retry_count=retry_count,
                )
            except RuntimeError as e:
                logger.warning(f"iframe.php?b={speech_id}: {e}")
                continue
            iframe_path.write_text(body, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=20)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--limit-session", type=str, default=None,
                        help="Regex on 5-digit session id (e.g. ^20119$)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    from .fetch_archive import fetch_archive
    archive = fetch_archive(
        period=args.period,
        media_dir=args.data_dir / "original" / "media",
        metadata_dir=args.data_dir / "metadata",
        retry_count=args.retry_count,
    )
    fetch_media_for_archive(
        archive=archive,
        media_dir=args.data_dir / "original" / "media",
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
