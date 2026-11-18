#! /usr/bin/env python3
"""Discover the DE-BY "Plenum Online" archive structure for a Wahlperiode.

Produces ``metadata/archive-wp{N}.json`` — the session index the downstream
``fetch_media`` / ``media2json`` consume. For each WP-N plenary session it
records:

- ``session_id`` (``19{NNN}`` = WP + citation Sitzungsnr),
- ``date`` (ISO) and the accordion ``gremium_id`` (the ``sitzungGremiumId``
  needed to (re)load the session via JSF),
- ``sitzungsnr`` (canonical Plenarprotokoll number, read from the loaded
  session's heading "Sitzungsablauf für die N. Plenarsitzung" — it is NOT in
  the dropdown or the playlist JSON),
- ``tops`` (the Tagesordnungspunkt headers: index + title).

The index is incremental: every run GETs the session dropdown once and loads
(one valueChange POST each) only the sittings not yet in the cached index,
cached entries whose ``session_id`` collides with another entry, and — under
``force`` — cached entries matching ``session_filter``. Cached entries are keyed
by sitting date, and ``gremium_id`` is always taken from the live dropdown: the
dropdown ids are not stable (the move to ``plon2-webanzeige`` renumbered them). The playlist URLs live
behind a per-TOP tabChange and are fetched by ``fetch_media`` only for the
sessions actually being processed.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

from .common import (
    WP_DATE_RANGE,
    PlonSession,
    parse_session_options,
    parse_sitzungsnr,
    parse_tab_headers,
)

logger = logging.getLogger(__name__)


def _iso(de_date: str) -> str | None:
    try:
        return datetime.strptime(de_date, "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def _in_period(iso: str, period: int) -> bool:
    rng = WP_DATE_RANGE.get(period)
    if not rng:
        # Unknown period: accept everything and let the operator notice.
        return True
    start, end = rng
    if start and iso < start:
        return False
    if end and iso >= end:
        return False
    return True


def _load_entry(session: PlonSession, period: int, gid: int, iso: str) -> dict | None:
    html = session.load_session(gid)
    sitzungsnr = parse_sitzungsnr(html)
    tops = parse_tab_headers(html)
    if sitzungsnr is None:
        logger.warning(f"{iso} (gremium {gid}): no Plenarsitzung heading found — skipping")
        return None
    session_id = f"{period:02d}{sitzungsnr:03d}"
    logger.debug(f"{session_id} ({iso}, gremium {gid}): {len(tops)} TOPs")
    return {
        "session_id": session_id,
        "date": iso,
        "gremium_id": gid,
        "sitzungsnr": sitzungsnr,
        "tops": [{"index": t["index"], "title": t["title"]} for t in tops],
    }


def _colliding_ids(entries) -> set[str]:
    counts = Counter(e["session_id"] for e in entries)
    return {sid for sid, n in counts.items() if n > 1}


def fetch_archive(*, period: int, media_dir: Path, metadata_dir: Path,
                  force: bool = False, retry_count: int = 20,
                  session_filter: str | None = None) -> dict:
    """Build or update the session index for ``period``.

    Cached at ``metadata/archive-wp{period}.json``. Each run reads the session
    dropdown and loads only the sittings (by date) that are new, whose cached
    ``session_id`` collides with another entry, or (under ``force``) whose
    cached ``session_id`` matches ``session_filter`` (all, without a filter).
    ``gremium_id`` always comes from the live dropdown.
    A ``session_id`` that two sittings still claim after loading is dropped
    with an error, never overwritten.
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    media_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / f"archive-wp{period}.json"

    cached: dict[str, dict] = {}
    if out_path.exists():
        with out_path.open() as f:
            for e in json.load(f).get("sessions", []):
                cached[e["date"]] = e

    session = PlonSession(retry_count=retry_count)
    accordion_html = session.start()
    options = parse_session_options(accordion_html)
    wanted = [(gid, iso) for gid, de_date in options
              if (iso := _iso(de_date)) and _in_period(iso, period)]

    pattern = re.compile(session_filter) if session_filter else None
    colliding = _colliding_ids(cached.values())
    to_load: list[tuple[int, str]] = []
    entries: dict[str, dict] = {}
    for gid, iso in wanted:
        entry = cached.get(iso)
        if (entry is None
                or entry["session_id"] in colliding
                or (force and (pattern is None or pattern.match(entry["session_id"])))):
            to_load.append((gid, iso))
        else:
            entries[iso] = {**entry, "gremium_id": gid}
    logger.info(f"WP{period}: {len(wanted)} of {len(options)} dropdown sessions in range, "
                f"{len(to_load)} to load")

    for gid, iso in to_load:
        entry = _load_entry(session, period, gid, iso)
        if entry is not None:
            entries[iso] = entry

    still_colliding = _colliding_ids(entries.values())
    for sid in sorted(still_colliding):
        dupes = sorted((e["date"], e["gremium_id"]) for e in entries.values()
                       if e["session_id"] == sid)
        logger.error(f"{sid}: claimed by several sittings {dupes} — dropped from the index")
    sessions = sorted((e for e in entries.values() if e["session_id"] not in still_colliding),
                      key=lambda e: (e["date"], e["gremium_id"]))

    archive = {
        "wp": period,
        "built": datetime.now().isoformat("T", "seconds"),
        "sessions": sessions,
    }
    out_path.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out_path.name}: {len(sessions)} sessions")
    return archive


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=19)
    parser.add_argument("--force", action="store_true",
                        help="Reload cached sittings (only those matching --limit-session, if given)")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--limit-session", type=str, default=None,
                        help="Regex on 5-digit session id; scopes --force")
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
        session_filter=args.limit_session,
    )
