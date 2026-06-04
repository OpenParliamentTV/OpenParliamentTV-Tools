#! /usr/bin/env python3
"""Build the DE-NI session index (the per-Sitzung meeting list) for a period.

Produces ``metadata/archive-wp{N}.json`` — the candidate list that
``fetch_media`` consumes. One entry per Sitzung (Plenar-TV "meeting")::

    {"session_id": "19080", "period": 19, "sitzung": 80,
     "tagungsabschnitt": 30, "date": "2025-12-16",
     "meeting_id": "76ce46ca-…"}

Discovery walks the Tagungsabschnitt numbers (Plenar-TV's "session") via
``GET /session/periode/{wp}/session/{N}`` and collects every ``meeting`` (Sitzung)
listed. Enumeration starts at 1 and stops after ``MAX_CONSEC_MISS`` consecutive
empty/absent Tagungsabschnitte (gaps within the real range are tolerated). The
Plenar-TV REST API is public and unauthenticated, so this is a handful of cheap
GETs.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-NI.scraper"

from .common import get_session, session_key

logger = logging.getLogger(__name__)

# Stop the Tagungsabschnitt walk after this many consecutive misses.
MAX_CONSEC_MISS = 4


def discover_meetings(period: int, *, max_tagungsabschnitt: int | None = None,
                      retry_count: int = 20) -> list[dict]:
    """Enumerate every Sitzung (meeting) in ``period`` via the session API."""
    meetings: dict[str, dict] = {}
    misses = 0
    ta = 1
    while True:
        if max_tagungsabschnitt is not None and ta > max_tagungsabschnitt:
            break
        if max_tagungsabschnitt is None and misses >= MAX_CONSEC_MISS:
            break
        sess = get_session(period, ta, retry_count=retry_count)
        ta_meetings = (sess or {}).get("meetings") or []
        if not ta_meetings:
            misses += 1
            ta += 1
            continue
        misses = 0
        for m in ta_meetings:
            sitzung = m.get("meetingNumber")
            if sitzung is None:
                continue
            sid = session_key(period, int(sitzung))
            meetings[sid] = {
                "session_id": sid,
                "period": period,
                "sitzung": int(sitzung),
                "tagungsabschnitt": int(sess.get("sessionNumber", ta)),
                "date": m.get("meetingDate") or "",
                "meeting_id": m.get("id") or "",
            }
        logger.info(f"WP{period} Tagungsabschnitt {ta}: {len(ta_meetings)} Sitzung(en)")
        ta += 1
    return [meetings[k] for k in sorted(meetings)]


def fetch_archive(*, period: int, metadata_dir: Path,
                  max_tagungsabschnitt: int | None = None,
                  force: bool = False, retry_count: int = 20) -> dict:
    """Build (and cache) the per-Sitzung meeting index for ``period``.

    Cached at ``metadata/archive-wp{period}.json``; rebuilt when missing or when
    ``force`` is set.
    """
    metadata_dir = Path(metadata_dir)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / f"archive-wp{period}.json"

    if out_path.exists() and not force:
        with out_path.open() as f:
            cached = json.load(f)
        logger.info(f"Loaded {len(cached.get('sessions', []))} cached Sitzung(en) "
                    f"from {out_path.name}")
        return cached

    sessions = discover_meetings(period, max_tagungsabschnitt=max_tagungsabschnitt,
                                 retry_count=retry_count)
    archive = {
        "wp": period,
        "built": datetime.now().isoformat("T", "seconds"),
        "sessions": sessions,
    }
    out_path.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out_path.name}: {len(sessions)} Sitzung(en)")
    return archive


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=19)
    parser.add_argument("--max-tagungsabschnitt", type=int, default=None,
                        help="Upper Tagungsabschnitt bound for the enumeration "
                             "(default: walk until consecutive misses)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    fetch_archive(
        period=args.period,
        metadata_dir=args.data_dir / "metadata",
        max_tagungsabschnitt=args.max_tagungsabschnitt,
        force=args.force,
        retry_count=args.retry_count,
    )
