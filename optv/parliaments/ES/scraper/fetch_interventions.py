#! /usr/bin/env python3

# Download the Congreso de los Diputados open-data "interventions" feed and
# split the Pleno (plenary) records into one raw media file per session.
#
# The feed (IntervencionesCronologicamente__<timestamp>.json) is a flat list of
# per-speech records, each carrying a direct MP4 URL (ENLACEDESCARGADIRECTA),
# speaker (ORADOR/CARGOORADOR), HH:MM times and the session document id. We:
#   1. discover the current dated filename from the open-data portal page,
#   2. download the JSON (browser UA — Cloudflare 403s otherwise),
#   3. keep only Pleno records (doc id DSCD-<leg>-PL-<n>), dropping Votación rows,
#   4. write raw-<period><nnn>-media.json per session for media2json + the
#      proceedings scraper to consume.

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sys
import time
from random import random

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

from .common import fetch_url

OPENDATA_PAGE = "https://www.congreso.es/es/opendata/intervenciones"
HOST = "https://www.congreso.es"
RETRY_MAX_WAIT_TIME = 10

# Doc id like "DSCD-15-PL-1" (Diario de Sesiones del Congreso de los Diputados,
# legislature 15, PLeno, session 1). Committee/joint docs use CO/CI/CM/DSCG.
DOC_ID_RE = re.compile(r'DSCD-(\d+)-PL-(\d+)')


def discover_interventions_url(kind: str = "Cronologicamente") -> str:
    """Return the absolute URL of the current dated interventions JSON.

    The portal page embeds a filename with a daily timestamp, e.g.
    /webpublica/opendata/intervenciones/IntervencionesCronologicamente__20260522050147.json
    """
    html = fetch_url(OPENDATA_PAGE, referer=HOST).decode("utf-8", "replace")
    pattern = re.compile(
        r'(/webpublica/opendata/intervenciones/Intervenciones'
        + re.escape(kind) + r'__\d+\.json)')
    matches = pattern.findall(html)
    if not matches:
        raise RuntimeError(f"Could not find Intervenciones{kind} JSON link on {OPENDATA_PAGE}")
    # Newest timestamp last when sorted lexically (zero-padded YYYYMMDDHHMMSS).
    return HOST + sorted(set(matches))[-1]


def pleno_session(record: dict):
    """Return (period, session_number) for a Pleno record, else None.

    Reads the document id from ENLACEPDF (most reliable) or ENLACETEXTOINTEGRO.
    """
    for field in ("ENLACEPDF", "ENLACETEXTOINTEGRO"):
        m = DOC_ID_RE.search(record.get(field) or "")
        if m:
            return int(m.group(1)), int(m.group(2))
    return None


def session_id(period: int, number: int) -> str:
    """5-char session id: period (2) + session number (3), e.g. 15001."""
    return f"{period}{number:03d}"


def split_pleno_sessions(records: list, period: int) -> dict:
    """Group Pleno intervention records by session id.

    Drops non-Pleno docs, other legislatures, and Votación rows (no speaker).
    Returns {session_id: [records...]} preserving feed order within a session.
    """
    sessions: dict = {}
    for rec in records:
        if (rec.get("TIPOINTERVENCION") or "").strip().lower() == "votación":
            continue
        info = pleno_session(rec)
        if info is None:
            continue
        rec_period, number = info
        if rec_period != period:
            continue
        sessions.setdefault(session_id(period, number), []).append(rec)
    return sessions


def write_raw_session(media_dir: Path, sid: str, period: int, number: int,
                      records: list, force: bool = False) -> bool:
    """Write raw-<sid>-media.json unless unchanged. Returns True if written."""
    out = media_dir / f"raw-{sid}-media.json"
    payload = {
        "meta": {
            "session": sid,
            "period": period,
            "sessionNumber": number,
            "docId": f"DSCD-{period}-PL-{number}",
            "parliament": "ES",
            "fetched": datetime.now().isoformat("T", "seconds"),
        },
        "interventions": records,
    }
    if out.exists() and not force:
        old = json.loads(out.read_text())
        if old.get("interventions") == records:
            return False
    media_dir.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return True


def update_interventions_period(period: int, media_dir: Path,
                                force: bool = False, retry_count: int = 0) -> dict:
    """Download the interventions feed and write per-session raw media files.

    Returns {session_id: docId} for the sessions written/seen.
    """
    media_dir = Path(media_dir)
    url = None
    should_retry = retry_count
    raw = None
    while should_retry >= 0:
        try:
            url = discover_interventions_url()
            logger.info(f"Downloading interventions feed: {url}")
            raw = json.loads(fetch_url(url, referer=OPENDATA_PAGE,
                                       accept="application/json"))
            break
        except Exception as e:
            should_retry -= 1
            if should_retry >= 0:
                wait = random() * RETRY_MAX_WAIT_TIME
                logger.warning(f"Download error ({type(e).__name__}: {e}) - retrying in {wait:.1f}s")
                time.sleep(wait)
            else:
                logger.error(f"Could not download interventions feed: {type(e).__name__}: {e}")
                return {}

    if not isinstance(raw, list):
        logger.error(f"Unexpected interventions payload type: {type(raw).__name__}")
        return {}

    sessions = split_pleno_sessions(raw, period)
    logger.info(f"Found {len(sessions)} Pleno sessions ({len(raw)} total records) for period {period}")
    seen = {}
    for sid, records in sorted(sessions.items()):
        number = records[0] and pleno_session(records[0])[1]
        written = write_raw_session(media_dir, sid, period, number, records, force=force)
        seen[sid] = f"DSCD-{period}-PL-{number}"
        if written:
            logger.debug(f"Wrote raw-{sid}-media.json ({len(records)} interventions)")
    return seen


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Congreso interventions feed into per-session raw media files.")
    parser.add_argument("media_dir", type=str, nargs="?", help="Media directory (output)")
    parser.add_argument("--period", type=int, default=15, help="Legislature/period (default: 15)")
    parser.add_argument("--retry-count", type=int, default=2, help="Max download retries")
    parser.add_argument("--force", action="store_true", default=False, help="Rewrite even if unchanged")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    if args.media_dir is None:
        parser.print_help()
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    update_interventions_period(args.period, Path(args.media_dir),
                                force=args.force, retry_count=args.retry_count)
