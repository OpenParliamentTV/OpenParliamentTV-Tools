#! /usr/bin/env python3

# Download the Congreso de los Diputados open-data "interventions" feed and
# split the plenary records into one raw media file per session.
#
# The feed (IntervencionesCronologicamente__<timestamp>.json) is a flat list of
# per-speech records, each carrying a direct MP4 URL (ENLACEDESCARGADIRECTA),
# speaker (ORADOR/CARGOORADOR), HH:MM times and the Diario document id. We:
#   1. discover the current dated filename from the open-data portal page,
#   2. download the JSON (browser UA — www.congreso.es refuses other clients),
#   3. group the records by Diario (doc id DSCD-<leg>-PL-<n>), dropping Votación
#      rows, and keep the plenary Diarios: the "PL" series also holds the
#      Diputación Permanente sittings and numbers both in one sequence, so the
#      Diario number is not the plenary session number. The session number
#      comes from the MP4 path ({leg}_000400_{session}_…, organ 400 = Pleno),
#   4. write raw-<period><session>-media.json per plenary session (meta.docId
#      keeps the Diario id) for media2json + the proceedings scraper.

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

_repo_root = str(Path(__file__).resolve().parents[4])
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from optv.parliaments.ES.parsers.common import PLENO_ORGAN, parse_video_url, video_urls

OPENDATA_PAGE = "https://www.congreso.es/es/opendata/intervenciones"
HOST = "https://www.congreso.es"
RETRY_MAX_WAIT_TIME = 10

# Doc id like "DSCD-15-PL-184" (Diario de Sesiones del Congreso de los
# Diputados, legislature 15, Pleno y Diputación Permanente, Diario number 184).
# Committee/joint docs use CO/CI/CM/DSCG.
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
    """Return (period, diario_number) for a record of the Pleno Diario series, else None.

    Reads the document id from ENLACEPDF (most reliable) or ENLACETEXTOINTEGRO.
    """
    for field in ("ENLACEPDF", "ENLACETEXTOINTEGRO"):
        m = DOC_ID_RE.search(record.get(field) or "")
        if m:
            return int(m.group(1)), int(m.group(2))
    return None


def session_id(period: int, number: int) -> str:
    """5-char session id: period (2) + plenary session number (3), e.g. 15001."""
    return f"{period}{number:03d}"


def diario_doc_id(period: int, diario: int) -> str:
    return f"DSCD-{period}-PL-{diario}"


def split_pleno_sessions(records: list, period: int) -> dict:
    """Group plenary intervention records by session id.

    Drops other legislatures, Votación rows (no speaker), Diputación
    Permanente Diarios, and Diarios without any per-speech video (their
    session number is not in the feed, and there is no media to publish).

    Returns {session_id: {"number": plenary session number,
    "diario": Diario number, "records": [...]}}, feed order within a session.
    """
    diarios: dict = {}
    for rec in records:
        if (rec.get("TIPOINTERVENCION") or "").strip().lower() == "votación":
            continue
        info = pleno_session(rec)
        if info is None:
            continue
        rec_period, diario = info
        if rec_period != period:
            continue
        diarios.setdefault(diario, []).append(rec)

    sessions: dict = {}
    for diario, recs in sorted(diarios.items()):
        doc_id = diario_doc_id(period, diario)
        videos = [v for v in (parse_video_url(u) for r in recs
                              for u in video_urls(r.get("ENLACEDESCARGADIRECTA"))) if v]
        if not videos:
            logger.info(f"{doc_id}: no per-speech video - skipped")
            continue
        organs = {v["organ"] for v in videos}
        if PLENO_ORGAN not in organs:
            organo = sorted({(r.get("ORGANO") or "").strip() for r in recs})
            logger.info(f"{doc_id}: not a plenary sitting ({', '.join(organo)}) - skipped")
            continue
        if len(organs) > 1:
            logger.warning(f"{doc_id}: videos of several organs {sorted(organs)} - using the plenary ones")
        numbers = {v["session"] for v in videos if v["organ"] == PLENO_ORGAN}
        if len(numbers) > 1:
            logger.warning(f"{doc_id}: several plenary session numbers {sorted(numbers)} - skipped")
            continue
        number = numbers.pop()
        sid = session_id(period, number)
        if sid in sessions:
            logger.error(f"{doc_id}: plenary session {number} already taken by "
                         f"{diario_doc_id(period, sessions[sid]['diario'])} - skipped")
            continue
        sessions[sid] = {"number": number, "diario": diario, "records": recs}
    return sessions


def write_raw_session(media_dir: Path, sid: str, period: int, number: int,
                      diario: int, records: list, force: bool = False) -> bool:
    """Write raw-<sid>-media.json unless unchanged. Returns True if written."""
    out = media_dir / f"raw-{sid}-media.json"
    payload = {
        "meta": {
            "session": sid,
            "period": period,
            "sessionNumber": number,
            "diarioNumber": diario,
            "docId": diario_doc_id(period, diario),
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
    logger.info(f"Found {len(sessions)} plenary sessions ({len(raw)} total records) for period {period}")
    if not sessions:
        legs = sorted({info[0] for info in map(pleno_session, raw) if info})
        logger.warning(f"Feed holds no plenary session of period {period}; "
                       f"Diario legislatures in the feed: {legs}")
    seen = {}
    for sid, sess in sorted(sessions.items()):
        written = write_raw_session(media_dir, sid, period, sess["number"],
                                    sess["diario"], sess["records"], force=force)
        seen[sid] = diario_doc_id(period, sess["diario"])
        if written:
            logger.debug(f"Wrote raw-{sid}-media.json ({len(sess['records'])} interventions, {seen[sid]})")
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
