#! /usr/bin/env python3

# Convert raw OPAL media rows (raw-<session>-media.json) into the
# intermediate per-session media JSON consumed by the merger.
#
# Each input row carries enough to populate the media side of a Stage 2
# speech record: speaker name+party, page range, function tag, video URL.
# We don't get explicit per-speech timestamps from OPAL — sentence-level
# timing is obtained downstream from aeneas. dateStart/dateEnd at this
# stage are placeholders read from the proceedings.json sibling (if
# present) and overwritten by the merger.

import argparse
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import sys

if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

from .common import fix_faction, fix_fullname, fix_role
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-RP", stream="media")["license"]


def _session_date_from_proceedings(media_dir: Path, session_id: str) -> tuple[str, str]:
    """Look up dateStart/dateEnd from the proceedings JSON sibling, if any."""
    proceedings_dir = media_dir.parent / "proceedings"
    pf = proceedings_dir / f"{session_id}-proceedings.json"
    if not pf.exists():
        return "", ""
    try:
        meta = json.loads(pf.read_text()).get("meta", {})
        return meta.get("dateStart", ""), meta.get("dateEnd", "")
    except (json.JSONDecodeError, OSError):
        return "", ""


def _fallback_dates(session_id: str) -> tuple[str, str]:
    """Last-resort placeholder so meta.dateStart is never empty.

    The merger always overwrites these with proceedings-derived timestamps
    once a merge happens; this only matters if media is parsed standalone.
    """
    return f"1970-01-01T00:00:00", f"1970-01-01T00:00:00"


def parse_media_session(raw_path: Path) -> dict:
    raw = json.loads(raw_path.read_text())
    rows = raw.get("rows", [])
    session_id = raw["meta"]["session"]
    period = int(rows[0]["session_period"]) if rows else 18
    session_number = int(rows[0]["session_number"]) if rows else int(session_id[2:])

    dateStart, dateEnd = _session_date_from_proceedings(raw_path.parent, session_id)
    if not dateStart:
        dateStart, dateEnd = _fallback_dates(session_id)

    # Sort by speech-within-session index — this becomes the speechIndex.
    rows = sorted(rows, key=lambda r: r["speech_index_in_session"])

    # Synthesize per-speech dateStart at one-second offsets so that any
    # downstream sort by dateStart preserves OPAL order. Real per-sentence
    # timing comes from aeneas later.
    base = datetime.fromisoformat(dateStart) if "T" in dateStart else datetime(1970, 1, 1)

    items: list[dict] = []
    for idx, r in enumerate(rows):
        item_start = base + timedelta(seconds=idx)
        item_end = item_start + timedelta(seconds=1)
        speaker_label = fix_fullname(r["speaker_label"])
        function = r.get("function", "")
        person: dict = {
            "label": speaker_label,
            "context": "main-speaker",
        }
        if r.get("faction"):
            person["faction"] = {"label": fix_faction(r["faction"])}
        if function:
            person["role"] = fix_role(function)

        item = {
            "parliament": "DE-RP",
            "electoralPeriod": {"number": period},
            "session": {"number": session_number},
            "speechIndex": idx + 1,
            # Media id belongs in media.originMediaID (below), not at the speech
            # top level — the proceedings id goes to textContents[].originTextID,
            # and speech.originID stays unset (DE-RP has no joint id).
            "agendaItem": {
                "officialTitle": "",
                "title": function or "",
            },
            "dateStart": item_start.isoformat("T", "seconds"),
            "dateEnd": item_end.isoformat("T", "seconds"),
            "media": {
                "videoFileURI": r["video_url"],
                "sourcePage": r.get("source_page", "https://opal.rlp.de/"),
                "originMediaID": r["origin_media_id"],
                "license": MEDIA_LICENSE,
                "creator": "Landtag Rheinland-Pfalz",
            },
            "people": [person],
            "documents": [],
            "debug": {
                "media-source": "OPAL",
                "page-range": r.get("page_range", ""),
                "function": function,
            },
        }
        items.append(item)

    return {
        "meta": {
            "session": session_id,
            "processing": {
                "parse_media": datetime.now().isoformat("T", "seconds"),
            },
            "dateStart": dateStart,
            "dateEnd": dateEnd or dateStart,
        },
        "data": items,
    }


def parse_media_directory(media_dir: Path) -> None:
    media_dir = Path(media_dir)
    for raw in sorted(media_dir.glob("raw-*-media.json")):
        out = media_dir / raw.name[len("raw-"):]
        if out.exists() and out.stat().st_mtime >= raw.stat().st_mtime:
            continue
        logger.info(f"Parsing {raw.name} -> {out.name}")
        data = parse_media_session(raw)
        with out.open("w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse raw OPAL media JSON into intermediate JSON.")
    parser.add_argument("source", type=str,
                        help="raw-<session>-media.json file or media directory")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    src = Path(args.source)
    if src.is_dir():
        parse_media_directory(src)
    else:
        data = parse_media_session(src)
        json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
