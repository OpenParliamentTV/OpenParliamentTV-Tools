#! /usr/bin/env python3
"""Parse the downloaded DE-BY per-TOP playlists into per-Sitzung intermediate JSON.

Inputs per session (written by the scraper):

- ``original/media/{session_id}-tops.json`` — the TOP manifest (index, title,
  playlist filename) for one Sitzung.
- ``original/media/{session_id}/meta_vod_*.json`` — the raw "Plenum Online"
  playlists, one per TOP, each ``items[]`` = ``{id, title, hls}`` where
  ``title`` is the speaker (with a party parenthetical for MdLs) and ``hls`` is
  the per-speech HLS master playlist.

Each speech's start time is taken from the 14-digit timestamp embedded in the
HLS filename (``…/20250702122334_…/master.m3u8``). There is no end/duration in
the source. One record per speech is emitted to
``original/media/{session_id}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-BY.parsers"

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-BY", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-BY", stream="media")["creator"]

PLAYER_URL = ("https://www.bayern.landtag.de/plon-webanzeige/views/ondemand/"
              "ondemand-playlist-param.html")

# Trailing "(Party)" in the speaker title, e.g. "Franz Schmid (AfD)".
_PARTY_RE = re.compile(r'^(?P<name>.*?)\s*\((?P<party>[^()]+)\)\s*$')
# 14-digit YYYYMMDDHHMMSS in the HLS filename.
_HLS_TS_RE = re.compile(r'/(\d{14})_')
_META_ID_RE = re.compile(r'meta_vod_(\d+)\.json', re.I)


def _split_name_party(title: str) -> tuple[str, str]:
    """``"Franz Schmid (AfD)"`` → ``("Franz Schmid", "AfD")``.

    Speakers with no parenthetical (government members, presiding officers)
    return an empty party string.
    """
    title = (title or "").strip()
    m = _PARTY_RE.match(title)
    if m:
        return m.group("name").strip(), m.group("party").strip()
    return title, ""


def _hls_datetime(hls: str) -> tuple[str | None, str | None]:
    """Return ``(iso_datetime, "HH:MM:SS")`` from the HLS filename timestamp."""
    m = _HLS_TS_RE.search(hls or "")
    if not m:
        return None, None
    try:
        dt = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    except ValueError:
        return None, None
    return dt.isoformat("T", "seconds"), dt.strftime("%H:%M:%S")


def _meta_vod_id(filename: str) -> str | None:
    m = _META_ID_RE.search(filename or "")
    return m.group(1) if m else None


def parse_session(tops_path: Path) -> dict | None:
    """Build the intermediate media doc for one ``{session_id}-tops.json``."""
    with tops_path.open() as f:
        manifest = json.load(f)

    session_id = manifest["session_id"]
    iso_date = manifest["date"]
    sitzungsnr = manifest["sitzungsnr"]
    wp = int(session_id[:2])
    raw_dir = tops_path.parent / session_id

    speeches: list[dict] = []
    for top in manifest.get("tops", []):
        playlist_file = top.get("playlist")
        if not playlist_file:
            continue  # TOP without video (e.g. "Eröffnung")
        playlist_path = raw_dir / playlist_file
        if not playlist_path.exists():
            logger.warning(f"{session_id} TOP {top['index']}: missing playlist {playlist_file}")
            continue
        with playlist_path.open() as f:
            playlist = json.load(f)
        meta_id = _meta_vod_id(playlist_file)
        meta_url = top.get("meta_vod_url") or ""
        for item in playlist.get("items", []):
            name, party = _split_name_party(item.get("title", ""))
            hls = item.get("hls", "")
            start_dt, start_clock = _hls_datetime(hls)
            item_id = item.get("id")
            speeches.append({
                "speech_id": f"{meta_id}_{item_id}",
                "date": iso_date,
                "wp": wp,
                "sitzung_no": sitzungsnr,
                "top_index": top["index"],
                "top_title": top.get("title") or playlist.get("title", "").strip(),
                "speaker_raw": (item.get("title") or "").strip(),
                "redner": name,
                "gruppe": party,
                "start_clock": start_clock or "",
                "start_datetime": start_dt,
                "videoFileURI": hls,
                "meta_vod_id": meta_id,
                "meta_vod_url": meta_url,
                "item_id": item_id,
                "sourcePage": f"{PLAYER_URL}?playlist={meta_url}&startId={item_id}",
            })

    if not speeches:
        logger.info(f"{session_id} ({iso_date}): no speeches with video — skipping")
        return None

    # Deterministic speechIndex by start time, then TOP/item order as tiebreak.
    speeches.sort(key=lambda s: (s.get("start_datetime") or "",
                                 s.get("top_index") or 0,
                                 s.get("item_id") or 0))
    for idx, s in enumerate(speeches, start=1):
        s["speech_index"] = idx

    return {
        "meta": {
            "session": session_id,
            "wp": wp,
            "date": iso_date,
            "sitzung": sitzungsnr,
            "processing": {
                "parse_media": datetime.now().isoformat("T", "seconds"),
            },
        },
        "data": speeches,
    }


def parse_media_directory(media_dir: Path) -> None:
    """Walk ``original/media/`` and emit ``{session_id}-media.json`` per Sitzung."""
    media_dir = Path(media_dir)
    tops_files = sorted(media_dir.glob("*-tops.json"))
    if not tops_files:
        logger.warning(f"No *-tops.json manifests under {media_dir} — nothing to parse.")
        return
    for tops_path in tops_files:
        doc = parse_session(tops_path)
        if doc is None:
            continue
        session_id = doc["meta"]["session"]
        out_path = media_dir / f"{session_id}-media.json"
        out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"Wrote {out_path.name} ({len(doc['data'])} speeches, {doc['meta']['date']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    parse_media_directory(args.data_dir / "original" / "media")
