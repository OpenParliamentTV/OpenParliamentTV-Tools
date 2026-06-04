#! /usr/bin/env python3
"""Parse per-speech video AJAX HTML into intermediate media JSON.

Each ``original/media/{08NNN}/{player-id}.html`` is the AJAX response from
``?videoSessions=videoAjax&videoId={pid}`` carrying ``data-jsb`` attributes
with a JSON-encoded ``selected_video`` block (duration, MP4 sources at
1080p/720p/360p, preview image).

We collapse all per-player-id files for a Sitzung into one
``{08NNN}-media.json`` keyed by player-id, so the merger can do a 1:1
lookup against the proceedings stream.
"""

from __future__ import annotations

import argparse
import html as htmllib
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
    __package__ = "optv.parliaments.DE-ST.parsers"

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-ST", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-ST", stream="media")["creator"]


# JSON blob lives in ``data-jsb='...'`` (single-quoted) on the
# ``.jsb_VideoPlaylist`` div. The outer payload carries ``selected_video``
# which is what we want.
_DATA_JSB_RE = re.compile(
    r'<div[^>]*\bclass="[^"]*\bjsb_VideoPlaylist\b[^"]*"[^>]*\bdata-jsb=\'([^\']+)\'',
    re.DOTALL,
)


def extract_video_metadata(ajax_html: str) -> dict | None:
    """Return ``{duration, preview_image_url, sources: {quality: url, ...}}``."""
    m = _DATA_JSB_RE.search(ajax_html)
    if not m:
        return None
    raw = htmllib.unescape(m.group(1))
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f"data-jsb JSON decode failed: {e}")
        return None
    sv = payload.get("selected_video") or {}
    sources_by_quality: dict[str, str] = {}
    for src in sv.get("sources", []):
        q = src.get("quality")
        u = src.get("src")
        if q and u:
            sources_by_quality[q] = u
    return {
        "id": sv.get("id"),
        "duration": sv.get("duration"),
        "preview_image_url": sv.get("preview_image_url"),
        "sources": sources_by_quality,
    }


def _best_mp4_url(sources: dict[str, str]) -> str:
    """Pick the highest available quality, falling back gracefully."""
    for q in ("mp4_1080p", "mp4_720p", "mp4_360p"):
        if q in sources:
            return sources[q]
    return next(iter(sources.values()), "")


def parse_session_media_directory(session_dir: Path, session_id: str) -> dict:
    """Aggregate all per-player-id HTML files for one Sitzung into one doc."""
    items: dict[str, dict] = {}
    for ajax_path in sorted(session_dir.glob("*.html")):
        pid = ajax_path.stem
        meta = extract_video_metadata(ajax_path.read_text(encoding="utf-8", errors="replace"))
        if meta is None:
            logger.warning(f"No video metadata in {ajax_path}")
            continue
        items[pid] = {
            "player_id": pid,
            "video_id": meta["id"],
            "duration": meta["duration"],
            "preview_image_url": meta["preview_image_url"],
            "videoFileURI": _best_mp4_url(meta["sources"]),
            "sources_by_quality": meta["sources"],
        }
    return {
        "meta": {
            "session": session_id,
            "processing": {
                "parse_media": datetime.now().isoformat("T", "seconds"),
            },
        },
        "data": items,
    }


def parse_media_directory(media_dir: Path) -> None:
    """For every per-Sitzung subdir, emit ``{session}-media.json`` alongside it."""
    media_dir = Path(media_dir)
    for sit_dir in sorted(p for p in media_dir.iterdir() if p.is_dir()):
        session_id = sit_dir.name
        out_path = media_dir / f"{session_id}-media.json"
        sources = sorted(sit_dir.glob("*.html"))
        if not sources:
            continue
        newest_src = max(s.stat().st_mtime for s in sources)
        if out_path.exists() and out_path.stat().st_mtime >= newest_src:
            continue
        doc = parse_session_media_directory(sit_dir, session_id)
        with out_path.open("w") as f:
            json.dump(doc, f, indent=2, ensure_ascii=False)
        logger.info(f"Wrote {out_path.name} ({len(doc['data'])} videos)")


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
