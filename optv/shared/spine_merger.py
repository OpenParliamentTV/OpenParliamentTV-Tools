"""The frame the media-spine mergers share.

A media-spine merger (the German Landtage) turns each speech of the parsed media file into one Stage 2 record and attaches proceedings or subtitle text to it. How a record is built and how text is joined differs per parliament; reading the media file and writing the merged document does not.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from optv.shared.meta import build_meta, now_iso

logger = logging.getLogger(__name__)


def load_spine(config, session: str) -> Optional[tuple[dict, list[dict]]]:
    """The parsed media document and its speeches, or ``None`` when there is nothing to merge (missing file, no speeches)."""
    media_path = config.file(session, "media")
    if not media_path.exists():
        logger.warning(f"No media file for {session} at {media_path}")
        return None
    with media_path.open() as f:
        media_doc = json.load(f)
    speeches = media_doc.get("data", [])
    if not speeches:
        logger.warning(f"{session}: media file has no speeches")
        return None
    return media_doc, speeches


def save_merged(config, session: str, parliament_id: str, merged: list[dict], media_doc: dict, *,
                electoral_period: int, date_start: str, date_end: str, extra: dict) -> Path:
    """Wrap ``merged`` in the Stage 2 envelope (the media file's processing history plus the merge stamp) and write the merged cache file."""
    doc = {
        "meta": build_meta(
            parliament_id,
            session=session,
            electoral_period=electoral_period,
            date_start=date_start,
            date_end=date_end,
            processing={
                **media_doc["meta"].get("processing", {}),
                "merge": now_iso(),
            },
            extra=extra,
        ),
        "data": merged,
    }
    return config.save_data(doc, session, "merged")
