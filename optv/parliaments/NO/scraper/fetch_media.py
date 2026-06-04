#! /usr/bin/env python3
"""Resolve and cache the Qbrick video metadata for one Storting meeting.

Writes ``original/media/{moteid}-raw.json`` with the list of video parts
(one entry per ``del``) including ``qbvid``, ``mp4_url``, ``hls_url``,
``tc_in_utc`` (the canonical UTC anchor for clock-time→offset conversion),
``tc_out_utc``, and ``duration_seconds``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.scraper"

from optv.parliaments.NO.common import Config
from optv.parliaments.NO.scraper.qbrick import resolve_meeting_video

logger = logging.getLogger(__name__)


def fetch_media_for_meeting(config: Config, moteid: int, *,
                            force: bool = False,
                            retry_count: int = 10,
                            retry_delay_max: float = 10.0) -> Path | None:
    target = config.dir("media", create=True) / f"{moteid}-raw.json"
    if target.exists() and not force:
        logger.info(f"[{moteid}] media cache hit: {target.name}")
        return target
    parts = resolve_meeting_video(moteid,
                                  retry_count=retry_count,
                                  retry_delay_max=retry_delay_max)
    if not parts:
        logger.warning(f"[{moteid}] no Qbrick video parts found")
        return None
    doc = {
        "moteid": moteid,
        "parts": [p.to_dict() for p in parts],
    }
    target.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{moteid}] wrote {target.name} ({len(parts)} part(s))")
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--meid", type=int, required=True)
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    fetch_media_for_meeting(config, args.meid, force=args.force,
                            retry_count=args.retry_count,
                            retry_delay_max=args.retry_delay_max)


if __name__ == "__main__":
    main()
