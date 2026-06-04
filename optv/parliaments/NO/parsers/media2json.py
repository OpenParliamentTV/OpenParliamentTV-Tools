#! /usr/bin/env python3
"""Convert the cached Qbrick part metadata into a per-meeting media doc.

Reads ``original/media/{moteid}-raw.json`` (written by ``fetch_media``) and
writes ``original/media/{session}-media.json`` with a ``parts`` list that
the merger maps each speech onto by clock-time. No Stage-2-shaped per-speech
media records yet; those are synthesised by the merger which knows the
speech list and clock times.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.parsers"

from optv.parliaments.NO.common import Config

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))


def parse_media_for_meeting(config: Config, period: int, moteid: int) -> dict:
    raw_path = config.dir("media") / f"{moteid}-raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"No raw media file for {moteid}: {raw_path}")
    raw = json.loads(raw_path.read_text())
    parts = raw.get("parts") or []
    if not parts:
        logger.warning(f"[{moteid}] no video parts in raw file — pipeline will degrade gracefully")

    return {
        "meta": {
            "session": f"{period}_{moteid}",
            "processing": {
                "parse_media": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "moteid": moteid,
        "parts": parts,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, required=True)
    parser.add_argument("--meid", type=int, required=True)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = parse_media_for_meeting(config, args.period, args.meid)
    session = f"{args.period}_{args.meid}"
    out = config.file(session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['parts'])} part(s))")


if __name__ == "__main__":
    main()
