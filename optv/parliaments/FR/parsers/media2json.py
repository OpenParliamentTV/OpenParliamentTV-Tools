#! /usr/bin/env python3
"""Normalize the resolved séance video into the intermediate media format.

Input:  ``original/media/{session}-event.json`` (scraper output: the séance's
        video compte-rendu id + HLS master URL).
Output: ``original/media/{session}-media.json``

The Assemblée nationale publishes **one continuous video per séance**, and the
per-speech offsets come from the proceedings (``<texte stime>``), so there is a
single media descriptor per session here — the merger applies each speech's
``stime`` as the ``#t=`` offset against this one HLS master. The HLS master URL
carries both video and audio; ``align_prep`` extracts the audio track with
ffmpeg, so ``audioFileURI`` is the same master playlist.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.FR.parsers"

from optv.parliaments.FR.common import Config

logger = logging.getLogger(__name__)


def parse_media_for_session(config: Config, session: str) -> dict:
    event_path = config.raw_event(session)
    if not event_path.exists():
        raise FileNotFoundError(f"[{session}] video reference missing: {event_path}")
    event = json.loads(event_path.read_text())
    hls = event.get("hlsUrl") or ""
    descriptor = {
        "hlsUrl": hls,
        "audioFileURI": hls,            # ffmpeg extracts audio from the master
        "sourcePage": event.get("sourcePage") or "",
        "crvId": event.get("crvId") or "",
        "seanceRef": event.get("seanceRef") or "",
    }
    return {
        "meta": {
            "session": session,
            "parliament": "FR",
            "processing": {
                "parse_media": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": descriptor,
    }


def parse_media_directory(config: Config, args) -> None:
    media_dir = config.dir("media")
    for event_path in sorted(media_dir.glob("*-event.json")):
        m = re.match(r"(.+)-event\.json$", event_path.name)
        if not m:
            continue
        session = m.group(1)
        if getattr(args, "fr_session", None) and session not in args.fr_session:
            continue
        if getattr(args, "limit_session", None):
            try:
                if not re.match(args.limit_session, session):
                    continue
            except re.error:
                if args.limit_session != session:
                    continue
        out = config.file(session, "media")
        if (out.exists() and not args.force
                and out.stat().st_mtime > event_path.stat().st_mtime):
            logger.debug(f"[{session}] media intermediate cached")
            continue
        logger.info(f"[{session}] parsing media")
        doc = parse_media_for_session(config, session)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"[{session}] wrote {out.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = parse_media_for_session(config, args.session)
    out = config.file(args.session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out}")


if __name__ == "__main__":
    main()
