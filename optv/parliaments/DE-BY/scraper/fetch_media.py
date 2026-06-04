#! /usr/bin/env python3
"""Fetch per-TOP ``meta_vod`` playlists for the DE-BY sessions being processed.

For each target session this:

1. (re)loads the Sitzungsablauf via JSF (``valueChange``) to get a live
   ViewState + the per-TOP JSF component ids,
2. expands each Tagesordnungspunkt panel (``tabChange``, the dynamic content
   load) and scrapes the ``meta_vod`` playlist URL out of its
   ``openTV1OndemandWindow(...)`` onclick handlers,
3. downloads each distinct ``meta_vod_*.json`` playlist (speaker + party +
   per-speech HLS master) into ``original/media/{session_id}/``,
4. writes ``original/media/{session_id}-tops.json`` mapping TOP index → title +
   playlist file, which ``media2json`` consumes.

``session_filter`` is a regex on the 5-digit session key (``19{NNN}``); only
matching sessions are fetched. TOPs with no video (e.g. "Eröffnung") yield no
``meta_vod`` and are recorded with ``playlist: null``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-BY.scraper"

from .common import PlonSession, parse_tab_headers, parse_tab_meta

logger = logging.getLogger(__name__)

_META_ID_RE = re.compile(r'(meta_vod_\d+)\.json', re.I)


def _meta_filename(meta_url: str) -> str:
    m = _META_ID_RE.search(meta_url)
    return f"{m.group(1)}.json" if m else meta_url.rsplit("/", 1)[-1]


def fetch_media_for_archive(*, archive: dict, media_dir: Path,
                            force: bool = False, retry_count: int = 20,
                            session_filter: str | None = None) -> None:
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)

    pattern = re.compile(session_filter) if session_filter else None
    targets = [s for s in archive.get("sessions", [])
               if pattern is None or pattern.match(s["session_id"])]
    if pattern is not None and not targets:
        logger.warning(f"session_filter {session_filter!r} matched no session in the archive index.")
        return
    logger.info(f"Fetching media for {len(targets)} session(s)")

    session = PlonSession(retry_count=retry_count)
    session.start()

    for entry in targets:
        session_id = entry["session_id"]
        tops_path = media_dir / f"{session_id}-tops.json"
        raw_dir = media_dir / session_id
        if tops_path.exists() and not force:
            logger.info(f"{session_id}: tops manifest exists — skipping (use --force to refetch)")
            continue

        html = session.load_session(entry["gremium_id"])
        headers = parse_tab_headers(html)
        raw_dir.mkdir(parents=True, exist_ok=True)

        tops_out: list[dict] = []
        for h in headers:
            panel = session.load_tab(h["index"], h["component_id"])
            meta_url, speech_count = parse_tab_meta(panel)
            top_rec = {
                "index": h["index"],
                "title": h["title"],
                "playlist": None,
                "meta_vod_url": meta_url,
                "speech_count": speech_count,
            }
            if meta_url:
                fname = _meta_filename(meta_url)
                raw_path = raw_dir / fname
                if force or not raw_path.exists():
                    try:
                        body = session.get_text(meta_url)
                    except RuntimeError as e:
                        logger.warning(f"{session_id} TOP {h['index']}: {meta_url}: {e}")
                        body = None
                    if body is not None:
                        raw_path.write_text(body, encoding="utf-8")
                top_rec["playlist"] = fname
            tops_out.append(top_rec)

        manifest = {
            "session_id": session_id,
            "date": entry["date"],
            "sitzungsnr": entry["sitzungsnr"],
            "gremium_id": entry["gremium_id"],
            "tops": tops_out,
        }
        tops_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
        n_vid = sum(1 for t in tops_out if t["playlist"])
        logger.info(f"{session_id} ({entry['date']}): {len(tops_out)} TOPs, {n_vid} with video")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=19)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--limit-session", type=str, default=None,
                        help="Regex on 5-digit session id (e.g. ^19054$)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    from .fetch_archive import fetch_archive
    archive = fetch_archive(
        period=args.period,
        media_dir=args.data_dir / "original" / "media",
        metadata_dir=args.data_dir / "metadata",
        retry_count=args.retry_count,
    )
    fetch_media_for_archive(
        archive=archive,
        media_dir=args.data_dir / "original" / "media",
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
