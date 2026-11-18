#! /usr/bin/env python3
"""Fetch the per-TOP playlists for the DE-BY sessions being processed.

For each target session this:

1. (re)loads the Sitzungsablauf via JSF (``valueChange``) to get a live
   ViewState + the per-TOP JSF component ids,
2. expands each Tagesordnungspunkt panel (``tabChange``, the dynamic content
   load) and scrapes its playlists and speech rows (``parse_tab_playlists``):
   one playlist per debated item — usually one per TOP, one per motion in the
   urgent-motions debate — and per speech row the ``startId``, the speaker's
   member id and the speech's ``aktionId``,
3. downloads each playlist JSON (``meta_vod_{N}.json`` up to sitting 87,
   ``streaming?topId={N}`` from sitting 88; speaker + party + per-speech HLS
   master) into ``original/media/{session_id}/playlist_{N}.json``,
4. writes ``original/media/{session_id}-tops.json`` mapping TOP index → title +
   playlists, which ``media2json`` consumes.

``session_filter`` is a regex on the 5-digit session key (``19{NNN}``); only
matching sessions are fetched. TOPs without video (votes, elections) have an
empty ``playlists`` list. A session with no video at all (not yet online) gets
no manifest, so the next run retries it.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

from .common import PlonSession, parse_tab_headers, parse_tab_playlists

logger = logging.getLogger(__name__)

# Manifest format; a manifest with an older (or no) format is fetched again.
MANIFEST_FORMAT = 2


def manifest_is_current(tops_path: Path) -> bool:
    try:
        with tops_path.open() as f:
            return json.load(f).get("format") == MANIFEST_FORMAT
    except (OSError, ValueError):
        return False


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
        if not force and manifest_is_current(tops_path):
            logger.info(f"{session_id}: tops manifest exists — skipping (use --force to refetch)")
            continue

        html = session.load_session(entry["gremium_id"])
        headers = parse_tab_headers(html)

        tops_out: list[dict] = []
        for h in headers:
            panel = session.load_tab(h["index"], h["component_id"])
            playlists_out: list[dict] = []
            for pl in parse_tab_playlists(panel):
                if not pl["tid"]:
                    logger.warning(f"{session_id} TOP {h['index']}: no playlist id in {pl['url']}")
                    continue
                fname = f"playlist_{pl['tid']}.json"
                raw_path = raw_dir / fname
                if force or not raw_path.exists():
                    try:
                        body = session.get_text(pl["url"])
                    except RuntimeError as e:
                        logger.warning(f"{session_id} TOP {h['index']}: {pl['url']}: {e}")
                        continue
                    raw_dir.mkdir(parents=True, exist_ok=True)
                    raw_path.write_text(body, encoding="utf-8")
                playlists_out.append({
                    "tid": pl["tid"],
                    "url": pl["url"],
                    "player_url": pl["player_url"],
                    "file": fname,
                    "rows": pl["rows"],
                })
            tops_out.append({
                "index": h["index"],
                "title": h["title"],
                "playlists": playlists_out,
            })

        n_vid = sum(1 for t in tops_out if t["playlists"])
        if not n_vid:
            logger.info(f"{session_id} ({entry['date']}): {len(tops_out)} TOPs, no video yet — "
                        "no manifest written, retried next run")
            continue
        manifest = {
            "format": MANIFEST_FORMAT,
            "session_id": session_id,
            "date": entry["date"],
            "sitzungsnr": entry["sitzungsnr"],
            "gremium_id": entry["gremium_id"],
            "tops": tops_out,
        }
        tops_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
        n_pl = sum(len(t["playlists"]) for t in tops_out)
        logger.info(f"{session_id} ({entry['date']}): {len(tops_out)} TOPs, {n_vid} with video, "
                    f"{n_pl} playlists")


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
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
    fetch_media_for_archive(
        archive=archive,
        media_dir=args.data_dir / "original" / "media",
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
