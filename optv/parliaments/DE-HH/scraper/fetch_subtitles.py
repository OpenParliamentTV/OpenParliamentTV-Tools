#! /usr/bin/env python3
"""Fetch the mediathek's WebVTT subtitles per speech (interim text tier).

From 2022-06-15 each mediathek clip's HLS master carries a subtitle rendition
(``#EXT-X-MEDIA:TYPE=SUBTITLES``, served as ~9 s ``segment-N-f5.vtt`` files)
whose cue times count from the clip start. Requested for a speech's own
server-cut clip (``common.speech_clip_url``), the track is that speech's text,
timed against exactly the clip the player plays. The subtitles follow the spoken
words closely but carry no interjections or speaker names; the merger uses them
only while a sitting has no protocol PDF (which appears months later).

For each session in scope whose ``original/proceedings/{sid}.pdf`` is missing,
and each speech whose agenda-item clip lists a WebVTT file
(``common.has_subtitles``), this fetches the per-speech master, its subtitle
playlist and every segment, merges the segments (dropping cues repeated across
segment boundaries) and writes ``original/subtitles/{sid}/{speechPk}.vtt``.
Existing files are kept unless ``--force``. A six-hour sitting is about 2,600
requests, paced by ``common.fetch``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from urllib.parse import urljoin

from optv.shared.vtt import merge_segments, write_vtt

from .common import fetch_text, has_subtitles, speech_clip_url

logger = logging.getLogger(__name__)

_SUBS_MEDIA_RE = re.compile(r'#EXT-X-MEDIA:[^\n]*TYPE=SUBTITLES[^\n]*URI="(?P<uri>[^"]+)"')


def _playlist_entries(playlist: str) -> list[str]:
    return [ln.strip() for ln in playlist.splitlines()
            if ln.strip() and not ln.startswith("#")]


def fetch_speech_subtitles(clip_url: str, *, retry_count: int = 20) -> str | None:
    """Fetch and merge the subtitle track of one clip; ``None`` when the clip's
    master lists no subtitle rendition. Raises ``RuntimeError`` when the
    playlist or a segment is missing, so no partial track is written."""
    master = fetch_text(clip_url, retry_count=retry_count)
    m = _SUBS_MEDIA_RE.search(master or "")
    if not m:
        return None
    sub_url = urljoin(clip_url, m.group("uri"))
    playlist = fetch_text(sub_url, retry_count=retry_count)
    if playlist is None:
        raise RuntimeError(f"subtitle playlist 404: {sub_url}")
    segments = []
    for seg in _playlist_entries(playlist):
        text = fetch_text(urljoin(sub_url, seg), retry_count=retry_count)
        if text is None:
            raise RuntimeError(f"subtitle segment 404: {seg}")
        segments.append(text)
    return write_vtt(merge_segments(segments))


def fetch_subtitles(*, media_dir: Path, proceedings_dir: Path, subtitles_dir: Path,
                    force: bool = False, retry_count: int = 20,
                    session_filter: str | None = None) -> None:
    for items_path in sorted(Path(media_dir).glob("*-items.json")):
        manifest = json.loads(items_path.read_text())
        sid = manifest["session_id"]
        if session_filter and not re.match(session_filter, sid):
            continue
        pdf = Path(proceedings_dir) / f"{sid}.pdf"
        if pdf.exists() and pdf.stat().st_size > 0:
            logger.debug(f"{sid}: protocol PDF present — subtitles not needed")
            continue
        out_dir = Path(subtitles_dir) / sid
        fetched = kept = failed = 0
        for item in manifest.get("items", []):
            hls = item.get("clean_hls") or ""
            if not has_subtitles(hls):
                continue
            for sp in item.get("speeches", []):
                pk = sp.get("speech_pk")
                if not pk:
                    continue
                out = out_dir / f"{pk}.vtt"
                if out.exists() and not force:
                    kept += 1
                    continue
                start = float(sp.get("start_offset") or 0)
                end = start + float(sp.get("duration") or 0)
                url = speech_clip_url(hls, start, end)
                if not url:
                    continue
                try:
                    vtt = fetch_speech_subtitles(url, retry_count=retry_count)
                except RuntimeError as ex:
                    logger.warning(f"{sid} {pk}: subtitle fetch failed ({ex})")
                    failed += 1
                    continue
                if vtt is None:
                    logger.warning(f"{sid} {pk}: clip master lists no subtitle track")
                    failed += 1
                    continue
                out_dir.mkdir(parents=True, exist_ok=True)
                out.write_text(vtt)
                fetched += 1
        if fetched or kept or failed:
            logger.info(f"{sid}: subtitles fetched {fetched}, kept {kept}, failed {failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--limit-session", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    fetch_subtitles(
        media_dir=args.data_dir / "original" / "media",
        proceedings_dir=args.data_dir / "original" / "proceedings",
        subtitles_dir=args.data_dir / "original" / "subtitles",
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
