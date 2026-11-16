#! /usr/bin/env python3
"""Parse the downloaded DE-BY per-TOP playlists into per-Sitzung intermediate JSON.

Inputs per session (written by the scraper):

- ``original/media/{session_id}-tops.json`` — the TOP manifest for one Sitzung:
  per TOP its index, title and playlists, each with the playlist file, the
  player URL and the speech rows scraped from the panel (``start_id``, speaker
  member id ``profile_id``, speech ``aktion_id``).
- ``original/media/{session_id}/playlist_{N}.json`` — the raw "Plenum Online"
  playlists, ``items[]`` = ``{id, title, hls}`` where ``id`` is the speech's
  ``startId``, ``title`` the speaker (with a party parenthetical for MdLs) and
  ``hls`` the per-speech HLS master playlist.

Each speech's start time is the 14-digit timestamp embedded in the HLS filename
(``…/20250702110145_…/master.m3u8``), which is UTC; it is emitted in
Europe/Berlin local time with its offset. There is no end/duration in the
source. A TOP with several playlists (the urgent-motions debate, one per motion)
yields a ``sub_index``/``sub_title`` per playlist. One record per speech is
emitted to ``original/media/{session_id}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-BY.parsers"

from ..scraper.fetch_media import MANIFEST_FORMAT

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-BY", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-BY", stream="media")["creator"]

# Fallback only; the player URL is scraped from each playlist's onclick.
PLAYER_URL = ("https://www.bayern.landtag.de/plon2-webanzeige/views/ondemand/"
              "ondemand-playlist-param.html")
BERLIN = ZoneInfo("Europe/Berlin")
# Characters JavaScript's encodeURIComponent leaves unescaped (besides _.-~,
# which quote() always keeps).
_URI_COMPONENT_SAFE = "!*'()"

# Trailing "(Party)" in the speaker title, e.g. "Franz Schmid (AfD)".
_PARTY_RE = re.compile(r'^(?P<name>.*?)\s*\((?P<party>[^()]+)\)\s*$')
# 14-digit YYYYMMDDHHMMSS in the HLS filename.
_HLS_TS_RE = re.compile(r'/(\d{14})_')


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


def _hls_datetime(hls: str) -> tuple[str | None, str | None, str | None]:
    """Return ``(local_iso, "HH:MM:SS" local, utc_iso)`` from the HLS filename.

    The 14-digit timestamp is the clip start in UTC; ``local_iso`` is
    Europe/Berlin with its offset (``2025-07-02T13:01:45+02:00``).
    """
    m = _HLS_TS_RE.search(hls or "")
    if not m:
        return None, None, None
    try:
        utc = datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None, None, None
    local = utc.astimezone(BERLIN)
    return (local.isoformat(timespec="seconds"), local.strftime("%H:%M:%S"),
            utc.isoformat(timespec="seconds"))


def source_page(player_url: str | None, playlist_url: str, start_id) -> str:
    """Player page for one speech, built like the site's own onclick handler
    (``playerUrl + '?playlist=' + encodeURIComponent(url) + '&startId=' + n``)."""
    encoded = quote(playlist_url, safe=_URI_COMPONENT_SAFE)
    return f"{player_url or PLAYER_URL}?playlist={encoded}&startId={start_id}"


def parse_session(tops_path: Path) -> dict | None:
    """Build the intermediate media doc for one ``{session_id}-tops.json``."""
    with tops_path.open() as f:
        manifest = json.load(f)
    if manifest.get("format") != MANIFEST_FORMAT:
        logger.warning(f"{tops_path.name}: manifest format {manifest.get('format')!r} is not "
                       f"{MANIFEST_FORMAT} — re-run the download stage")
        return None

    session_id = manifest["session_id"]
    iso_date = manifest["date"]
    sitzungsnr = manifest["sitzungsnr"]
    wp = int(session_id[:2])
    raw_dir = tops_path.parent / session_id

    speeches: list[dict] = []
    for top in manifest.get("tops", []):
        playlists = top.get("playlists") or []
        multi = len(playlists) > 1
        for k, pl in enumerate(playlists, start=1):
            playlist_path = raw_dir / pl["file"]
            if not playlist_path.exists():
                logger.warning(f"{session_id} TOP {top['index']}: missing playlist {pl['file']}")
                continue
            with playlist_path.open() as f:
                playlist = json.load(f)
            items = playlist.get("items", [])
            rows = {r["start_id"]: r for r in pl.get("rows", [])}
            if len(rows) != len(items):
                logger.warning(f"{session_id} TOP {top['index']} playlist {pl['tid']}: "
                               f"{len(items)} items but {len(rows)} speech rows")
            tid = pl["tid"]
            for item in items:
                name, party = _split_name_party(item.get("title", ""))
                hls = item.get("hls", "")
                start_dt, start_clock, start_utc = _hls_datetime(hls)
                item_id = item.get("id")
                row = rows.get(item_id) or {}
                rec = {
                    "speech_id": f"{tid}_{item_id}",
                    "date": iso_date,
                    "wp": wp,
                    "sitzung_no": sitzungsnr,
                    "top_index": top["index"],
                    "top_title": top.get("title") or (playlist.get("title") or "").strip(),
                    "speaker_raw": (item.get("title") or "").strip(),
                    "redner": name,
                    "gruppe": party,
                    "profile_id": row.get("profile_id"),
                    "aktion_id": row.get("aktion_id"),
                    "start_clock": start_clock or "",
                    "start_datetime": start_dt,
                    "start_utc": start_utc,
                    "videoFileURI": hls,
                    "playlist_id": tid,
                    "playlist_url": pl["url"],
                    "item_id": item_id,
                    "sourcePage": source_page(pl.get("player_url"), pl["url"], item_id),
                }
                if multi:
                    rec["sub_index"] = k
                    rec["sub_title"] = (playlist.get("title") or "").strip()
                speeches.append(rec)

    if not speeches:
        logger.info(f"{session_id} ({iso_date}): no speeches with video — skipping")
        return None

    # Deterministic speechIndex by start instant, then TOP/playlist/item order.
    speeches.sort(key=lambda s: (s.get("start_utc") or "",
                                 s.get("top_index") or 0,
                                 s.get("sub_index") or 0,
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
