#! /usr/bin/env python3
"""Download per-speech video metadata via the portal's AJAX endpoint.

Each speech block in a Sitzungsperiode page carries two ``data-player-id``
values: the standard video and the DGS (sign-language) variant, identified
by ``data-js-id="video-std"`` / ``video-sign``. We only fetch the std one;
the sign player-id is preserved by the proceedings parser for reference but
isn't part of the speech media.

The AJAX endpoint
``/{sp}-sitzungsperiode?videoSessions=videoAjax&videoId={pid}``
returns HTML with a ``data-jsb`` JSON attribute on ``.jsb_VideoPlaylist``
carrying the duration, preview image, and direct MP4 URLs at 1080p/720p/360p.
We persist the raw HTML so the parser can re-extract without re-fetching;
the parser is responsible for JSON-decoding the embedded config.

Files land at ``original/media/{08NNN}/{player-id}.html`` where ``08NNN`` is
the Landtagssitzung session key resolved from the cumulative day-count map.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

from lxml import html as lxml_html

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.scraper"

from .common import LANDTAG_BASE, fetch_text
from .fetch_archive import _sp_page_path, session_id

logger = logging.getLogger(__name__)


def extract_section_player_ids(sp_html: str) -> dict[int, list[str]]:
    """Return {section_index: [std_player_id, ...]} in DOM order.

    Walks each ``<section id="section-N">`` (top-level day container) and
    collects every standard-video player-id under it. Sign-language IDs are
    skipped (the proceedings parser captures them for reference).
    """
    tree = lxml_html.fromstring(sp_html)
    out: dict[int, list[str]] = {}
    for sec in tree.xpath('//section[starts-with(@id, "section-")]'):
        sec_id = sec.get("id", "")
        m = re.match(r"section-(\d+)$", sec_id)
        if not m:
            continue  # skip section-inner-*
        idx = int(m.group(1))
        pids = []
        for a in sec.xpath('.//a[@data-js-id="video-std"]'):
            pid = a.get("data-player-id")
            if pid:
                pids.append(pid)
        out[idx] = pids
    return out


def fetch_media_for_session_map(
    *,
    sitzung_map: dict,
    proceedings_dir: Path,
    media_dir: Path,
    force: bool = False,
    retry_count: int = 10,
    session_filter: str = "",
) -> None:
    """Fetch per-speech video AJAX for every Sitzung in the map.

    ``session_filter`` is a regex matched against the session key (e.g.
    ``'0810[56]'`` for sittings 105+106). Empty means fetch all — typically
    way too expensive at WP-wide scale, so the workflow passes
    ``args.limit_session`` through.
    """
    proceedings_dir = Path(proceedings_dir)
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)
    period = sitzung_map.get("period", 8)
    filter_re = re.compile(session_filter) if session_filter else None

    for entry in sitzung_map.get("sitzungsperioden", []):
        sp = entry["sp"]
        # Skip the SP entirely if none of its sittings match the filter — saves
        # the cost of re-parsing the SP page just to discover nothing matches.
        if filter_re and not any(
            filter_re.match(session_id(period, sit["sitzung"]))
            for sit in entry["sittings"]
        ):
            continue
        sp_path = _sp_page_path(proceedings_dir, sp)
        if not sp_path.exists():
            logger.warning(f"SP {sp}: page HTML missing at {sp_path} — skipping media fetch")
            continue
        sp_html = sp_path.read_text(encoding="utf-8", errors="replace")
        section_pids = extract_section_player_ids(sp_html)

        for sit in entry["sittings"]:
            session = session_id(period, sit["sitzung"])
            if filter_re and not filter_re.match(session):
                continue
            section = sit["section"]
            sit_dir = media_dir / session
            pids = section_pids.get(section, [])
            if not pids:
                logger.warning(f"Session {session} (SP {sp} section {section}): no std player-ids found")
                continue
            sit_dir.mkdir(parents=True, exist_ok=True)
            for pid in pids:
                out = sit_dir / f"{pid}.html"
                if out.exists() and not force:
                    continue
                url = f"{LANDTAG_BASE}/{sp}-sitzungsperiode?videoSessions=videoAjax&videoId={pid}"
                logger.info(f"Fetching {session} pid={pid}")
                body = fetch_text(url, retry_count=retry_count)
                out.write_text(body, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    from .fetch_archive import load_sitzung_map
    sitzung_map = load_sitzung_map(args.data_dir / "metadata")
    fetch_media_for_session_map(
        sitzung_map=sitzung_map,
        proceedings_dir=args.data_dir / "original" / "proceedings",
        media_dir=args.data_dir / "original" / "media",
        force=args.force,
        retry_count=args.retry_count,
    )
