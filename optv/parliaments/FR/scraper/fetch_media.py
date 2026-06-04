#! /usr/bin/env python3
"""Resolve the session video for each séance.

The Syceron compte rendu already carries per-speech ``stime`` offsets into the
session recording, so the only thing the media side has to supply is **one
video URL per séance**. The bridge is the réunion id:

* the compte rendu names its séance in ``<seanceRef>RUANR5L17S…IDS…</seanceRef>``;
* the ``interventions-video`` index pairs that same ``RUANR…`` id with the
  video compte rendu id ``CRVANR5L17S…IDV…`` (``data-id="CRV… RU…"``);
* the video compte rendu page ``/dyn/videos/{CRV…}`` embeds the HLS
  master playlist URL (``…/master.m3u8`` on ``videos-diffusion.assemblee-nationale.fr``).

We page the index (most-recent-first) building a cached ``RU → CRV`` map until
the target séance is found, then fetch the CRV page for the HLS URL. Output:
``original/media/{session}-event.json``.

No JSON/XML endpoint behind the index was found (the ``/dyn/`` front renders the
results server-side and the player loads markers via JS); the documented HTML
index is the bridge, so we scrape it. ``stime`` removes any need to scrape the
per-speech timecodes themselves.
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
    __package__ = "optv.parliaments.FR.scraper"

from optv.parliaments.FR.common import Config
from optv.parliaments.FR.scraper.common import http_get

logger = logging.getLogger(__name__)

INDEX_URL = "https://www.assemblee-nationale.fr/dyn/{leg}/interventions-video?page={page}&limit={limit}"
# The video compte-rendu page is served without a legislature segment.
CRV_PAGE_URL = "https://www.assemblee-nationale.fr/dyn/videos/{crv}"

_SEANCE_REF_RE = re.compile(r"<seanceRef>\s*(RUANR5L\d+S\d{4}IDS\d+)\s*</seanceRef>")
# data-id="CRVANR5L17S2026IDV19039494 RUANR5L17S2026IDS30671 / Bloc de style…"
_INDEX_PAIR_RE = re.compile(
    r'data-id="(CRVANR5L\d+S\d{4}IDV\d+)\s+(RUANR5L\d+S\d{4}IDS\d+)')
_HLS_RE = re.compile(r'https://[^"\'\s\\]+?\.m3u8')

# The index ignores large ``limit`` values (limit>12 collapses to a handful of
# highlighted cards); 12 is the only reliable page size, so we page in 12s.
INDEX_LIMIT = 12
INDEX_MAX_PAGES = 120         # most-recent interventions before giving up


def _read_seance_ref(config: Config, session: str) -> str | None:
    cr = config.raw_cr(session)
    if not cr.exists():
        logger.warning(f"[{session}] no compte rendu — cannot resolve video")
        return None
    m = _SEANCE_REF_RE.search(cr.read_text(encoding="utf-8"))
    return m.group(1) if m else None


def _index_cache_path(config: Config) -> Path:
    return config.dir("media", create=True) / "_crv_index.json"


def _load_index_cache(config: Config) -> dict[str, str]:
    p = _index_cache_path(config)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def _save_index_cache(config: Config, cache: dict[str, str]) -> None:
    _index_cache_path(config).write_text(json.dumps(cache, indent=2, ensure_ascii=False))


def _resolve_crv(config: Config, ru_id: str, args, cache: dict[str, str]) -> str | None:
    """Return the CRV video id for a réunion id, paging the index as needed.

    The index interleaves séance réunions (``…IDS…``) with commission ones
    (``…IDC…``); a page can legitimately hold only commission cards and yet the
    target séance still lies further down. We therefore keep paging until we
    reach a page with *no cards at all* (the real end of the index).
    """
    if ru_id in cache:
        return cache[ru_id]
    for page in range(1, INDEX_MAX_PAGES + 1):
        url = INDEX_URL.format(leg=args.period, page=page, limit=INDEX_LIMIT)
        try:
            html = http_get(url, retry_count=args.retry_count,
                            retry_delay_max=args.retry_delay_max)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"interventions-video page {page} failed: {e}")
            break
        if "data-id=" not in html:
            break  # ran past the end of the index
        for crv, ru in _INDEX_PAIR_RE.findall(html):
            cache.setdefault(ru, crv)
        cache["_pages_scanned"] = page
        if ru_id in cache:
            _save_index_cache(config, cache)
            return cache[ru_id]
    _save_index_cache(config, cache)
    return cache.get(ru_id)


def _extract_hls(config: Config, crv_id: str, args) -> str | None:
    url = CRV_PAGE_URL.format(crv=crv_id)
    html = http_get(url, retry_count=args.retry_count,
                    retry_delay_max=args.retry_delay_max)
    m = _HLS_RE.search(html)
    return m.group(0) if m else None


def _resolve_one(config: Config, session: str, args, cache: dict[str, str]) -> bool:
    out = config.raw_event(session)
    if out.exists() and not args.force:
        logger.debug(f"[{session}] video reference cached")
        return False
    ru_id = _read_seance_ref(config, session)
    if not ru_id:
        return False
    crv_id = _resolve_crv(config, ru_id, args, cache)
    if not crv_id:
        logger.warning(f"[{session}] no video compte rendu found for {ru_id}")
        return False
    hls = _extract_hls(config, crv_id, args)
    if not hls:
        logger.warning(f"[{session}] CRV {crv_id} carried no HLS master URL")
        return False
    event = {
        "session": session,
        "seanceRef": ru_id,
        "crvId": crv_id,
        "hlsUrl": hls,
        "sourcePage": CRV_PAGE_URL.format(crv=crv_id),
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(event, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] {ru_id} → {crv_id}")
    return True


def _target_sessions(config: Config, args) -> list[str]:
    fr_sessions = getattr(args, "fr_session", None) or []
    if fr_sessions:
        return fr_sessions
    limit = getattr(args, "limit_session", "") or ""
    sessions = config.sessions()
    if limit:
        def _ok(s):
            try:
                return bool(re.match(limit, s))
            except re.error:
                return limit == s
        sessions = [s for s in sessions if _ok(s)]
    return sessions


def download_media(config: Config, args) -> None:
    """Workflow hook: resolve session video URLs for the requested séances."""
    cache = _load_index_cache(config)
    for session in _target_sessions(config, args):
        try:
            _resolve_one(config, session, args, cache)
        except Exception as e:  # noqa: BLE001
            logger.error(f"[{session}] media resolution failed: {e}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--fr-session", action="append", default=[])
    parser.add_argument("--limit-session", default="")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    download_media(config, args)


if __name__ == "__main__":
    main()
