#! /usr/bin/env python3

# Fetch the per-session HTML Diario de Sesiones ("texto íntegro") for each
# Pleno session discovered by fetch_interventions.py. The interventions feed
# only links text per session (one document per ~40 speeches), so we download
# one HTML file per session and let proceedings2json.py segment it by speaker.
#
# The search interface sits behind Cloudflare and round-trips a cookie; the
# shared opener (UA + cookie jar) in scraper/common.py handles that.

import logging
logger = logging.getLogger(__name__)

import argparse
import json
from pathlib import Path
import sys
import time
from random import random

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

from .common import fetch_url

SEARCH_BASE = "https://www.congreso.es/busqueda-de-intervenciones"
RETRY_MAX_WAIT_TIME = 10

_ROMAN = [(1000, "M"), (900, "CM"), (500, "D"), (400, "CD"), (100, "C"),
          (90, "XC"), (50, "L"), (40, "XL"), (10, "X"), (9, "IX"),
          (5, "V"), (4, "IV"), (1, "I")]


def int_to_roman(n: int) -> str:
    out = []
    for value, sym in _ROMAN:
        while n >= value:
            out.append(sym)
            n -= value
    return "".join(out)


def textointegro_url(doc_id: str, period: int) -> str:
    """Build the mostrarTextoIntegro URL for a session document id."""
    leg = int_to_roman(period)
    return (f"{SEARCH_BASE}?p_p_id=intervenciones&p_p_lifecycle=0"
            f"&p_p_state=normal&p_p_mode=view"
            f"&_intervenciones_mode=mostrarTextoIntegro"
            f"&_intervenciones_legislatura={leg}"
            f"&_intervenciones_id_texto=({doc_id}.CODI.)")


def fetch_session_html(doc_id: str, period: int, retry_count: int = 0) -> str:
    """Fetch the texto-integro HTML for one session document, with retries."""
    url = textointegro_url(doc_id, period)
    should_retry = retry_count
    while should_retry >= 0:
        try:
            html = fetch_url(url, referer=SEARCH_BASE).decode("utf-8", "replace")
            if '<div class="textoIntegro">' in html:
                return html
            logger.warning(f"{doc_id}: response has no textoIntegro div")
        except Exception as e:
            logger.warning(f"{doc_id}: fetch error {type(e).__name__}: {e}")
        should_retry -= 1
        if should_retry >= 0:
            time.sleep(random() * RETRY_MAX_WAIT_TIME)
    return ""


def download_proceedings_period(period: int, proceedings_dir: Path,
                                media_dir: Path, force: bool = False,
                                retry_count: int = 0) -> None:
    """Fetch one HTML Diario per session listed in the raw media files."""
    proceedings_dir = Path(proceedings_dir)
    media_dir = Path(media_dir)
    proceedings_dir.mkdir(parents=True, exist_ok=True)

    raws = sorted(media_dir.glob("raw-*-media.json"))
    if not raws:
        logger.warning(f"No raw media files in {media_dir} - run fetch_interventions first")
        return
    for raw in raws:
        meta = json.loads(raw.read_text()).get("meta", {})
        sid = meta.get("session")
        doc_id = meta.get("docId")
        if not sid or not doc_id:
            continue
        out = proceedings_dir / f"{sid}-proceedings.html"
        if out.exists() and not force:
            continue
        logger.info(f"Fetching proceedings for {sid} ({doc_id})")
        html = fetch_session_html(doc_id, period, retry_count=retry_count)
        if html:
            out.write_text(html, encoding="utf-8")
        else:
            logger.error(f"Could not fetch proceedings for {sid} ({doc_id})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch per-session HTML Diario de Sesiones for Congreso Pleno sessions.")
    parser.add_argument("proceedings_dir", type=str, nargs="?", help="Proceedings directory (output)")
    parser.add_argument("--media-dir", type=str, help="Media directory holding raw-*-media.json (defaults alongside)")
    parser.add_argument("--period", type=int, default=15, help="Legislature/period (default: 15)")
    parser.add_argument("--retry-count", type=int, default=2)
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    if args.proceedings_dir is None:
        parser.print_help()
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    media_dir = Path(args.media_dir) if args.media_dir else Path(args.proceedings_dir).parent / "media"
    download_proceedings_period(args.period, Path(args.proceedings_dir), media_dir,
                                force=args.force, retry_count=args.retry_count)
