#! /usr/bin/env python3
"""Download the verbatim DAR text for each reunião from debates.parlamento.pt.

The debates catalog page is a pdf.js viewer over the Diário PDF, but appending
``?sft=true`` ("Texto Completo") returns the parliament's own auto-extracted
**full verbatim text** as plain HTML — ``<p>`` paragraphs with inline
speaker-turn markers ``O/A Sr.(ª) <Name> (<Party>):``. That is the proceedings
text source (no PDF parsing needed)::

    https://debates.parlamento.pt/catalogo/r3/dar/01/{leg}/{sl}/{meeting:03d}/{YYYY-MM-DD}?sft=true

The ``{YYYY-MM-DD}`` comes from the av JSON ``eventDate`` (so this stage runs
after fetch_media). Output: ``original/proceedings/{session}-dar.html``.
Idempotent.
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
    __package__ = "optv.parliaments.PT.scraper"

from optv.parliaments.PT.common import Config, parse_session
from optv.parliaments.PT.scraper.common import http_get

logger = logging.getLogger(__name__)

DEBATES_URL = ("https://debates.parlamento.pt/catalogo/r3/dar/01/"
               "{leg}/{sl:02d}/{meeting:03d}/{date}?sft=true")


def _event_date(config: Config, session: str) -> str | None:
    """Return the reunião date (YYYY-MM-DD) from the cached av JSON."""
    av_path = config.raw_av(session)
    if not av_path.exists():
        logger.warning(f"[{session}] av JSON missing — cannot build debates URL")
        return None
    try:
        doc = json.loads(av_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    raw = doc.get("eventDate") or ""
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
    return m.group(1) if m else None


def _fetch_one(config: Config, session: str, args) -> bool:
    out = config.raw_dar(session)
    if out.exists() and not args.force:
        logger.debug(f"[{session}] DAR text cached")
        return False
    leg, sl, meeting = parse_session(session)
    date = _event_date(config, session)
    if not date:
        logger.warning(f"[{session}] no event date — skipping proceedings")
        return False
    url = DEBATES_URL.format(leg=leg, sl=sl, meeting=meeting, date=date)
    logger.info(f"[{session}] fetching {url}")
    html = http_get(url, retry_count=args.retry_count,
                    retry_delay_max=args.retry_delay_max)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info(f"[{session}] wrote {out.name} ({len(html)} bytes)")
    return True


def _matches_limit(session: str, args) -> bool:
    limit = getattr(args, "limit_session", "") or ""
    if not limit:
        return True
    try:
        return bool(re.match(limit, session))
    except re.error:
        return limit == session


def download_proceedings(config: Config, args) -> None:
    """Workflow hook: download DAR text for the requested reuniões.

    Iterates the reuniões the media scraper has already fetched (the av JSON is
    the spine and supplies the date the debates URL needs).
    """
    pt_sessions = getattr(args, "pt_session", None) or []
    sessions = pt_sessions or config.sessions()
    for session in sessions:
        if not _matches_limit(session, args):
            continue
        try:
            _fetch_one(config, session, args)
        except Exception as e:  # noqa: BLE001
            logger.error(f"[{session}] proceedings download failed: {e}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--pt-session", action="append", default=[], dest="pt_session")
    parser.add_argument("--limit-session", default="")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    download_proceedings(config, args)


if __name__ == "__main__":
    main()
