#! /usr/bin/env python3
"""Download av.parlamento.pt per-meeting JSON (the media spine).

av.parlamento.pt (Metatheke) exposes an undocumented but clean REST API. The
per-meeting endpoint returns one JSON object per plenary reunião::

    GET https://av.parlamento.pt/api/v1/videos/Plenary/{leg}/{sl}/{meeting}
    → { title, description, legislature, legislativeSession, sessionNumber,
        eventDate, duration, interventions: [ {number, interventionType,
        speakerType, speaker, role, affiliation:{name,initials},
        startTime, endTime, duration}, ... ] }

The ``startTime``/``endTime`` are HH:MM:SS.ms offsets into the session
recording; the per-speech video clip + audio are derived from them (see
parsers/media2json.py). The JSON carries **no transcript text** (that comes from
debates.parlamento.pt) and **no Wikidata/BID** (resolved via the entity dump).

Two acquisition modes:

* **Targeted** (``--pt-session 17-1-059``, repeatable): fetch those reuniões.
* **Bulk** (default): enumerate the legislatura's sessões legislativas from
  ``/videos/Plenary/{leg}`` and the meetings from ``/videos/Plenary/{leg}/{sl}``
  (HTML listing pages), then fetch each meeting's JSON.

Output: ``original/media/{session}-av.json`` (raw API JSON). Idempotent.
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

from optv.parliaments.PT.common import Config, make_session, parse_session
from optv.parliaments.PT.scraper.common import http_get

logger = logging.getLogger(__name__)

API_MEETING_URL = "https://av.parlamento.pt/api/v1/videos/Plenary/{leg}/{sl}/{meeting}"
LIST_LEG_URL = "https://av.parlamento.pt/videos/Plenary/{leg}"
LIST_SL_URL = "https://av.parlamento.pt/videos/Plenary/{leg}/{sl}"

_SL_LINK_RE = re.compile(r"/videos/Plenary/(\d+)/(\d+)(?![\d/])")
_MEETING_LINK_RE = re.compile(r"/videos/Plenary/(\d+)/(\d+)/(\d+)(?![\d/])")


def _matches_limit(session: str, args) -> bool:
    limit = getattr(args, "limit_session", "") or ""
    if not limit:
        return True
    try:
        return bool(re.match(limit, session))
    except re.error:
        return limit == session


def _list_sessoes(leg: int, args) -> list[int]:
    """Return the sessão-legislativa numbers listed for a legislatura."""
    html = http_get(LIST_LEG_URL.format(leg=leg), retry_count=args.retry_count,
                    retry_delay_max=args.retry_delay_max)
    sls = {int(sl) for (lg, sl) in _SL_LINK_RE.findall(html) if int(lg) == leg}
    return sorted(sls)


def _list_meetings(leg: int, sl: int, args) -> list[int]:
    """Return the meeting numbers listed for one sessão legislativa."""
    html = http_get(LIST_SL_URL.format(leg=leg, sl=sl), retry_count=args.retry_count,
                    retry_delay_max=args.retry_delay_max)
    meetings = {int(mt) for (lg, s, mt) in _MEETING_LINK_RE.findall(html)
                if int(lg) == leg and int(s) == sl}
    return sorted(meetings)


def _fetch_one(config: Config, session: str, args) -> bool:
    out = config.raw_av(session)
    if out.exists() and not args.force:
        logger.debug(f"[{session}] av JSON cached")
        return False
    leg, sl, meeting = parse_session(session)
    url = API_MEETING_URL.format(leg=leg, sl=sl, meeting=meeting)
    logger.info(f"[{session}] fetching {url}")
    raw = http_get(url, retry_count=args.retry_count,
                   retry_delay_max=args.retry_delay_max)
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"[{session}] av JSON not parseable: {e}")
        return False
    if not doc.get("interventions"):
        logger.warning(f"[{session}] av JSON has no interventions — skipping")
        return False
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"[{session}] wrote {out.name} ({len(doc['interventions'])} interventions)")
    return True


def download_media(config: Config, args) -> None:
    """Workflow hook: download av per-meeting JSON for the requested reuniões."""
    pt_sessions = getattr(args, "pt_session", None) or []
    if pt_sessions:
        for session in pt_sessions:
            try:
                parse_session(session)
            except ValueError:
                logger.error(f"ignoring malformed --pt-session {session!r}")
                continue
            try:
                _fetch_one(config, session, args)
            except Exception as e:  # noqa: BLE001
                logger.error(f"[{session}] media download failed: {e}")
        return

    leg = int(getattr(args, "period", None) or 17)
    try:
        sessoes = _list_sessoes(leg, args)
    except Exception as e:  # noqa: BLE001
        logger.error(f"could not list sessões legislativas for legislatura {leg}: {e}")
        return
    for sl in sessoes:
        try:
            meetings = _list_meetings(leg, sl, args)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"could not list meetings for L{leg} SL{sl}: {e}")
            continue
        for meeting in meetings:
            session = make_session(leg, sl, meeting)
            if not _matches_limit(session, args):
                continue
            try:
                _fetch_one(config, session, args)
            except Exception as e:  # noqa: BLE001
                logger.error(f"[{session}] media download failed: {e}")


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
    download_media(config, args)


if __name__ == "__main__":
    main()
