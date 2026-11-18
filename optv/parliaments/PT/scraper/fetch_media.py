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

Two sidecars per meeting:

* ``/api/v1/videos/Plenary/{leg}/{sl}/{meeting}/metadata/json`` — a one-line
  summary per intervention (``title``, e.g. "Apresenta o Projeto de Lei n.º
  447/XVII/1.ª (BE) — …"), used to assign agenda items.
* The meeting page ``/videos/Plenary/{leg}/{sl}/{meeting}`` — its
  ``meetingVideoSources`` script variable names the recording (HLS, DASH, MP4).

Two acquisition modes:

* **Targeted** (``--pt-session 17-1-059``, repeatable): fetch those reuniões.
* **Bulk** (default): enumerate the legislatura's sessões legislativas from
  ``/videos/Plenary/{leg}`` and the meetings from ``/videos/Plenary/{leg}/{sl}``
  (HTML listing pages), then fetch each meeting's JSON.

Outputs: ``original/media/{session}-av.json`` (raw API JSON),
``{session}-av-metadata.json`` (raw summaries) and ``{session}-av-sources.json``
(``{"hls": …, "dash": …, "mp4": …}``). Each file is fetched once; a missing one
is fetched on the next run (``--force`` re-fetches all three).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

from optv.parliaments.PT.common import Config, make_session, parse_session
from optv.parliaments.PT.scraper.common import http_get

logger = logging.getLogger(__name__)

API_MEETING_URL = "https://av.parlamento.pt/api/v1/videos/Plenary/{leg}/{sl}/{meeting}"
API_METADATA_URL = API_MEETING_URL + "/metadata/json"
MEETING_PAGE_URL = "https://av.parlamento.pt/videos/Plenary/{leg}/{sl}/{meeting}"
LIST_LEG_URL = "https://av.parlamento.pt/videos/Plenary/{leg}"
LIST_SL_URL = "https://av.parlamento.pt/videos/Plenary/{leg}/{sl}"

_SL_LINK_RE = re.compile(r"/videos/Plenary/(\d+)/(\d+)(?![\d/])")
_MEETING_LINK_RE = re.compile(r"/videos/Plenary/(\d+)/(\d+)/(\d+)(?![\d/])")
_SOURCES_BLOCK_RE = re.compile(r"meetingVideoSources\s*=\s*\[(.*?)\]", re.S)
_SOURCE_RE = re.compile(r"""src\s*:\s*["']([^"']+)["']\s*,\s*type\s*:\s*["']([^"']+)["']""")
_SOURCE_KINDS = {"application/vnd.apple.mpegurl": "hls",
                 "application/x-mpegurl": "hls",
                 "application/dash+xml": "dash",
                 "video/mp4": "mp4"}


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


def parse_meeting_sources(page_html: str) -> dict[str, str]:
    """``{"hls": url, "dash": url, "mp4": url}`` from a meeting page's
    ``meetingVideoSources`` script variable (empty if absent)."""
    m = _SOURCES_BLOCK_RE.search(page_html or "")
    if not m:
        return {}
    out: dict[str, str] = {}
    for src, mime in _SOURCE_RE.findall(m.group(1)):
        kind = _SOURCE_KINDS.get(mime.strip().lower())
        if kind and kind not in out:
            out[kind] = src.strip()
    return out


def _get(url: str, args) -> str:
    return http_get(url, retry_count=args.retry_count,
                    retry_delay_max=args.retry_delay_max)


def _write_json(path: Path, doc) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")


def _fetch_one(config: Config, session: str, args) -> bool:
    """Fetch the meeting JSON and its two sidecars; return True if any was written."""
    leg, sl, meeting = parse_session(session)
    fmt = dict(leg=leg, sl=sl, meeting=meeting)
    force = getattr(args, "force", False)
    wrote = False

    out = config.raw_av(session)
    if out.exists() and not force:
        logger.debug(f"[{session}] av JSON cached")
    else:
        url = API_MEETING_URL.format(**fmt)
        logger.info(f"[{session}] fetching {url}")
        try:
            doc = json.loads(_get(url, args))
        except json.JSONDecodeError as e:
            logger.error(f"[{session}] av JSON not parseable: {e}")
            return False
        if not doc.get("interventions"):
            logger.warning(f"[{session}] av JSON has no interventions — skipping")
            return False
        _write_json(out, doc)
        wrote = True
        logger.info(f"[{session}] wrote {out.name} ({len(doc['interventions'])} interventions)")

    meta_out = config.raw_av_metadata(session)
    if force or not meta_out.exists():
        try:
            meta = json.loads(_get(API_METADATA_URL.format(**fmt), args))
            if isinstance(meta, list) and meta:
                _write_json(meta_out, meta)
                wrote = True
                logger.info(f"[{session}] wrote {meta_out.name} ({len(meta)} summaries)")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[{session}] intervention summaries unavailable: {e}")

    src_out = config.raw_av_sources(session)
    if force or not src_out.exists():
        try:
            sources = parse_meeting_sources(_get(MEETING_PAGE_URL.format(**fmt), args))
            if sources:
                _write_json(src_out, sources)
                wrote = True
                logger.info(f"[{session}] wrote {src_out.name}")
            else:
                logger.warning(f"[{session}] no meetingVideoSources on the meeting page")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[{session}] meeting page unavailable: {e}")
    return wrote


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
