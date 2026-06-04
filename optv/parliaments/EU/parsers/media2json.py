#! /usr/bin/env python3
"""Parse EU per-day raw events into the intermediate media format.

Input:  ``original/media/raw-{YYYYMMDD}-events.json`` (one record per sitting,
        as produced by ``scraper/fetch_media.py``).

Output: ``original/media/{YYYYMMDD}-media.json`` — one record per sitting,
        with the EN audio HLS URL resolved by walking the master playlist.

Output shape::

    {
      "meta": {"session": "20251008", "date": "2025-10-08", "parliament": "EU",
               "processing": {"parse_media": "..."}},
      "data": [
        {
          "eventRef":      "20251008-0900-PLENARY",
          "title":         "Plenary session",
          "sittingStart":  1759906970,                          # unix epoch
          "sittingEnd":    1759950335,
          "dateStart":     "2025-10-08T07:02:50+00:00",          # ISO 8601
          "dateEnd":       "2025-10-08T19:05:35+00:00",
          "hlsMasterUrl":  "https://vod.media.eup.glcloud.eu/.../master.m3u8",
          "hlsAudioUrls": {
            "or": "https://.../index-f4-a1.m3u8",  # original/floor audio (default-selected)
            "en": "https://.../index-f4-a5.m3u8"   # English interpretation
          },
          "playerDownload": true
        },
        ...
      ]
    }

CRE speech bodies preserve the original spoken language (a Spanish MEP's text
stays in Spanish even on the ``_EN.html`` URL), so the OR/floor audio track is
the right alignment target — it matches the speech text. EN is the English
interpretation overlay; keep both for future routing.
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger(__name__)

# Parse: #EXT-X-MEDIA:TYPE=AUDIO,...,LANGUAGE="en",...,URI="https://..."
HLS_AUDIO_LINE_RE = re.compile(
    r'#EXT-X-MEDIA:[^\n]*?TYPE=AUDIO[^\n]*',
    re.IGNORECASE,
)
HLS_FIELD_RE = re.compile(r'\b(\w+)="([^"]*)"')

# EP's HLS marks the floor/original audio with LANGUAGE="qaj" (a private-use
# ISO 639-2 code) and AUTOSELECT=YES,DEFAULT=YES. We accept either signal.
OR_LANGUAGE_CODES = {"qaj", "or"}


def _normalize_iso(dt_str: str) -> str:
    """Parse "2025-10-08T07:02:50.000Z" → ISO 8601 with timezone offset."""
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str).astimezone(timezone.utc).isoformat()


def _parse_audio_renditions(playlist: str) -> list[dict]:
    """Parse the audio renditions out of an HLS master playlist."""
    renditions = []
    for line_match in HLS_AUDIO_LINE_RE.finditer(playlist):
        attrs = dict(HLS_FIELD_RE.findall(line_match.group(0)))
        renditions.append(attrs)
    return renditions


def extract_audio_urls(hls_master_url: str) -> dict[str, str]:
    """Fetch the HLS master and return a mapping of well-known audio tracks.

    Returns at most two keys::

        {"or": "<URL of original/floor audio>",
         "en": "<URL of English interpretation>"}

    OR is picked as the rendition with LANGUAGE in {qaj, or} or the
    AUTOSELECT+DEFAULT rendition.
    """
    req = Request(hls_master_url, headers={"User-Agent": "optv-eu-parser/0.1"})
    try:
        with urlopen(req, timeout=30) as resp:
            playlist = resp.read().decode("utf-8", errors="replace")
    except (URLError, OSError) as e:
        logger.warning(f"failed to fetch HLS master {hls_master_url}: {e}")
        return {}

    renditions = _parse_audio_renditions(playlist)
    out: dict[str, str] = {}
    for r in renditions:
        lang = (r.get("LANGUAGE") or "").lower()
        if lang == "en" and "en" not in out:
            out["en"] = r.get("URI") or ""
        if "or" not in out and (lang in OR_LANGUAGE_CODES or
                                (r.get("DEFAULT") == "YES" and r.get("AUTOSELECT") == "YES")):
            out["or"] = r.get("URI") or ""
    return {k: v for k, v in out.items() if v}


def parse_event(event: dict) -> dict:
    """Convert one scraper event payload into the intermediate per-sitting record."""
    common_id = event.get("commonId") or ""
    hls_master = event.get("playerUrl") or ""
    audio_urls = extract_audio_urls(hls_master) if hls_master else {}
    if "or" not in audio_urls:
        logger.warning(f"{common_id}: no OR/original audio rendition found in HLS master")
    return {
        "eventRef": common_id,
        "title": event.get("title") or "",
        "sittingStart": int(event.get("startTime") or 0),
        "sittingEnd": int(event.get("endTime") or 0),
        "dateStart": _normalize_iso(event["startDate"]) if event.get("startDate") else None,
        "dateEnd": _normalize_iso(event["endDate"]) if event.get("endDate") else None,
        "hlsMasterUrl": hls_master,
        "hlsAudioUrls": audio_urls,
        "playerDownload": bool(event.get("playerDownload")),
    }


def parse_media_for_session(config, session: str) -> dict:
    """Read the raw events file for the given session-key (YYYYMMDD) and
    produce the intermediate media doc."""
    raw_path = config.dir("media") / f"raw-{session}-events.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"[{session}] raw events file missing: {raw_path}")
    raw = json.loads(raw_path.read_text())
    sittings = [parse_event(ev) for ev in raw.get("events") or []]
    return {
        "meta": {
            "session": session,
            "date": raw.get("date"),
            "parliament": "EU",
            "processing": {
                "parse_media": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            },
        },
        "data": sittings,
    }


def parse_media_directory(config, args) -> None:
    """Workflow hook entry: parse all raw-*-events.json files in the media dir."""
    media_dir = config.dir("media")
    raw_files = sorted(media_dir.glob("raw-*-events.json"))
    if not raw_files:
        logger.warning(f"no raw-*-events.json under {media_dir}")
        return
    for raw_path in raw_files:
        m = re.match(r"raw-(\d{8})-events\.json$", raw_path.name)
        if not m:
            continue
        session = m.group(1)
        if getattr(args, "limit_session", None):
            try:
                if not re.match(args.limit_session, session):
                    continue
            except re.error:
                if args.limit_session != session:
                    continue
        out_path = config.file(session, "media")
        if out_path.exists() and not args.force and out_path.stat().st_mtime > raw_path.stat().st_mtime:
            logger.debug(f"[{session}] media intermediate cached")
            continue
        logger.info(f"[{session}] parsing media")
        doc = parse_media_for_session(config, session)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"[{session}] wrote {out_path.name} ({len(doc['data'])} sitting(s))")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_file", type=Path, help="Path to raw-{YYYYMMDD}-events.json")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output path (default: stdout)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raw = json.loads(args.raw_file.read_text())
    session = re.search(r"raw-(\d{8})-events\.json$", args.raw_file.name)
    if not session:
        sys.exit(f"unrecognized input filename: {args.raw_file.name}")
    sittings = [parse_event(ev) for ev in raw.get("events") or []]
    doc = {
        "meta": {
            "session": session.group(1),
            "date": raw.get("date"),
            "parliament": "EU",
            "processing": {
                "parse_media": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            },
        },
        "data": sittings,
    }
    out_text = json.dumps(doc, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(out_text)
        logger.info(f"wrote {args.output} ({len(sittings)} sitting(s))")
    else:
        print(out_text)


if __name__ == "__main__":
    main()
