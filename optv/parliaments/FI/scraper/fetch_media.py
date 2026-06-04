#! /usr/bin/env python3
"""Fetch per-session video metadata from the Eduskunta web-broadcast platform.

``verkkolahetys.eduskunta.fi/fi/taysistunnot/taysistunto-{number}-{year}`` is a
Next.js app that server-renders the session data as an RSC (React Server
Component) *flight* payload embedded in ``self.__next_f.push([1, "…"])`` script
chunks. That payload contains, per session:

- ``speakers[]`` — one entry per speech with ``time`` / ``endTime`` (seconds
  into the session video), ``personNumber`` (= ``Henkilo/@muuTunnus`` in the
  PTK and ``henkilonumero`` in SaliDB), ``party.{fi,sv}``, ``topicId``,
  ``onkoVastauspuheenvuoro`` (reply flag) and absolute UTC ``timeStamp``.
- the HLS master playlist + a session MP4 reference on the VideoSync CDN,
  ``sessionCuepoints`` (video start/end), and a ``plenum`` block with the PTK
  link and session title/date.

We extract that into a compact ``{session}-event.json`` the parsers consume.
No browser or private API is needed — the flight payload is in the page HTML.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.FI.scraper"

from optv.parliaments.FI.common import Config, parse_session_str, session_str

logger = logging.getLogger(__name__)

VL_ROOT = "https://verkkolahetys.eduskunta.fi"
SESSION_URL = VL_ROOT + "/fi/taysistunnot/taysistunto-{number}-{year}"
USER_AGENT = "Mozilla/5.0 (OpenParliamentTV-Tools; +https://github.com/OpenParliamentTV)"


def _get_html(url: str, *, timeout: float = 60.0,
              retry_count: int = 5, retry_delay_max: float = 10.0) -> str | None:
    req = Request(url, headers={"User-Agent": USER_AGENT,
                                "Accept": "text/html,application/xhtml+xml"})
    delay = 1.0
    for attempt in range(1, retry_count + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except HTTPError as e:
            if e.code == 404:
                return None
            if attempt >= retry_count:
                raise
        except (URLError, TimeoutError) as e:
            if attempt >= retry_count:
                raise
            logger.warning(f"verkkolähetys retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
        time.sleep(delay)
        delay = min(delay * 2, retry_delay_max)
    return None


def _flight_text(html: str) -> str:
    """Concatenate and un-escape all RSC flight string chunks from the page."""
    chunks = re.findall(r'self\.__next_f\.push\(\[1,(".*?")\]\)', html, re.S)
    return "".join(json.loads(c) for c in chunks)


def _extract_json_value(flight: str, key: str):
    """Bracket-match the JSON array/object that follows ``"key":`` in the flight.

    RSC encodes dates as ``"$D<iso>"`` and undefined as ``$undefined``; we
    normalise both before parsing.
    """
    marker = f'"{key}":'
    i = flight.find(marker)
    if i < 0:
        return None
    j = i + len(marker)
    while j < len(flight) and flight[j] not in "[{":
        j += 1
    if j >= len(flight):
        return None
    open_c = flight[j]
    close_c = "]" if open_c == "[" else "}"
    depth = 0
    in_str = False
    esc = False
    for k in range(j, len(flight)):
        c = flight[k]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == open_c:
            depth += 1
        elif c == close_c:
            depth -= 1
            if depth == 0:
                raw = flight[j:k + 1]
                raw = raw.replace("$undefined", "null")
                raw = re.sub(r'"\$D([^"]+)"', r'"\1"', raw)
                return json.loads(raw)
    return None


def extract_event(html: str) -> dict | None:
    """Pull the compact session-event dict out of the page HTML."""
    flight = _flight_text(html)
    speakers = _extract_json_value(flight, "speakers")
    if speakers is None:
        return None
    plenum = _extract_json_value(flight, "plenum")
    cuepoints = _extract_json_value(flight, "sessionCuepoints") or []
    event_ids = re.findall(r"events/eduskunta/([a-f0-9]{16,})/video", flight)
    event_id = event_ids[0] if event_ids else None
    hls = re.search(r"https?://[^\s\"\\]+playlist\.m3u8[^\s\"\\]*", flight)
    started = _extract_json_value(flight, "sessionStarted")
    ended = _extract_json_value(flight, "sessionEnded")
    return {
        "speakers": speakers,
        "plenum": plenum,
        "sessionCuepoints": cuepoints,
        "eventId": event_id,
        "hlsUrl": hls.group(0) if hls else None,
        "sessionStarted": started,
        "sessionEnded": ended,
    }


def fetch_media(config: Config, period: int, year: int, number: int, *,
                force: bool = False, retry_count: int = 5,
                retry_delay_max: float = 10.0) -> Path | None:
    session = session_str(year, number)
    out = config.raw_event(session)
    if out.exists() and not force:
        logger.info(f"[{session}] event.json exists — skipping (use --force)")
        return out
    url = SESSION_URL.format(number=number, year=year)
    logger.info(f"[{session}] fetching {url}")
    html = _get_html(url, retry_count=retry_count, retry_delay_max=retry_delay_max)
    if html is None:
        logger.warning(f"[{session}] no broadcast page (404) — skipping")
        return None
    event = extract_event(html)
    if event is None or not event.get("speakers"):
        logger.warning(f"[{session}] no speakers[] in page — skipping")
        return None
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(event, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {out.name} ({len(event['speakers'])} speakers)")
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 2026-058")
    parser.add_argument("--period", type=int, default=2023)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    year, number = parse_session_str(args.session)
    config = Config(args.data_dir)
    fetch_media(config, args.period, year, number, force=args.force)


if __name__ == "__main__":
    main()
