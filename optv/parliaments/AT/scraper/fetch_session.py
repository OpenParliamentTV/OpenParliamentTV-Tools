#! /usr/bin/env python3
"""Fetch one AT Nationalrat sitting from parlament.gv.at.

Per-sitting data comes from three live surfaces:

1. **Mediathek detail page** ``/aktuelles/mediathek/{GP}/NRSITZ/{n}?json=true``
   — the documented Open Data JSON endpoint (``?json=true`` is **case-sensitive,
   lowercase**; ``?json=TRUE`` returns the SvelteKit HTML shell). It carries the
   media spine ``content.mediumdata.debatten[].redner[]``. Each ``redner`` is one
   on-camera speech with ``std_id`` (proceedings join key), ``uuid``/``ts``
   (per-speech video clip), ``name``, ``pad_intern`` (person id), ``time`` and
   ``protokoll`` (the per-speech stenographic-protocol HTML).
2. **Video resolver** ``api.ausp.cloud.insysgo.com/.../AcquireContent?id={uuid}&ts={ts}``
   → HLS (trimmed via ``startseconds``/``stopseconds``), MP4 and a per-speech MP3
   clip. This is the one **undocumented/internal** dependency (the same one the
   site itself calls); the Mediathek JSON exposes ``uuid``/``ts`` but not the
   resolved media URLs.
3. **Protocol HTML** (``redner.protokoll``) — the speech's verbatim text.

Outputs (idempotent; ``--force`` refetches):
- ``original/media/{session}-mediathek.json`` — the raw debatten payload with
  each ``redner`` augmented with a resolved ``video`` block. ``Config.sessions()``
  globs this file to enumerate downloaded sittings.
- ``original/proceedings/{session}/{std_id}.html`` — one protocol HTML per speech.

Session key: ``{period}{sitting:03d}`` (e.g. period 27, sitting 144 → ``27144``).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                  # AT/
    sys.path.insert(0, str(module_dir.parent.parent.parent))    # repo root
    __package__ = "optv.parliaments.AT.scraper"

from optv.parliaments.AT.common import Config

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

BASE_URL = "https://www.parlament.gv.at"
VIDEO_API = "https://api.ausp.cloud.insysgo.com/v1/ParliamentMedia/AcquireContent"
USER_AGENT = "OpenParliamentTV-Tools (+https://github.com/OpenParliamentTV)"

DEFAULT_RETRY_COUNT = 10
DEFAULT_RETRY_DELAY_MAX = 10.0
# A sitting "exists" if its Mediathek page yields debatten; walk numbers until
# this many consecutive sittings yield nothing (EP27 sittings are contiguous).
STOP_AFTER_EMPTY = 8
INTER_REQUEST_DELAY = 0.05

_ROMAN = [(1000, "M"), (900, "CM"), (500, "D"), (400, "CD"), (100, "C"),
          (90, "XC"), (50, "L"), (40, "XL"), (10, "X"), (9, "IX"),
          (5, "V"), (4, "IV"), (1, "I")]


def to_roman(n: int) -> str:
    """Electoral-period number → Roman GP code (27 → ``XXVII``)."""
    out = []
    for value, sym in _ROMAN:
        while n >= value:
            out.append(sym)
            n -= value
    return "".join(out)


def _fetch(url: str, retry_count: int, retry_delay_max: float, *, accept: str) -> bytes:
    """GET ``url`` → raw bytes, retrying transient errors / 5xx with backoff.

    HTTP 404 returns ``b""`` (the caller treats an empty body as "no such
    resource"), matching the SE scraper's idiom.
    """
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept})
            with urlopen(req, timeout=60) as resp:
                return resp.read()
        except HTTPError as e:
            if e.code == 404:
                return b""
            if 500 <= e.code < 600 and attempt < retry_count:
                logger.warning(f"HTTP {e.code} on {url}, retry {attempt}/{retry_count} after {delay:.1f}s")
                time.sleep(min(delay, retry_delay_max))
                delay = min(delay * 2, retry_delay_max)
                last_exc = e
                continue
            raise
        except (URLError, TimeoutError, ConnectionError) as e:
            if attempt < retry_count:
                logger.warning(f"{type(e).__name__} on {url}: {e}, retry {attempt}/{retry_count} after {delay:.1f}s")
                time.sleep(min(delay, retry_delay_max))
                delay = min(delay * 2, retry_delay_max)
                last_exc = e
                continue
            raise
    raise RuntimeError(f"Exhausted {retry_count} attempts for {url}: {last_exc}")


def _classify_formats(formats: list[dict]) -> dict:
    """Map an AcquireContent ``Formats`` list to ``{hls, mp4, mp3,
    startSeconds, stopSeconds, duration}``.

    The HLS format addresses the full-session asset trimmed by the
    ``startseconds``/``stopseconds`` query; the MP4/MP3 are ready per-speech
    clip assets. ``duration`` is the trim window (the platform's per-speech
    length).
    """
    out: dict = {"hls": None, "mp4": None, "mp3": None,
                 "startSeconds": None, "stopSeconds": None, "duration": None}
    for f in formats or []:
        url = (f.get("Url") or "").strip()
        if not url:
            continue
        path = urlparse(url).path.lower()
        if path.endswith(".m3u8"):
            out["hls"] = url
            qs = parse_qs(urlparse(url).query)
            if "startseconds" in qs:
                out["startSeconds"] = int(qs["startseconds"][0])
            if "stopseconds" in qs:
                out["stopSeconds"] = int(qs["stopseconds"][0])
        elif path.endswith(".mp4"):
            out["mp4"] = url
        elif path.endswith(".mp3"):
            out["mp3"] = url
    if out["startSeconds"] is not None and out["stopSeconds"] is not None:
        out["duration"] = max(0, out["stopSeconds"] - out["startSeconds"])
    return out


def resolve_video(uuid: str, ts, retry_count: int, retry_delay_max: float) -> dict | None:
    """Resolve a per-speech video clip via the AcquireContent endpoint."""
    url = f"{VIDEO_API}?platformCodename=www&role=clean&id={uuid}&ts={ts}"
    body = _fetch(url, retry_count, retry_delay_max, accept="application/json")
    if not body.strip():
        return None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None
    media_files = payload.get("MediaFiles") or []
    if not media_files:
        logger.debug(f"  no MediaFiles for uuid={uuid} ts={ts}: "
                     f"{(payload.get('Result') or {}).get('MessageCodename')}")
        return None
    return _classify_formats(media_files[0].get("Formats") or [])


def fetch_debatten(mediathek_url: str, retry_count: int, retry_delay_max: float) -> tuple[list, dict]:
    """Return ``(debatten, session_meta)`` for a Mediathek sitting via the
    documented Open Data endpoint ``?json=true``.

    ``?json=true`` is **case-sensitive — lowercase only** (``?json=TRUE`` returns
    the SvelteKit HTML shell). The response carries the spine at
    ``content.mediumdata.debatten[].redner[]``.

    Out-of-range / non-existent sittings return a persistent 5xx (not 404); after
    the built-in retries that surfaces as an ``HTTPError``, which here means "no
    such sitting" → ``([], {})``. Genuine network failures (``URLError``) still
    propagate so an outage is loud rather than silently truncating a discovery
    walk.
    """
    try:
        body = _fetch(mediathek_url + "?json=true", retry_count, retry_delay_max,
                      accept="application/json")
    except HTTPError as e:
        logger.debug("?json=true HTTP %s for %s — treating as no sitting", e.code, mediathek_url)
        return [], {}
    if not body.strip():
        return [], {}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("?json=true for %s did not return JSON — skipping", mediathek_url)
        return [], {}
    content = (payload or {}).get("content") or {}
    debatten = (content.get("mediumdata") or {}).get("debatten") or []
    meta = {k: v for k, v in content.items() if k != "mediumdata"}
    return debatten, meta


def fetch_session(config: Config, period: int, sitting: int, *, force: bool,
                  retry_count: int, retry_delay_max: float) -> str | None:
    """Fetch one sitting. Returns the session key, or ``None`` if it has no
    on-camera debatten (no such sitting / no video)."""
    gp_code = to_roman(period)
    session = f"{period}{sitting:03d}"
    media_dir = config.dir("media", create=True)
    raw_path = media_dir / f"{session}-mediathek.json"

    if raw_path.exists() and not force:
        logger.info(f"[{session}] raw payload exists, reusing (use --force to refetch)")
        return session

    mediathek_url = f"{BASE_URL}/aktuelles/mediathek/{gp_code}/NRSITZ/{sitting}"
    logger.info(f"[{session}] GET {mediathek_url}?json=true")
    debatten, session_meta = fetch_debatten(mediathek_url, retry_count, retry_delay_max)
    if not debatten:
        logger.info(f"[{session}] no debatten — no such sitting / no video, skipping")
        return None

    proc_dir = config.dir("proceedings", create=True) / session
    proc_dir.mkdir(parents=True, exist_ok=True)

    n_speeches = 0
    fetched_protocols: set[str] = set()
    for debatte in debatten:
        debatte_id = debatte.get("debatte_id")
        for redner in debatte.get("redner") or []:
            n_speeches += 1
            uuid = redner.get("uuid")
            ts = redner.get("ts")
            redner["sourcePage"] = f"{mediathek_url}?DEBATTE={debatte_id}&TS={ts}"
            if uuid and ts is not None:
                redner["video"] = resolve_video(uuid, ts, retry_count, retry_delay_max)
            else:
                redner["video"] = None
            # Per-speech protocol HTML, keyed by std_id.
            protokoll = redner.get("protokoll")
            std_id = redner.get("std_id")
            if protokoll and std_id is not None:
                out_html = proc_dir / f"{std_id}.html"
                if str(std_id) not in fetched_protocols and (force or not out_html.exists()):
                    purl = protokoll if protokoll.startswith("http") else f"{BASE_URL}{protokoll}"
                    body = _fetch(purl, retry_count, retry_delay_max, accept="text/html")
                    if body.strip():
                        out_html.write_bytes(body)
                        logger.debug(f"  wrote {out_html.name}")
                    else:
                        logger.warning(f"[{session}] empty protocol for std_id={std_id} ({purl})")
                fetched_protocols.add(str(std_id))
            time.sleep(INTER_REQUEST_DELAY)

    raw = {
        "gpCode": gp_code,
        "period": period,
        "sitting": sitting,
        "session": session,
        "mediathekURL": mediathek_url,
        "sessionMeta": session_meta,
        "debatten": debatten,
    }
    raw_path.write_text(json.dumps(raw, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {raw_path.name} ({len(debatten)} debatten, "
                f"{n_speeches} speeches, {len(fetched_protocols)} protocols)")
    return session


def discover_sittings(period: int, *, retry_count: int, retry_delay_max: float,
                      start: int = 1, stop_after_empty: int = STOP_AFTER_EMPTY) -> list[int]:
    """Walk sitting numbers from ``start`` until ``stop_after_empty`` consecutive
    misses. EP27 sittings are contiguous and all carry video, so a sequential
    probe of the Mediathek pages enumerates the full period."""
    gp_code = to_roman(period)
    found: list[int] = []
    consecutive_empty = 0
    n = start
    while consecutive_empty < stop_after_empty:
        url = f"{BASE_URL}/aktuelles/mediathek/{gp_code}/NRSITZ/{n}"
        debatten, _ = fetch_debatten(url, retry_count, retry_delay_max)
        has = bool(debatten)
        if has:
            found.append(n)
            consecutive_empty = 0
            logger.info(f"  sitting {n}: present")
        else:
            consecutive_empty += 1
            logger.debug(f"  sitting {n}: empty ({consecutive_empty}/{stop_after_empty})")
        n += 1
        time.sleep(INTER_REQUEST_DELAY)
    return found


def main():
    parser = argparse.ArgumentParser(description="Fetch one AT Nationalrat sitting.")
    parser.add_argument("data_dir", type=Path, help="OpenParliamentTV-Data-AT root directory")
    parser.add_argument("--period", type=int, default=27, help="Electoral period (default 27)")
    parser.add_argument("--sitting", type=int, required=True, help="Sitting number (e.g. 144)")
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--retry-count", type=int, default=DEFAULT_RETRY_COUNT)
    parser.add_argument("--retry-delay-max", type=float, default=DEFAULT_RETRY_DELAY_MAX)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    session = fetch_session(config, args.period, args.sitting, force=args.force,
                            retry_count=args.retry_count, retry_delay_max=args.retry_delay_max)
    logger.info(f"Done. Session: {session}")


if __name__ == "__main__":
    main()
