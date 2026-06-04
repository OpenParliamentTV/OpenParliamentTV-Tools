#! /usr/bin/env python3
"""
Fetch one Riksdag protokoll's anföranden + per-debate metadata.

Per-session walk: given a protokoll dok_id (e.g. ``HD0991``), this script
fetches:

1. ``dokumentstatus/{protokoll_id}.json`` — protokoll metadata (riksmöte,
   nummer, datum, titel). Used to derive the OPTV session string
   ``{rm_start_year}-{nummer:03d}`` (e.g. ``2025-091``).
2. ``anforande/{protokoll_id}-N.json`` for N = 1, 2, … — each call returns
   one speech's full payload (text, speaker, rel_dok_id, …). Walks until
   ``STOP_AFTER_EMPTY`` consecutive empty responses (the API answers 200 with
   an empty body for out-of-range N, *not* 404).
3. ``dokumentstatus/{rel_dok_id}.json?utdata=debatt,media`` for each unique
   ``rel_dok_id`` discovered in step 2 — provides per-debate video URLs and
   per-speech timestamps (``startpos``, ``anf_sekunder``, ``anf_klockslag``).

Outputs:
- ``original/proceedings/{session}-anforanden.json`` — bundled session file:
  ``{"protokoll": <dokumentstatus.dokument>, "anforanden": [<full-anforande-payload>, …]}``.
  ``Config.sessions()`` globs this filename to enumerate sessions on disk.
- ``original/media/{rel_dok_id}-debatt.json`` — raw per-debate dokumentstatus
  payload (one file per unique rel_dok_id).

Idempotent: skips the per-anforande walk if the bundle already exists;
skips per-debate fetches whose output files already exist. ``--force``
re-fetches everything.

The list endpoint ``anforandelista`` is not used here — its filter
parameters (``prot_nr``, ``dok_id``, ``rm``, ``bet``, ``from``/``tom``,
page index ``p=``) are silently ignored by the upstream API
(verified 2026-04-30). Direct per-speech lookup is the reliable per-session
path; the bulk dataset at ``data.riksdagen.se/dataset/anforande/`` is the
recommended path for period-wide backfill (not implemented here).
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
from urllib.request import Request, urlopen

# Allow ``./fetch_session.py`` (script) and ``python -m
# optv.parliaments.SE.scraper.fetch_session`` (module). Mirrors the bootstrap
# in optv/parliaments/DE/workflow.py.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                  # SE/
    sys.path.insert(0, str(module_dir.parent.parent.parent))    # repo root
    __package__ = "optv.parliaments.SE.scraper"

from optv.parliaments.SE.common import Config

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

API_ROOT = "https://data.riksdagen.se"
USER_AGENT = "OpenParliamentTV-Tools (+https://github.com/OpenParliamentTV)"

DEFAULT_RETRY_COUNT = 5
DEFAULT_RETRY_DELAY_MAX = 10.0
# Out-of-range anforande IDs return HTTP 200 with an empty body (not 404),
# so we walk until we see this many empty responses in a row.
STOP_AFTER_EMPTY = 3
# Politeness delay between requests during the per-speech walk.
INTER_REQUEST_DELAY = 0.05


def _fetch(url: str, retry_count: int, retry_delay_max: float) -> bytes:
    """GET ``url`` and return raw response bytes.

    Retries transient network errors and 5xx responses with exponential
    backoff capped at ``retry_delay_max``. HTTP 4xx (other than 404) and
    final-attempt failures raise.
    """
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            req = Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            })
            with urlopen(req, timeout=30) as resp:
                return resp.read()
        except HTTPError as e:
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
    # Exhausted retries (only reachable if last attempt also failed transiently).
    raise RuntimeError(f"Exhausted {retry_count} attempts for {url}: {last_exc}")


def fetch_json(url: str, retry_count: int, retry_delay_max: float):
    """Fetch ``url`` and parse JSON. Returns ``None`` for an empty body
    (Riksdag's idiom for "no such resource" on the anforande endpoint).
    """
    body = _fetch(url, retry_count, retry_delay_max)
    if not body.strip():
        return None
    return json.loads(body)


def session_string_from_protokoll(prot_dokument: dict) -> str:
    """Build the SE session identifier from the protokoll's ``dokument`` block.

    Format: ``{rm_start_year}-{nummer:03d}`` — e.g. rm ``"2025/26"`` + nummer
    ``"91"`` → ``"2025-091"``.
    """
    rm = prot_dokument["rm"]                 # e.g. "2025/26"
    nummer = int(prot_dokument["nummer"])    # e.g. 91
    rm_start_year = int(rm.split("/")[0])
    return f"{rm_start_year}-{nummer:03d}"


def fetch_protokoll(protokoll_id: str, retry_count: int, retry_delay_max: float) -> dict:
    """Fetch dokumentstatus for the protokoll. Returns the ``dokumentstatus`` block."""
    url = f"{API_ROOT}/dokumentstatus/{protokoll_id}.json"
    logger.info(f"GET {url}")
    payload = fetch_json(url, retry_count, retry_delay_max)
    if payload is None:
        raise ValueError(f"Protokoll {protokoll_id} returned empty body — no such document?")
    return payload["dokumentstatus"]


def walk_anforanden(protokoll_id: str, *, retry_count: int, retry_delay_max: float,
                    limit: int | None = None):
    """Walk ``anforande/{protokoll_id}-N.json`` for N = 1, 2, ….

    Stops after ``STOP_AFTER_EMPTY`` consecutive empty responses. Yields the
    full per-speech payload (the ``anforande`` sub-dict) in numeric order.
    Each yielded dict carries an extra ``_fetch_id`` key with the URL-style
    id (``HD0991-1``) since the API's own ``anforande_id`` is a UUID-style
    value, not the URL form.
    """
    n = 1
    consecutive_empty = 0
    yielded = 0
    while consecutive_empty < STOP_AFTER_EMPTY:
        if limit is not None and yielded >= limit:
            logger.info(f"Reached --limit-anforanden={limit}, stopping walk")
            return
        fetch_id = f"{protokoll_id}-{n}"
        url = f"{API_ROOT}/anforande/{fetch_id}.json"
        payload = fetch_json(url, retry_count, retry_delay_max)
        anf = (payload or {}).get("anforande") if payload else None
        if not anf:
            consecutive_empty += 1
            logger.debug(f"  miss: {fetch_id} (consecutive_empty={consecutive_empty})")
        else:
            consecutive_empty = 0
            anf["_fetch_id"] = fetch_id
            yielded += 1
            logger.debug(f"  ok:   {fetch_id} ({(anf.get('talare') or '')[:50]})")
            yield anf
        n += 1
        time.sleep(INTER_REQUEST_DELAY)


def fetch_debate(rel_dok_id: str, retry_count: int, retry_delay_max: float) -> dict:
    """Fetch ``dokumentstatus?utdata=debatt,media`` for one debate dok_id."""
    url = f"{API_ROOT}/dokumentstatus/{rel_dok_id}.json?utdata=debatt,media"
    logger.info(f"GET {url}")
    payload = fetch_json(url, retry_count, retry_delay_max)
    if payload is None:
        raise ValueError(f"Debate dokumentstatus {rel_dok_id} returned empty body")
    return payload["dokumentstatus"]


def fetch_session(config: Config, protokoll_id: str, *, force: bool,
                  retry_count: int, retry_delay_max: float,
                  limit_anforanden: int | None = None) -> str:
    """Fetch all session data for a single protokoll.

    Returns the resolved OPTV session string (e.g. ``"2025-091"``).
    """
    prot_status = fetch_protokoll(protokoll_id, retry_count, retry_delay_max)
    prot_doc = prot_status["dokument"]
    session = session_string_from_protokoll(prot_doc)
    logger.info(f"Resolved {protokoll_id} → session {session} ({prot_doc.get('titel', '')})")

    proceedings_dir = config.dir("proceedings", create=True)
    media_dir = config.dir("media", create=True)
    bundle_path = proceedings_dir / f"{session}-anforanden.json"

    if bundle_path.exists() and not force:
        logger.info(f"Bundle exists, reusing: {bundle_path} (use --force to refetch)")
        bundle = json.loads(bundle_path.read_text())
    else:
        logger.info(f"Walking anforande/{protokoll_id}-N.json …")
        anforanden = list(walk_anforanden(
            protokoll_id,
            retry_count=retry_count,
            retry_delay_max=retry_delay_max,
            limit=limit_anforanden,
        ))
        logger.info(f"Fetched {len(anforanden)} anförande(n) for {session}")
        bundle = {"protokoll": prot_doc, "anforanden": anforanden}
        bundle_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False))
        logger.info(f"Wrote {bundle_path}")

    rel_dok_ids = sorted({
        a.get("rel_dok_id") for a in bundle["anforanden"]
        if a.get("rel_dok_id")
    })
    logger.info(f"Fetching {len(rel_dok_ids)} debate dokumentstatus payload(s)")
    for rel_id in rel_dok_ids:
        out = media_dir / f"{rel_id}-debatt.json"
        if out.exists() and not force:
            logger.debug(f"  exists: {out.name}")
            continue
        debate = fetch_debate(rel_id, retry_count, retry_delay_max)
        out.write_text(json.dumps(debate, indent=2, ensure_ascii=False))
        logger.info(f"  wrote: {out.name}")

    return session


def main():
    parser = argparse.ArgumentParser(
        description="Fetch one Riksdag protokoll into the SE data directory."
    )
    parser.add_argument("data_dir", type=Path,
                        help="OpenParliamentTV-Data-SE root directory")
    parser.add_argument("--protokoll", required=True,
                        help="Protokoll dok_id (e.g. HD0991 for 2025/26 protokoll 91)")
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=False,
                        help="Re-fetch even if files exist")
    parser.add_argument("--retry-count", type=int, default=DEFAULT_RETRY_COUNT)
    parser.add_argument("--retry-delay-max", type=float, default=DEFAULT_RETRY_DELAY_MAX)
    parser.add_argument("--limit-anforanden", type=int, default=None,
                        help="Stop the per-speech walk after this many speeches "
                             "(useful for testing the rest of the pipeline quickly)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = Config(args.data_dir)
    session = fetch_session(
        config, args.protokoll,
        force=args.force,
        retry_count=args.retry_count,
        retry_delay_max=args.retry_delay_max,
        limit_anforanden=args.limit_anforanden,
    )
    logger.info(f"Done. Session: {session}")


if __name__ == "__main__":
    main()
