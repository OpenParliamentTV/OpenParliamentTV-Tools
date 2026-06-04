#! /usr/bin/env python3
"""Fetch per-IVOD detail (with transcripts) for one plenary's IVODs.

Reads ``original/media/{session}-media.json`` (produced by
:mod:`.fetch_media`), then calls ``GET /v2/ivods/{IVOD_ID}`` for every
non-``Full`` IVOD listed. Stores the joined result at
``original/proceedings/{session}-proceedings.json`` as::

    {"meta": {...}, "ivods": [<detail with transcript.whisperx / .pyannote>, ...]}

The ``Full`` IVOD entries (a session-long video, 種類="Full") are skipped:
the per-speech ``Clip`` IVODs are the platform's per-speech records.

Idempotent: existing proceedings file is reused unless ``--force``. When
re-downloading, individual IVOD detail blobs are also cached on disk by
:class:`LYApiClient`, so a re-run is a no-op against the upstream API.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                          # TW/
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))     # repo root
    __package__ = "optv.parliaments.TW.scraper"

from optv.parliaments.TW.common import Config
from .ly_api import LYApiClient

logger = logging.getLogger(__name__)


def _is_clip(ivod: dict) -> bool:
    """Only per-speech clips carry meaningful per-speaker text/timestamps."""
    return (ivod.get("影片種類") or "").lower() == "clip"


def fetch_proceedings_for_session(
    session: str,
    config: Config,
    client: LYApiClient,
    *,
    force: bool = False,
    limit_ivods: int | None = None,
) -> Path | None:
    """Fetch IVOD details for all per-speech clips in one session.

    Returns the proceedings JSON path, or None if the media file is missing.
    """
    ivods_file = config.file(session, "ivods")
    if not ivods_file.exists():
        logger.error(f"[{session}] no IVOD list at {ivods_file}; "
                     "run --download-original to populate first.")
        return None

    out = config.file(session, "details", create=True)
    if out.exists() and not force:
        logger.info(f"[{session}] details cached → {out.name}")
        return out

    media_doc = json.loads(ivods_file.read_text())
    ivods = media_doc.get("ivods") or []
    clips = [iv for iv in ivods if _is_clip(iv)]
    if limit_ivods is not None:
        clips = clips[:limit_ivods]

    logger.info(f"[{session}] fetching detail for {len(clips)} clip(s) "
                f"(skipped {len(ivods) - len(clips)} non-clip)")

    details: list[dict] = []
    for i, stub in enumerate(clips, start=1):
        ivod_id = stub.get("IVOD_ID")
        if not ivod_id:
            logger.warning(f"[{session}] stub {i} has no IVOD_ID; skipping")
            continue
        try:
            data = client.get_ivod(ivod_id)
        except (LookupError, Exception) as e:  # noqa: BLE001
            logger.warning(f"[{session}] IVOD {ivod_id} fetch failed: "
                           f"{type(e).__name__}: {e}; skipping")
            continue
        details.append(data)
        if i % 10 == 0:
            logger.info(f"[{session}] fetched {i}/{len(clips)} details")

    doc = {
        "meta": {
            "session": session,
            "meetingCode": (media_doc.get("meta") or {}).get("meetingCode"),
            "fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "count": len(details),
        },
        "ivods": details,
    }
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {out.name} ({len(details)} details)")
    return out


def download_proceedings(config: Config, args) -> None:
    """Workflow hook: fetch IVOD detail for every in-scope session."""
    client = LYApiClient(
        cache_dir=config.dir("cache", create=True) / "ly-api",
        retry_count=getattr(args, "retry_count", 5) or 5,
        retry_delay_max=getattr(args, "retry_delay_max", 10.0),
    )
    limit = getattr(args, "limit_ivods", None)
    sessions = config.sessions()
    if not sessions:
        logger.info("No media files on disk; nothing to fetch proceedings for.")
        return
    for session in sessions:
        fetch_proceedings_for_session(
            session, config, client,
            force=args.force,
            limit_ivods=limit,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True,
                        help="Session key (e.g. 1105011)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit-ivods", type=int, default=None,
                        help="Fetch at most N IVODs (testing only)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args.retry_count = 5
    args.retry_delay_max = 10.0
    config = Config(args.data_dir)
    client = LYApiClient(cache_dir=config.dir("cache", create=True) / "ly-api")
    fetch_proceedings_for_session(args.session, config, client,
                                  force=args.force,
                                  limit_ivods=args.limit_ivods)


if __name__ == "__main__":
    main()
