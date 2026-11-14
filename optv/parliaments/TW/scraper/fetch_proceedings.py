#! /usr/bin/env python3
"""Fetch per-IVOD detail (with transcripts) for one plenary's IVODs.

Reads ``original/media/{session}-ivods.json`` (produced by
:mod:`.fetch_media`), then calls ``GET /v2/ivods/{IVOD_ID}`` for every
``Clip`` IVOD listed. Stores the joined result at
``original/proceedings/{session}-details.json`` as::

    {"meta": {...}, "ivods": [<detail with transcript.whisperx / .pyannote>, ...]}

The ``Full`` IVOD entries (a session-long video, 種類="Full") are skipped:
the per-speech ``Clip`` IVODs are the platform's per-speech records.

Incremental: only clips missing from the details file are fetched (plus
clips whose WhisperX transcript had not been published yet), and the file
is rewritten only when it changed. A clip whose fetch failed (e.g. after
exhausting retries on HTTP 429) is therefore retried on the next run.
Individual IVOD detail blobs are also cached on disk by :class:`LYApiClient`.
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

from optv.parliaments.TW.common import Config, sessions_in_scope
from .ly_api import LYApiClient

logger = logging.getLogger(__name__)


def _is_clip(ivod: dict) -> bool:
    """Only per-speech clips carry meaningful per-speaker text/timestamps."""
    return (ivod.get("影片種類") or "").lower() == "clip"


def _has_whisperx(detail: dict) -> bool:
    return bool((detail.get("transcript") or {}).get("whisperx"))


def _wants_refetch(stub: dict, detail: dict | None) -> bool:
    """A clip needs (re-)fetching if we have no detail, or the stub advertises
    an AI transcript that the stored detail does not carry yet."""
    if detail is None:
        return True
    return "ai-transcript" in (stub.get("支援功能") or []) and not _has_whisperx(detail)


def fetch_proceedings_for_session(
    session: str,
    config: Config,
    client: LYApiClient,
    *,
    force: bool = False,
    limit_ivods: int | None = None,
) -> Path | None:
    """Fetch IVOD details for all per-speech clips in one session.

    Returns the details JSON path, or None if the IVOD index is missing or
    no detail could be fetched.
    """
    ivods_file = config.file(session, "ivods")
    if not ivods_file.exists():
        logger.error(f"[{session}] no IVOD list at {ivods_file}; "
                     "run --download-original to populate first.")
        return None

    out = config.file(session, "details", create=True)
    media_doc = json.loads(ivods_file.read_text())
    ivods = media_doc.get("ivods") or []
    clips = [iv for iv in ivods if _is_clip(iv) and iv.get("IVOD_ID")]
    if limit_ivods is not None:
        clips = clips[:limit_ivods]

    existing_list: list[dict] = []
    if out.exists() and not force:
        existing_list = json.loads(out.read_text()).get("ivods") or []
    existing = {str(d.get("IVOD_ID")): d for d in existing_list}

    todo = [stub for stub in clips
            if _wants_refetch(stub, existing.get(str(stub["IVOD_ID"])))]
    if not todo and len(existing) == len(clips):
        logger.info(f"[{session}] details up to date → {out.name}")
        return out

    logger.info(f"[{session}] fetching detail for {len(todo)} of {len(clips)} clip(s)")
    fetched: dict[str, dict] = {}
    for i, stub in enumerate(todo, start=1):
        ivod_id = stub["IVOD_ID"]
        try:
            fetched[str(ivod_id)] = client.get_ivod(ivod_id)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[{session}] IVOD {ivod_id} fetch failed: "
                           f"{type(e).__name__}: {e}; will retry next run")
            continue
        if i % 10 == 0:
            logger.info(f"[{session}] fetched {i}/{len(todo)} details")

    details: list[dict] = []
    for stub in clips:
        key = str(stub["IVOD_ID"])
        detail = fetched.get(key) or existing.get(key)
        if detail is not None:
            details.append(detail)
    if not details:
        logger.warning(f"[{session}] no IVOD details available yet")
        return None
    if details == existing_list:
        logger.info(f"[{session}] details unchanged → {out.name}")
        return out

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
    sessions = sessions_in_scope(config, args)
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
