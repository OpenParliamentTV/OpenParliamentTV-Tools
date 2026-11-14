#! /usr/bin/env python3
"""Fetch the official per-clip text (gazette record) from the IVOD site.

For every clip in ``original/media/{session}-ivods.json`` we GET
``https://ivod.ly.gov.tw/Demand/Speech/{IVOD_ID}``. The page is an HTML
fragment, empty (HTTP 200, no body) until the gazette is published, which
happens weeks after the sitting. Filled pages land in
``original/proceedings/{session}-records.json``::

    {"meta": {"session", "source", "updatedAt"},
     "records": {"<IVOD_ID>": "<html fragment>", ...}}

Only clips with text are stored, and the file is written only when a record
was added or changed, so its mtime drives the proceedings re-parse.

Re-check policy for clips without text yet: at most once per
``RECHECK_INTERVAL`` (about daily), and no longer once the sitting is more
than ``MAX_AGE_DAYS`` old (a backfill of older sittings still checks each clip
once). Check times live in ``cache/tw-speech-record-checks.json``.

When official text arrives for a session that is already published, the
publish guard would refuse the changed transcript (and the merge/align stages
skip already-aligned sessions), so the session key is appended to
``cache/tw-official-text-upgrades.txt``; the ``update`` wrapper re-runs those
sessions with ``--rebuild``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                          # TW/
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))     # repo root
    __package__ = "optv.parliaments.TW.scraper"

from optv.parliaments.TW.common import (
    OFFICIAL_TEXT_UPGRADES_FILE,
    SPEECH_RECORD_URL,
    Config,
    sessions_in_scope,
)
from .ly_api import LYApiClient

logger = logging.getLogger(__name__)

RECHECK_INTERVAL = timedelta(hours=20)
MAX_AGE_DAYS = 180
CHECKS_FILE = "tw-speech-record-checks.json"


def _is_clip(ivod: dict) -> bool:
    return (ivod.get("影片種類") or "").lower() == "clip"


def _due(stub: dict, last_checked: str | None, now: datetime) -> bool:
    """Whether a clip without official text should be checked now."""
    if not last_checked:
        return True
    if now - datetime.fromisoformat(last_checked) < RECHECK_INTERVAL:
        return False
    sitting = stub.get("日期")
    if sitting and (now.date() - date.fromisoformat(sitting)).days > MAX_AGE_DAYS:
        return False
    return True


def fetch_records_for_session(session: str, config: Config, client: LYApiClient,
                              checks: dict[str, str], *, now: datetime,
                              force: bool = False) -> bool:
    """Fetch official text for one session's clips.

    ``checks`` (IVOD_ID → last check time) is updated in place. Returns True
    if any clip gained or changed its official text.
    """
    ivods_file = config.file(session, "ivods")
    if not ivods_file.exists():
        return False
    clips = [iv for iv in json.loads(ivods_file.read_text()).get("ivods") or []
             if _is_clip(iv) and iv.get("IVOD_ID")]

    out = config.file(session, "records", create=True)
    records: dict[str, str] = {}
    if out.exists():
        records = dict(json.loads(out.read_text()).get("records") or {})

    changed: list[str] = []
    for stub in clips:
        key = str(stub["IVOD_ID"])
        if not force and (key in records or not _due(stub, checks.get(key), now)):
            continue
        try:
            html = client.get_text(SPEECH_RECORD_URL.format(ivod_id=key)).strip()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[{session}] official text for IVOD {key} failed: "
                           f"{type(e).__name__}: {e}; will retry next run")
            continue
        checks[key] = now.isoformat(timespec="seconds")
        if html and records.get(key) != html:
            records[key] = html
            changed.append(key)

    if not changed:
        return False
    doc = {
        "meta": {
            "session": session,
            "source": SPEECH_RECORD_URL,
            "updatedAt": now.isoformat(timespec="seconds"),
        },
        "records": {k: records[k] for k in sorted(records, key=int)},
    }
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] official text for {len(changed)} clip(s); "
                f"{len(records)}/{len(clips)} clips have it → {out.name}")
    return True


def _load_checks(path: Path) -> dict[str, str]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def note_upgrade(config: Config, session: str) -> None:
    """Record a published session that needs a --rebuild pass."""
    path = config.dir("cache", create=True) / OFFICIAL_TEXT_UPGRADES_FILE
    pending = set(path.read_text().split()) if path.exists() else set()
    if session not in pending:
        pending.add(session)
        path.write_text("\n".join(sorted(pending)) + "\n")
        logger.info(f"[{session}] already published; queued for a --rebuild pass "
                    f"to add the official text ({path.name})")


def download_records(config: Config, args) -> None:
    """Workflow hook: fetch official per-clip text for every in-scope session."""
    client = LYApiClient(
        retry_count=getattr(args, "retry_count", 5) or 5,
        retry_delay_max=getattr(args, "retry_delay_max", 10.0),
        legacy_tls=True,
    )
    checks_path = config.dir("cache", create=True) / CHECKS_FILE
    checks = _load_checks(checks_path)
    now = datetime.now(timezone.utc)
    for session in sessions_in_scope(config, args):
        gained = fetch_records_for_session(session, config, client, checks,
                                           now=now, force=args.force)
        checks_path.write_text(json.dumps(checks, indent=1, sort_keys=True))
        if gained and config.file(session, "processed").exists():
            note_upgrade(config, session)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key (e.g. 1105011)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = Config(args.data_dir)
    args.period = None
    args.limit_session = f"^{args.session}$"
    download_records(config, args)


if __name__ == "__main__":
    main()
