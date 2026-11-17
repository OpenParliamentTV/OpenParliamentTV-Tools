#! /usr/bin/env python3
"""Fetch DE-HH Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

Hamburgische Bürgerschaft — ParlDok exposes each Plenarprotokoll under a
predictable, document-id-free resolver: ``parldok/dokument/{wp}/art/
Plenarprotokoll/num/{nth}`` streams the PDF directly (``Content-Type:
application/pdf``). So a session id is all we need — no document search, no date.
The protocol appears months after the sitting; until then the resolver yields no
PDF and the attempt is repeated on the next run.

A protocol that arrives for a sitting already published with subtitle text
(``debug.textSource == "subtitles"``) is appended to
``cache/de-hh-protocol-upgrades.txt``: replacing the published transcript needs a
``--rebuild`` pass, which the ``update`` wrapper runs for the listed sittings.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from optv.shared.pdf2tei.fetch import run_template_fetch, session_wp_nth

logger = logging.getLogger(__name__)

_BASE = "https://www.buergerschaft-hh.de/parldok/dokument"
UPGRADES_FILE = "de-hh-protocol-upgrades.txt"
# A protocol appears months after its sitting and a missing one is retried on
# every run, so a few attempts per run are enough (the workflow's
# --retry-count is sized for the mediathek).
PDF_RETRY_COUNT = 3


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    wp, nth = session_wp_nth(session_id)  # "23018" -> (23, 18); num is the bare int
    return f"{_BASE}/{wp}/art/Plenarprotokoll/num/{nth}"


def _pdfs(proc_dir: Path) -> set[str]:
    return {p.stem for p in proc_dir.glob("*.pdf") if p.stat().st_size > 0}


def _published_with_subtitles(config, session: str) -> bool:
    processed = config.file(session, "processed")
    if not processed.exists():
        return False
    try:
        data = json.loads(processed.read_text()).get("data") or []
    except (json.JSONDecodeError, OSError):
        return False
    return any((s.get("debug") or {}).get("textSource") == "subtitles" for s in data)


def queue_protocol_upgrades(config, sessions: list[str]) -> list[str]:
    """Append the sittings published with subtitle text to the upgrade queue."""
    upgrades = [s for s in sessions if _published_with_subtitles(config, s)]
    if not upgrades:
        return []
    queue = config.dir("cache", create=True) / UPGRADES_FILE
    queued = set(queue.read_text().split()) if queue.exists() else set()
    with queue.open("a") as f:
        for s in upgrades:
            if s not in queued:
                f.write(f"{s}\n")
    logger.info(f"protocol arrived for subtitle-text sitting(s) {', '.join(upgrades)} "
                f"— queued in {queue.name} for a --rebuild pass")
    return upgrades


def fetch_proceedings(config, args) -> None:
    proc_dir = config.dir('proceedings', create=True)
    before = _pdfs(proc_dir)
    retry_count = min(getattr(args, "retry_count", PDF_RETRY_COUNT), PDF_RETRY_COUNT)
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=retry_count,
                       session_filter=getattr(args, "limit_session", None))
    queue_protocol_upgrades(config, sorted(_pdfs(proc_dir) - before))
