#! /usr/bin/env python3
"""Fetch DE-NW Plenarprotokolle (HTML version) into ``original/proceedings/{sid}.html``.

Each protocol is published as a Word-export HTML page at
``/portal/WWW/dokumentenarchiv/Dokument/MMP{wp}-{nth}.html`` a few days after the
sitting. A 404 means the protocol is not published yet; the session is tried
again on the next run. Sessions come from the media files already on disk
(proceedings download runs right after media in the workflow).
"""
from __future__ import annotations

import logging
import re

from optv.shared.pdf2tei.fetch import session_wp_nth

from .common import BASE, fetch

logger = logging.getLogger(__name__)


def protocol_url(session_id: str) -> str:
    wp, nth = session_wp_nth(session_id)
    return f"{BASE}/portal/WWW/dokumentenarchiv/Dokument/MMP{wp}-{nth}.html"


def fetch_proceedings(config, args) -> None:
    force = getattr(args, "force", False)
    retry_count = getattr(args, "retry_count", 3)
    session_filter = getattr(args, "limit_session", None)
    proc_dir = config.dir('proceedings', create=True)
    pending: list[str] = []
    for sid in config.sessions():
        if session_filter and not re.match(session_filter, sid):
            continue
        dest = proc_dir / f"{sid}.html"
        if dest.exists() and dest.stat().st_size > 0 and not force:
            continue
        url = protocol_url(sid)
        try:
            body = fetch(url, retry_count=retry_count)
        except RuntimeError as ex:
            logger.warning(f"{sid}: protocol fetch failed ({ex})")
            continue
        if body is None:
            pending.append(sid)
            continue
        dest.write_bytes(body)
        logger.info(f"downloaded {dest.name} ({len(body)} bytes)")
    if pending:
        logger.info(f"protocol not published yet for {len(pending)} session(s): "
                    f"{', '.join(pending[:6])}{'…' if len(pending) > 6 else ''}")
