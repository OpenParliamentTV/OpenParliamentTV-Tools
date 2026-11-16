#! /usr/bin/env python3
"""Fetch DE-BY Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

The Bayerischer Landtag's ``protokolledirektanzeige.xhtml`` viewer looks opaque
but actually streams the PDF directly (``Content-Type: application/pdf``) for a
fully templated query — ``?date={DDMMYY}&sitznr={nth:03d}&wp={wp}``. So a session
id plus its sitting date is all we need; no document-search step. Sessions whose
date is unknown fall back to ``None`` (manual PDF drop into
``original/proceedings/``).

Session ids and dates come from the TOP manifests ``fetch_media`` writes in the
same download stage (the ``-media.json`` files only follow in the parse stage),
so a new sitting's protocol is fetched in the run that fetches its video.
"""
from __future__ import annotations

import json
from typing import Optional

from optv.shared.pdf2tei.fetch import run_template_fetch, session_wp_nth

_BASE = ("https://www.bayern.landtag.de/webangebot3/views/protokolledirektanzeige/"
         "protokolledirektanzeige.xhtml")


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    # date is ISO ``YYYY-MM-DD`` (from the media meta); the viewer wants ``DDMMYY``.
    if not date:
        return None
    parts = date.split("-")
    if len(parts) != 3:
        return None
    y, m, d = parts
    wp, nth = session_wp_nth(session_id)
    return f"{_BASE}?date={d}{m}{y[2:]}&sitznr={nth:03d}&wp={wp}"


def manifest_sessions(config) -> list[tuple[str, Optional[str]]]:
    """``(session_id, ISO date)`` of every session with a TOP manifest."""
    out = []
    for path in sorted(config.dir('media').glob('*-tops.json')):
        try:
            manifest = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if manifest.get("session_id"):
            out.append((manifest["session_id"], manifest.get("date")))
    return out


def fetch_proceedings(config, args) -> None:
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=getattr(args, "retry_count", 3),
                       session_filter=getattr(args, "limit_session", None),
                       sessions=manifest_sessions(config))
