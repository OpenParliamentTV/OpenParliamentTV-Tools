#! /usr/bin/env python3
"""Fetch DE-HH Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

Hamburgische Bürgerschaft — ParlDok exposes each Plenarprotokoll under a
predictable, document-id-free resolver: ``parldok/dokument/{wp}/art/
Plenarprotokoll/num/{nth}`` streams the PDF directly (``Content-Type:
application/pdf``). So a session id is all we need — no document search, no date.
"""
from __future__ import annotations

from typing import Optional

from optv.shared.pdf2tei.fetch import run_template_fetch, session_wp_nth

_BASE = "https://www.buergerschaft-hh.de/parldok/dokument"


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    wp, nth = session_wp_nth(session_id)  # "23018" -> (23, 18); num is the bare int
    return f"{_BASE}/{wp}/art/Plenarprotokoll/num/{nth}"


def fetch_proceedings(config, args) -> None:
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=getattr(args, "retry_count", 3),
                       session_filter=getattr(args, "limit_session", None))
