#! /usr/bin/env python3
"""Fetch DE-SN Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

Sächsischer Landtag — the edas document system has no templated PDF URL (the
download is keyed by an opaque ``datei_id``), but its Angular front-end is backed
by a plain JSON REST API we can query:

1. ``GET /redas/querygetall?wahlperiode={wp}&dokArt=PlPr&dokNr={nth}`` → the
   Plenarprotokoll documents for that sitting; the *main* protocol is the record
   whose title is ``"{nth}. Sitzung …"`` (the others are per-debate excerpts).
2. ``GET /redas/dokument/{id}`` → that document's ``dateien`` list; the PDF entry
   carries the ``datei_id``.
3. ``…/redas/download/file?datei_id={id}`` streams the PDF.

So ``url_for`` resolves the session id → download URL via two API calls and
returns ``None`` (graceful manual-drop fallback) if anything is missing.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

import requests

from optv.shared.pdf2tei.fetch import run_template_fetch, session_wp_nth

logger = logging.getLogger(__name__)

_EDAS = "https://edas.landtag.sachsen.de/redas"
_HEADERS = {
    "User-Agent": "OpenParliamentTV-Tools/1.0 (+https://openparliament.tv)",
    "Accept": "application/json",
}


def _resolve_pdf_url(wp: int, nth: int) -> Optional[str]:
    try:
        r = requests.get(f"{_EDAS}/querygetall", headers=_HEADERS, timeout=30,
                         params={"wahlperiode": wp, "dokArt": "PlPr", "dokNr": nth,
                                 "pageNumber": 0, "pageSize": 50})
        r.raise_for_status()
        records = r.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"edas query failed for {wp}/{nth}: {e}")
        return None
    # The full protocol is titled "{nth}. Sitzung …"; the per-topic excerpts that
    # share the same dokNr carry a debate title instead.
    main = next((x for x in records
                 if re.match(rf"^\s*{nth}\.\s*Sitzung\b", x.get("titel") or "")), None)
    if not main:
        logger.warning(f"no main Plenarprotokoll record for {wp}/{nth}")
        return None
    try:
        d = requests.get(f"{_EDAS}/dokument/{main['id']}", headers=_HEADERS, timeout=30)
        d.raise_for_status()
        files = d.json().get("dateien", [])
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"edas detail failed for doc {main.get('id')}: {e}")
        return None
    pdf = next((f for f in files
                if f.get("format") == "application/pdf" and f.get("id")), None)
    if not pdf:
        logger.warning(f"no PDF file on edas doc {main.get('id')} ({wp}/{nth})")
        return None
    return f"{_EDAS}/download/file?datei_id={pdf['id']}"


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    wp, nth = session_wp_nth(session_id)
    return _resolve_pdf_url(wp, nth)


def fetch_proceedings(config, args) -> None:
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=getattr(args, "retry_count", 3),
                       session_filter=getattr(args, "limit_session", None))
