#! /usr/bin/env python3
"""Fetch DE-NW Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

Landtag Nordrhein-Westfalen — protocol PDFs follow MMP{wp}-{nth}.pdf (verified against protocols.csv).
Sessions whose ``url_for`` returns ``None`` are logged; parse/merge run on any
PDF dropped manually into ``original/proceedings/``.
"""
from __future__ import annotations

from typing import Optional

from optv.shared.pdf2tei.fetch import run_template_fetch, session_wp_nth


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    wp, nth = session_wp_nth(session_id)
    return (f"https://www.landtag.nrw.de/Dokumentenservice/portal/WWW/"
            f"dokumentenarchiv/Dokument/MMP{wp}-{nth}.pdf")


def fetch_proceedings(config, args) -> None:
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=getattr(args, "retry_count", 3),
                       session_filter=getattr(args, "limit_session", None))
