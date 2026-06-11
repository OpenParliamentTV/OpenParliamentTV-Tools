#! /usr/bin/env python3
"""Fetch DE-BY Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

The Bayerischer Landtag publishes protocols through a viewer page
(``protokolledirektanzeige.xhtml?date=…&sitznr=…&wp=…``), not a stable direct
PDF URL, so no template is wired yet — ``url_for`` returns ``None`` and this is a
graceful no-op that logs which sessions still need a PDF. Parse/merge run on any
PDF dropped manually into ``original/proceedings/``.
"""
from __future__ import annotations

from typing import Optional

from optv.shared.pdf2tei.fetch import run_template_fetch


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    return None  # BY protocols are behind an xhtml viewer; no direct PDF scheme


def fetch_proceedings(config, args) -> None:
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=getattr(args, "retry_count", 3),
                       session_filter=getattr(args, "limit_session", None))
