#! /usr/bin/env python3
"""Fetch DE-SN Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

Sächsischer Landtag — protocols are behind opaque edas document ids; no stable template.
Sessions whose ``url_for`` returns ``None`` are logged; parse/merge run on any
PDF dropped manually into ``original/proceedings/``.
"""
from __future__ import annotations

from typing import Optional

from optv.shared.pdf2tei.fetch import run_template_fetch, session_wp_nth


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    return None  # SN protocols are served behind opaque edas datei_id links


def fetch_proceedings(config, args) -> None:
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=getattr(args, "retry_count", 3),
                       session_filter=getattr(args, "limit_session", None))
