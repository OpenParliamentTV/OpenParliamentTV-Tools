#! /usr/bin/env python3
"""Fetch DE-SH Plenarprotokoll PDFs into ``original/proceedings/{sid}.pdf``.

Schleswig-Holsteinischer Landtag — wahl{wp}/.../{wp}-{nth:03d}_{mm}-{yy}.pdf (verified, period 20).
Sessions whose ``url_for`` returns ``None`` are logged; parse/merge run on any
PDF dropped manually into ``original/proceedings/``.

Session ids and dates come from the archive descriptor ``fetch_archive`` writes
in the same download stage (``metadata/archive-wp{N}.json``: the m7k sitting
days and the date → Sitzung map); the ``-media.json`` files only follow in the
parse stage, so a new sitting's protocol is fetched in the run that fetches its
video.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from optv.shared.pdf2tei.fetch import run_template_fetch, session_wp_nth


def url_for(session_id: str, date: Optional[str]) -> Optional[str]:
    if not date:
        return None
    y, m, d = date.split("-")
    wp, nth = session_wp_nth(session_id)
    return (f"http://www.landtag.ltsh.de/export/sites/ltsh/infothek/wahl{wp}/plenum/"
            f"plenprot/{y}/{wp}-{nth:03d}_{m}-{y[2:]}.pdf")


def archive_sessions(config) -> list[tuple[str, Optional[str]]]:
    """``(session_id, ISO date)`` of every m7k sitting day with a Sitzung number."""
    out = []
    for path in sorted(config.dir('nel_data').glob('archive-wp*.json')):
        try:
            archive = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        days = set()
        for tagung in archive.get("tagungen") or []:
            for _sid, label in tagung.get("sitzungen") or []:
                try:
                    days.add(datetime.strptime(label.strip(), "%d.%m.%Y").date().isoformat())
                except ValueError:
                    continue
        wp = int(archive.get("wp") or 0)
        for iso, nr in sorted((archive.get("sitzung_by_date") or {}).items()):
            if iso in days:
                out.append((f"{wp:02d}{int(nr):03d}", iso))
    return out


def fetch_proceedings(config, args) -> None:
    run_template_fetch(config, url_for, force=getattr(args, "force", False),
                       retry_count=getattr(args, "retry_count", 3),
                       session_filter=getattr(args, "limit_session", None),
                       sessions=archive_sessions(config))
