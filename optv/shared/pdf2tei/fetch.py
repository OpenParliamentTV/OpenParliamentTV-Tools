"""Download Plenarprotokoll PDFs for the PDF tier.

A small, reusable helper so each parliament's ``scraper/fetch_proceedings.py``
only has to supply how a session id maps to a URL (``url_for(session_id, date)``;
return ``None`` when the scheme is unknown). The PDF is written to
``original/proceedings/{sid}.pdf``, which the proceedings parser then reads.

URL schemes were read off ``_planning/protocols.csv``; several states follow a
clean template (BW/NW/SH/NI — see :func:`session_wp_nth`), while BY/SN/HH publish
via opaque viewer/document ids and are left for manual drop.
"""
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Callable, Optional

import requests

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "OpenParliamentTV-Tools/1.0 (+https://openparliament.tv)"}


def session_wp_nth(session_id: str) -> tuple[int, int]:
    """Split a ``{wp:02d}{nth:03d}`` session id (e.g. ``17118``) into (wp, nth)."""
    return int(session_id[:2]), int(session_id[2:])


def download_pdf(url: str, dest: Path, *, force: bool = False,
                 retry_count: int = 3, retry_delay: float = 2.0,
                 timeout: float = 60.0) -> Optional[Path]:
    """Download ``url`` to ``dest`` (a ``…/{sid}.pdf`` path). Returns the path on
    success, ``None`` on failure. Skips an existing non-empty file unless
    ``force``. Verifies the response really is a PDF (some sites answer 200 with
    an HTML error page)."""
    dest = Path(dest)
    if dest.exists() and dest.stat().st_size > 0 and not force:
        logger.debug(f"{dest.name} already present — skipping")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, retry_count + 1):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout)
            ctype = r.headers.get("content-type", "")
            if r.status_code == 200 and (r.content[:5] == b"%PDF-" or "pdf" in ctype):
                dest.write_bytes(r.content)
                logger.info(f"downloaded {dest.name} ({len(r.content)} bytes)")
                return dest
            logger.warning(f"{url} -> HTTP {r.status_code} ({ctype}); not a PDF")
            return None
        except requests.RequestException as e:
            logger.warning(f"attempt {attempt}/{retry_count} for {url} failed: {e}")
            if attempt < retry_count:
                time.sleep(retry_delay)
    return None


def run_template_fetch(config, url_for: Callable[[str, Optional[str]], Optional[str]],
                       *, force: bool = False, retry_count: int = 3,
                       session_filter: Optional[str] = None) -> None:
    """Fetch a protocol PDF for every discovered session.

    Sessions come from the media files already on disk (proceedings download runs
    right after media in the workflow), so ``url_for`` gets the session id and the
    sitting date (from the media meta, for the date-bearing schemes). Sessions
    whose ``url_for`` returns ``None`` are logged as needing a manual PDF drop.
    """
    proc_dir = config.dir('proceedings', create=True)
    missing: list[str] = []
    for sid in config.sessions():
        if session_filter and not re.match(session_filter, sid):
            continue
        dest = proc_dir / f"{sid}.pdf"
        if dest.exists() and dest.stat().st_size > 0 and not force:
            continue
        date = None
        mf = config.file(sid, 'media')
        if mf.exists():
            try:
                date = (json.loads(mf.read_text()).get('meta') or {}).get('date')
            except (json.JSONDecodeError, OSError):
                pass
        url = url_for(sid, date)
        if not url:
            missing.append(sid)
            continue
        download_pdf(url, dest, force=force, retry_count=retry_count)
    if missing:
        logger.warning(f"no protocol-PDF URL scheme for {len(missing)} session(s) "
                       f"({', '.join(missing[:6])}{'…' if len(missing) > 6 else ''}); "
                       f"drop {{sid}}.pdf into {proc_dir} to parse them")
