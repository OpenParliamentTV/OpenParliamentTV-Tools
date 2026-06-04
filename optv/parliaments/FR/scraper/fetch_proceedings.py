#! /usr/bin/env python3
"""Download Assemblée nationale Syceron comptes rendus (proceedings spine).

Two acquisition modes:

* **Targeted** (``--fr-session 2026O1N232``, repeatable): fetch each séance's
  compte rendu directly from the per-document open-data endpoint
  ``/dyn/opendata/{uid}.xml`` — cheap, incremental.
* **Bulk** (default): download the legislature's
  ``syceronbrut/syseron.xml.zip`` archive once and extract every
  ``xml/compteRendu/CRS*.xml`` into ``original/proceedings/{session}-cr.xml``.

Both write the modern namespaced Syceron compte-rendu XML (one file per
séance), which carries per-speech ``stime`` video offsets, ``id_acteur``
speaker ids and the agenda ``<point>`` structure. Idempotent: existing files
are kept unless ``--force``.
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import zipfile
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.FR.scraper"

from optv.parliaments.FR.common import Config, session_to_uid, uid_to_session
from optv.parliaments.FR.scraper.common import http_get

logger = logging.getLogger(__name__)

BULK_URL = ("https://data.assemblee-nationale.fr/static/openData/repository/"
            "{leg}/vp/syceronbrut/syseron.xml.zip")
PER_SEANCE_URL = "https://www.assemblee-nationale.fr/dyn/opendata/{uid}.xml"

_ZIP_MEMBER_RE = re.compile(r"compteRendu/(CRSANR5L\d+S\d{4}[OE]\d+N\d+)\.xml$")


def _matches_limit(session: str, args) -> bool:
    limit = getattr(args, "limit_session", "") or ""
    if not limit:
        return True
    try:
        return bool(re.match(limit, session))
    except re.error:
        return limit == session


def _fetch_one(config: Config, session: str, args) -> bool:
    """Fetch a single séance compte rendu directly. Returns True if written."""
    out = config.raw_cr(session)
    if out.exists() and not args.force:
        logger.debug(f"[{session}] compte rendu cached")
        return False
    uid = session_to_uid(session, legislature=args.period)
    url = PER_SEANCE_URL.format(uid=uid)
    logger.info(f"[{session}] fetching {uid}")
    xml = http_get(url, retry_count=args.retry_count,
                   retry_delay_max=args.retry_delay_max)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(xml, encoding="utf-8")
    return True


def _download_bulk(config: Config, args) -> int:
    """Download the legislature archive and extract every séance compte rendu."""
    url = BULK_URL.format(leg=args.period)
    logger.info(f"downloading Syceron bulk archive {url}")
    blob = http_get(url, timeout=300, retry_count=args.retry_count,
                    retry_delay_max=args.retry_delay_max, binary=True)
    written = 0
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            m = _ZIP_MEMBER_RE.search(name)
            if not m:
                continue
            session = uid_to_session(m.group(1))
            if not _matches_limit(session, args):
                continue
            out = config.raw_cr(session)
            if out.exists() and not args.force:
                continue
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(zf.read(name))
            written += 1
    logger.info(f"extracted {written} compte-rendu file(s) into "
                f"{config.dir('proceedings')}")
    return written


def download_proceedings(config: Config, args) -> None:
    """Workflow hook: download proceedings for the requested séances."""
    fr_sessions = getattr(args, "fr_session", None) or []
    if fr_sessions:
        for session in fr_sessions:
            try:
                _fetch_one(config, session, args)
            except Exception as e:  # noqa: BLE001
                logger.error(f"[{session}] proceedings download failed: {e}")
        return
    _download_bulk(config, args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--fr-session", action="append", default=[])
    parser.add_argument("--limit-session", default="")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    download_proceedings(config, args)


if __name__ == "__main__":
    main()
