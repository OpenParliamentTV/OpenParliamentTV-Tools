#! /usr/bin/env python3
"""Download the speech records of FI plenary sittings.

The text comes from the Eduskunta open data API (``api.eduskunta.fi``, CC BY
4.0): ``POST /api/v1/search`` with category ``puheenvuoro`` returns one record
per speech with speaker, agenda item, UTC start/end and the verbatim text.

The text is revised after the sitting: each record carries a status (``tila``)
that starts as ``Käsitelty`` and becomes ``Tarkistettu`` or ``Hyväksytty``
some weeks later. A cached sitting is therefore downloaded again while it still
holds non-final records and is younger than :data:`REFRESH_DAYS`; the file is
rewritten only when the records changed, so later stages re-run only on a real
revision.

Writes ``original/proceedings/{session}-speeches.json`` (the raw records).
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
from pathlib import Path

from optv.parliaments.FI.common import Config, parse_session_str, session_str
from optv.parliaments.FI.scraper.eduskunta_api import fetch_speeches

logger = logging.getLogger(__name__)

# Record states after which the text no longer changes.
FINAL_STATES = frozenset({"Hyväksytty", "Tarkistettu"})
# Re-download a sitting with non-final records for this many days after it.
REFRESH_DAYS = 90


def _sitting_date(records: list[dict]) -> datetime.date | None:
    starts = [r.get("aloitushetki") for r in records if r.get("aloitushetki")]
    if not starts:
        return None
    try:
        return datetime.datetime.fromisoformat(min(starts).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def needs_refresh(records: list[dict], today: datetime.date | None = None) -> bool:
    """True while the sitting has non-final records and is within the refresh window."""
    if all((r.get("tila") or "") in FINAL_STATES for r in records):
        return False
    sitting = _sitting_date(records)
    if sitting is None:
        return False
    today = today or datetime.date.today()
    return (today - sitting).days <= REFRESH_DAYS


def _dump(records: list[dict]) -> str:
    return json.dumps(records, indent=2, ensure_ascii=False) + "\n"


def fetch_proceedings(config: Config, year: int, number: int, *,
                      force: bool = False, retry_count: int = 5,
                      retry_delay_max: float = 10.0) -> Path | None:
    session = session_str(year, number)
    out = config.raw_speeches(session)
    cached = None
    if out.exists():
        cached = json.loads(out.read_text(encoding="utf-8"))
        if not force and not needs_refresh(cached):
            logger.info(f"[{session}] speeches.json exists — skipping (use --force)")
            return out
        if not force:
            logger.info(f"[{session}] speeches.json has non-final text — refreshing")
    records = fetch_speeches(year, number, retry_count=retry_count,
                             retry_delay_max=retry_delay_max)
    if not records:
        logger.warning(f"[{session}] no speech records in the open data API yet — skipping")
        return out if out.exists() else None
    if cached == records:
        logger.info(f"[{session}] speech records unchanged ({len(records)})")
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_dump(records), encoding="utf-8")
    logger.info(f"[{session}] wrote {out.name} ({len(records)} speeches)")
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 2026-058")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    year, number = parse_session_str(args.session)
    config = Config(args.data_dir)
    fetch_proceedings(config, year, number, force=args.force)


if __name__ == "__main__":
    main()
