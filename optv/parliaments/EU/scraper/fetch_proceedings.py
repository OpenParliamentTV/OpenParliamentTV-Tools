#! /usr/bin/env python3
"""Fetch EU plenary verbatim proceedings via the EP Open Data Portal API.

Replaces the AWS-WAF-bypassing CRE HTML scraper. For each plenary day we save
two JSON-LD blobs and a small marker:

    <data_dir>/original/proceedings/
        raw-{YYYYMMDD}-cre.json                    # marker JSON
        {YYYYMMDD}/speeches.jsonld                 # all speeches with inline xml_fragment
        {YYYYMMDD}/meeting.jsonld                  # meeting envelope + agenda items

Date sources, in priority order:
  1. ``args.eu_date``  (CLI list, repeatable)
  2. ``args.year``     (CLI; enumerates all plenary sittings for the year)
  3. ``args.limit_session`` if it looks like YYYY-MM-DD or YYYYMMDD
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from .ep_api import EPApiClient

logger = logging.getLogger(__name__)


def _normalize_date(date_str: str) -> str:
    s = date_str.replace("-", "")
    if not re.fullmatch(r"\d{8}", s):
        raise ValueError(f"Bad date {date_str!r} (expected YYYY-MM-DD or YYYYMMDD)")
    return s


def _meeting_id_for_date(date_yyyymmdd: str) -> str:
    d = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
    return f"MTG-PL-{d}"


def fetch_cre_day(
    date_yyyymmdd: str,
    out_dir: Path,
    client: EPApiClient,
    *,
    force: bool = False,
) -> tuple[Path, Path] | None:
    """Download speeches + meeting envelope for one plenary day.

    Returns ``(speeches_path, meeting_path)`` or None on failure.
    """
    day_dir = out_dir / date_yyyymmdd
    speeches_path = day_dir / "speeches.jsonld"
    meeting_path = day_dir / "meeting.jsonld"

    if speeches_path.exists() and meeting_path.exists() and not force:
        logger.info(f"[{date_yyyymmdd}] cached → {speeches_path.name}, {meeting_path.name}")
        return speeches_path, meeting_path

    meeting_id = _meeting_id_for_date(date_yyyymmdd)
    try:
        meeting = client.get_meeting(meeting_id)
    except LookupError:
        logger.error(f"[{date_yyyymmdd}] no meeting found for {meeting_id}")
        return None
    except Exception as e:  # noqa: BLE001
        logger.error(f"[{date_yyyymmdd}] meeting fetch failed: {type(e).__name__}: {e}")
        return None

    try:
        agenda_items = client.list_agenda_items(meeting_id)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[{date_yyyymmdd}] agenda-items fetch failed: {e}; continuing without titles")
        agenda_items = {}

    speeches: list[dict] = []
    try:
        for sp in client.iter_day_speeches(date_yyyymmdd):
            speeches.append(sp)
    except Exception as e:  # noqa: BLE001
        logger.error(f"[{date_yyyymmdd}] speech listing failed: {type(e).__name__}: {e}")
        return None

    if not speeches:
        logger.warning(f"[{date_yyyymmdd}] no speeches returned by API")

    day_dir.mkdir(parents=True, exist_ok=True)
    speeches_path.write_text(
        json.dumps({"data": speeches}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    meeting_path.write_text(
        json.dumps({"meeting": meeting, "agenda_items": agenda_items},
                   indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(
        f"[{date_yyyymmdd}] saved {speeches_path.name} ({len(speeches)} speeches)"
        f" + {meeting_path.name} ({len(agenda_items)} agenda items)"
    )
    return speeches_path, meeting_path


def write_raw_marker(
    out_dir: Path,
    date_yyyymmdd: str,
    speeches_path: Path,
    meeting_path: Path,
) -> Path:
    """Write the per-day raw marker JSON used by Config.sessions()."""
    out_dir.mkdir(parents=True, exist_ok=True)
    marker = out_dir / f"raw-{date_yyyymmdd}-cre.json"
    payload = {
        "fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date": f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}",
        "source": "data.europarl.europa.eu/api/v2",
        "speechesPath": str(speeches_path.relative_to(out_dir)),
        "meetingPath": str(meeting_path.relative_to(out_dir)),
        "speechesSize": speeches_path.stat().st_size,
        "meetingSize": meeting_path.stat().st_size,
    }
    marker.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    return marker


def _dates_from_args(args, client: EPApiClient) -> list[str]:
    dates: list[str] = []
    if getattr(args, "eu_date", None):
        dates.extend(_normalize_date(d) for d in args.eu_date)
    elif getattr(args, "year", None):
        try:
            sittings = client.list_plenary_sittings(int(args.year))
        except Exception as e:  # noqa: BLE001
            logger.error(f"plenary enumeration failed for year {args.year}: {e}")
            return []
        for s in sittings:
            d = s.get("activity_date")
            if d:
                dates.append(d.replace("-", ""))
        logger.info(f"--year {args.year} → {len(dates)} plenary sitting(s)")
    elif getattr(args, "limit_session", None):
        try:
            dates.append(_normalize_date(args.limit_session.strip()))
        except ValueError:
            logger.error(
                f"--limit-session {args.limit_session!r} is not a YYYY-MM-DD/YYYYMMDD date."
            )
    if getattr(args, "limit_session", None) and getattr(args, "year", None):
        try:
            pattern = re.compile(args.limit_session)
            dates = [d for d in dates if pattern.match(d)]
        except re.error:
            dates = [d for d in dates if d == args.limit_session]
    return dates


def download_proceedings(config, args) -> None:
    """Workflow hook entry."""
    client = EPApiClient(
        cache_dir=config.dir("cache", create=True) / "ep-api",
    )
    dates = _dates_from_args(args, client)
    if not dates:
        logger.error(
            "No dates to fetch. Pass --eu-date YYYY-MM-DD (repeatable), "
            "--year YYYY, or --limit-session YYYYMMDD."
        )
        return

    proc_dir = config.dir("proceedings", create=True)
    for date in dates:
        result = fetch_cre_day(date, proc_dir, client, force=args.force)
        if result is None:
            continue
        speeches_path, meeting_path = result
        marker = write_raw_marker(proc_dir, date, speeches_path, meeting_path)
        logger.info(f"[{date}] wrote {marker.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path, help="Data directory root")
    parser.add_argument("--eu-date", action="append", default=[],
                        help="Plenary date YYYY-MM-DD or YYYYMMDD (repeatable)")
    parser.add_argument("--year", type=int, default=None,
                        help="Fetch every plenary sitting in this calendar year")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not args.eu_date and not args.year:
        parser.error("at least one --eu-date or --year is required")

    proc_dir = args.data_dir / "original" / "proceedings"
    cache_root = args.data_dir / "cache" / "ep-api"
    client = EPApiClient(cache_dir=cache_root)

    dates: list[str] = []
    if args.eu_date:
        dates = [_normalize_date(d) for d in args.eu_date]
    elif args.year:
        sittings = client.list_plenary_sittings(args.year)
        dates = [s["activity_date"].replace("-", "") for s in sittings if s.get("activity_date")]

    for date in dates:
        result = fetch_cre_day(date, proc_dir, client, force=args.force)
        if result is None:
            sys.exit(1)
        speeches_path, meeting_path = result
        marker = write_raw_marker(proc_dir, date, speeches_path, meeting_path)
        logger.info(f"[{date}] wrote {marker.name}")


if __name__ == "__main__":
    main()
