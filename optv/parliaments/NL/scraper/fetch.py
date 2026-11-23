"""Fetch the originals of NL: the report of a sitting and its Debat Direct records.

Per sitting three files are written:

- ``original/proceedings/{session}-vergadering.json``: the sitting's record from the House's open data API with its report versions, and which version was fetched.
- ``original/proceedings/{session}-verslag.xml``: that version of the report. The corrected final report is taken when it exists, otherwise the newest interim version.
- ``original/media/{session}-debatdirect.json``: the Debat Direct agenda of the plenary hall for the sitting's day, and the record of each debate the report refers to.

Which sittings: the ones named with ``--session`` (repeatable), otherwise every plenary sitting of the period, which costs one listing request per session year. ``--limit-session`` confines either selection before anything is fetched for a sitting. Without both, a run fetches every sitting of the period that is missing or whose report has a newer version.

A sitting whose stored report is the final one is complete and is skipped without a request. ``--force`` fetches it again.

A sitting's text changes when a newer report version replaces the one fetched before, at the latest when the corrected final report replaces the interim one. Such a sitting is listed in ``cache/nl-report-upgrades.txt``; the workflow re-runs the listed sittings with rebuild semantics (``rebuild_queue``), because the publish guard refuses a changed text otherwise.

Run on its own: ``python -m optv.parliaments.NL.scraper.fetch <data_dir> --session 2026-005``.
"""

import argparse
import datetime
import json
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from optv.parliaments import load_manifest
from optv.shared.cli import load_config, setup_logging
from optv.shared.http import PoliteHTTP

from ..common import in_scope, parse_session_key, session_key, session_year_label, session_years

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

ODATA = "https://gegevensmagazijn.tweedekamer.nl/OData/v5"
DEBATDIRECT_API = "https://cdn.debatdirect.tweedekamer.nl/api"
PLENARY_HALL = "plenaire-zaal"
# Sittings whose report was replaced by a newer version, for the workflow's rebuild pass.
UPGRADES_FILE = "nl-report-upgrades.txt"

# The report versions of a sitting, as the listing expands them.
_REPORT_FIELDS = "Id,Soort,Status,ContentType,ContentLength,GewijzigdOp"
_EXPAND_REPORTS = f"Verslag($filter=Verwijderd eq false;$select={_REPORT_FIELDS})"


class _Source:
    """One host, with its request spacing and a count of the requests sent."""

    def __init__(self, name: str, retry_count: int):
        self.name = name
        self.retry_count = retry_count
        self.requests = 0
        self._http = PoliteHTTP(polite_delay=1.0)

    def get(self, url: str) -> bytes:
        self.requests += 1
        logger.debug(f"GET {url}")
        return self._http.fetch(url, retry_count=self.retry_count)

    def get_json(self, url: str):
        return json.loads(self.get(url).decode("utf-8-sig"))


def _odata_url(entity: str, **options: str) -> str:
    query = "&".join(f"${name}={quote(value, safe='(),;=$')}" for name, value in options.items())
    return f"{ODATA}/{entity}?{query}"


def _odata_list(source: _Source, url: str) -> list[dict]:
    """All records of a listing, following the server's paging."""
    records: list[dict] = []
    while url:
        page = source.get_json(url)
        records.extend(page.get("value") or [])
        url = page.get("@odata.nextLink")
    return records


def _sitting_key(record: dict) -> Optional[str]:
    """The session key of a sitting record, ``None`` when it carries no usable number."""
    label = record.get("Vergaderjaar") or ""
    number = record.get("Nummer")
    if not re.fullmatch(r"\d{4}-\d{4}", label) or not number:
        return None
    return session_key(int(label[:4]), number)


def list_sittings(source: _Source, year: int, number: Optional[int] = None) -> dict[str, dict]:
    """The plenary sittings of a session year by session key, or the one with ``number``."""
    condition = f"Soort eq 'Plenair' and Verwijderd eq false and Vergaderjaar eq '{session_year_label(year)}'"
    if number is not None:
        condition += f" and Nummer eq {int(number)}"
    records = _odata_list(source, _odata_url(
        "Vergadering", filter=condition, orderby="Nummer", expand=_EXPAND_REPORTS))
    return {key: r for r in records if (key := _sitting_key(r))}


def choose_report(versions: list[dict]) -> Optional[dict]:
    """The report version to use: the newest final one, else the newest interim one."""
    usable = [v for v in versions or [] if "xml" in (v.get("ContentType") or "")]
    if not usable:
        return None
    final = [v for v in usable if v.get("Soort") == "Eindpublicatie"]
    return max(final or usable, key=lambda v: v.get("GewijzigdOp") or "")


def is_final(report: Optional[dict]) -> bool:
    return bool(report) and report.get("Soort") == "Eindpublicatie"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def report_activities(xml: bytes) -> list[tuple[str, str]]:
    """``(object id, date)`` of each agenda item in a report."""
    activities = []
    for el in ET.fromstring(xml).iter():
        if _local(el.tag) != "activiteit" or not el.get("objectid"):
            continue
        start = next((c.text for c in el if _local(c.tag) == "aanvangstijd" and c.text), "")
        activities.append((el.get("objectid"), start[:10]))
    return activities


def fetch_debates(source: _Source, activities: list[tuple[str, str]], sitting_date: str) -> dict:
    """The plenary hall's agenda for each day of the sitting, and the record of each debate the report names."""
    days: dict[str, list[dict]] = {}
    listed: dict[str, str] = {}          # debate id -> the day it is listed under
    for day in sorted({sitting_date} | {date for _, date in activities if date}):
        agenda = source.get_json(f"{DEBATDIRECT_API}/agenda/{day}")
        days[day] = [d for d in agenda.get("debates") or [] if d.get("locationId") == PLENARY_HALL]
        for debate in days[day]:
            listed.setdefault(debate["id"], day)
    debates = {}
    for activity_id, _ in activities:
        if activity_id in listed and activity_id not in debates:
            debates[activity_id] = source.get_json(
                f"{DEBATDIRECT_API}/agenda/{listed[activity_id]}/debates/{activity_id}")
    return {"days": days, "debates": debates}


def _stored_report(config, session: str) -> Optional[dict]:
    path = config.raw_sitting(session)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8")).get("report")


def _complete(config, session: str) -> bool:
    return config.raw_report(session).exists() and config.raw_debates(session).exists()


def queue_report_upgrade(config, session: str) -> None:
    """List a sitting whose report was replaced in the upgrade queue, once."""
    queue = config.dir("cache", create=True) / UPGRADES_FILE
    queued = set(queue.read_text().split()) if queue.exists() else set()
    if session not in queued:
        with queue.open("a") as f:
            f.write(f"{session}\n")


def fetch_sitting(config, session: str, record: dict, odata: _Source, debatdirect: _Source,
                  *, force: bool = False) -> bool:
    """Fetch what is missing or outdated for one sitting. Returns whether anything was written."""
    report = choose_report(record.get("Verslag"))
    if report is None:
        logger.info(f"[{session}] no report published yet, skipped")
        return False
    stored = _stored_report(config, session)
    if not force and stored and stored.get("Id") == report["Id"] and _complete(config, session):
        logger.debug(f"[{session}] report {report['Id']} already fetched")
        return False

    xml = odata.get(f"{ODATA}/Verslag({report['Id']})/resource")
    activities = report_activities(xml)
    sitting_date = (record.get("Datum") or "")[:10]
    debates = fetch_debates(debatdirect, activities, sitting_date)

    for directory in ("proceedings", "media"):
        config.dir(directory, create=True)
    config.raw_report(session).write_bytes(xml)
    config.raw_debates(session).write_text(json.dumps(
        {"session": session, "reportId": report["Id"], **debates},
        indent=2, ensure_ascii=False), encoding="utf-8")
    # Written last: it is the marker that the other two files belong to this report version.
    config.raw_sitting(session).write_text(json.dumps(
        {"session": session, "report": report, "vergadering": record},
        indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"[{session}] fetched {report['Soort']} report ({len(xml)} bytes), "
                f"{len(debates['debates'])} of {len(activities)} agenda items on Debat Direct")
    if stored and stored.get("Id") != report["Id"]:
        queue_report_upgrade(config, session)
        logger.info(f"[{session}] the report replaces an earlier version, queued in {UPGRADES_FILE} for a rebuild")
    return True


def _current_session_year(today: datetime.date) -> int:
    """The session year running on ``today``; it starts on the third Tuesday of September."""
    return today.year if today.month >= 9 else today.year - 1


def fetch_originals(config, args) -> None:
    if args.period is None:
        args.period = max(load_manifest(PARLIAMENT_ID)["periods"])
    retry_count = getattr(args, "retry_count", None) or 10
    force = bool(getattr(args, "force", False))
    odata = _Source("open data API", retry_count)
    debatdirect = _Source("Debat Direct", retry_count)

    named = list(getattr(args, "nl_session", None) or [])
    sittings: dict[str, dict] = {}
    if named:
        for session in named:
            year, number = parse_session_key(session)
            if not in_scope(args, session):
                logger.info(f"[{session}] outside period {args.period} or --limit-session, skipped")
            elif not force and is_final(_stored_report(config, session)) and _complete(config, session):
                logger.debug(f"[{session}] final report already fetched")
            else:
                record = list_sittings(odata, year, number).get(session)
                if record is None:
                    logger.warning(f"[{session}] the open data API lists no such plenary sitting")
                else:
                    sittings[session] = record
    else:
        today = datetime.date.today()
        for year in session_years(args.period, _current_session_year(today)):
            for session, record in list_sittings(odata, year).items():
                if in_scope(args, session):
                    sittings[session] = record

    written = 0
    for session in sorted(sittings):
        written += fetch_sitting(config, session, sittings[session], odata, debatdirect, force=force)
    logger.info(f"NL fetch: {written} of {len(sittings)} sitting(s) written; "
                f"{odata.requests} request(s) to the {odata.name}, "
                f"{debatdirect.requests} to {debatdirect.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=None)
    parser.add_argument("--session", dest="nl_session", action="append", default=[],
                        help="session key to fetch, e.g. 2026-005 (repeatable)")
    parser.add_argument("--limit-session", default="", help="regex of the session keys to fetch")
    parser.add_argument("--force", action="store_true", help="fetch again what is already there")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    fetch_originals(load_config(PARLIAMENT_ID, args.data_dir), args)


if __name__ == "__main__":
    main()
