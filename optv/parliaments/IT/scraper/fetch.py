"""Fetch the originals of IT: the WebTV page of a sitting and its stenographic record.

Per sitting three files are written:

- ``original/media/{session}-evento.html``: the sitting's page on the Chamber's WebTV. It lists the interventions with their offsets in the recording, the recording's MP4 parts, and for each intervention the id of the record intervention it starts with.
- ``original/proceedings/{session}-stenografico.xml``: the XML version of the sitting's stenographic record.
- ``original/media/{session}-archivio.json``: the sitting's entry in the WebTV archive (event id, title, start and end) and what was fetched for it. It is written last and marks the other two as belonging together.

Which sittings: the ones named with ``--session`` (repeatable), each searched in the archive service by its number, which takes one request for a number of three or four digits and more for a shorter one, because the search also finds the numbers that contain it. Without ``--session`` the archive of the period is listed, newest first, 20 sittings per request; that listing stops at the first page that holds nothing new, unless ``--limit-session`` or ``--force`` is given, which list the whole period. ``--limit-session`` confines either selection before anything is fetched for a sitting. A sitting then costs two requests, its page and its record.

A sitting whose page links the record and whose record is stored is complete and is skipped without a request. ``--force`` fetches it again. A sitting that is still live is skipped.

Run on its own: ``python -m optv.parliaments.IT.scraper.fetch <data_dir> --session 190731``.
"""

import argparse
import json
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

from optv.parliaments import load_manifest
from optv.shared.cli import load_config, setup_logging
from optv.shared.http import PoliteHTTP

from ..common import in_scope, parse_session_key, session_key

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

WEBTV = "https://webtv.camera.it"
ARCHIVE_SERVICE = f"{WEBTV}/ws/archivio"
EVENT_PAGE = WEBTV + "/evento/{event}"
RECORD_XML = ("https://documenti.camera.it/apps/commonServices/getDocumento.ashx"
              "?sezione=assemblea&tipoDoc=formato_xml&tipologia=stenografico"
              "&idNumero={number:04d}&idLegislatura={period}")

# The archive's section of the assembly's sittings, and the size of its pages.
ASSEMBLY_SECTION = "1"
PAGE_SIZE = 20

_PAGE_SITTING_RE = re.compile(rb"var leg = (\d+);\s*var nume_sed = (\d+);")
_PAGE_LIVE_RE = re.compile(rb"var is_live = 1;")
_PAGE_LINKS_RECORD_RE = re.compile(rb'data-steno_map="tit')


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

    def post(self, url: str, data: dict[str, str]) -> bytes:
        self.requests += 1
        logger.debug(f"POST {url} {data}")
        return self._http.post(url, data, retry_count=self.retry_count)


# --------------------------------------------------------------------------- #
# The WebTV archive
# --------------------------------------------------------------------------- #

def parse_archive(xml: bytes) -> tuple[int, list[dict]]:
    """The number of records a query matches and the sittings on the page that was answered."""
    root = ET.fromstring(xml)
    total = int(root.findtext("tot_records") or 0)
    sittings = []
    for item in root.iterfind("list/item"):
        number = (item.findtext("numero_seduta") or "").strip()
        period = (item.findtext("legislatura") or "").strip()
        event = (item.findtext("id") or "").strip()
        if not (number.isdigit() and period.isdigit() and event.isdigit()):
            continue
        sittings.append({
            "event": int(event),
            "legislature": int(period),
            "sitting": int(number),
            "title": " ".join((item.findtext("titolo") or "").split()),
            "dateStart": (item.findtext("dataora_evento") or "").strip(),
            "dateEnd": (item.findtext("dataora_fine") or "").strip(),
        })
    return total, sittings


def _archive_page(source: _Source, period: int, page: int, keyword: Optional[str] = None) -> tuple[int, list[dict]]:
    data = {
        f"filter_label_sez_{ASSEMBLY_SECTION}": ASSEMBLY_SECTION,
        f"filter_label_leg_{period}": str(period),
    }
    if keyword:
        data["filter_label_txt_1"] = keyword
    data["page"] = str(page)
    return parse_archive(source.post(ARCHIVE_SERVICE, data))


def find_sitting(source: _Source, period: int, number: int) -> Optional[dict]:
    """The archive entry of one sitting, searched by its number in the title."""
    page = 1
    while True:
        total, sittings = _archive_page(source, period, page, keyword=f"Seduta {number}")
        matches = [s for s in sittings if s["sitting"] == number and s["legislature"] == period]
        if matches:
            if len(matches) > 1:
                logger.warning(f"The archive lists {len(matches)} events for sitting {number}, taking {matches[0]['event']}")
            return matches[0]
        if page * PAGE_SIZE >= total:
            return None
        page += 1


def list_sittings(source: _Source, period: int, *, known=lambda session: False) -> dict[str, dict]:
    """The sittings of a legislature by session key, newest first. The listing stops after the first page on which ``known`` holds for every sitting."""
    listed: dict[str, dict] = {}
    page = 1
    while True:
        total, sittings = _archive_page(source, period, page)
        for sitting in sittings:
            if sitting["legislature"] == period:
                listed.setdefault(session_key(period, sitting["sitting"]), sitting)
        if page * PAGE_SIZE >= total:
            break
        if sittings and all(known(session_key(period, s["sitting"])) for s in sittings):
            break
        page += 1
    return listed


# --------------------------------------------------------------------------- #
# One sitting
# --------------------------------------------------------------------------- #

def page_sitting(page: bytes) -> Optional[tuple[int, int]]:
    """``(legislature, sitting)`` as the WebTV page states it, ``None`` for a page that is not a sitting's."""
    m = _PAGE_SITTING_RE.search(page)
    return (int(m.group(1)), int(m.group(2))) if m else None


def is_record_of(xml: bytes, period: int, number: int) -> bool:
    """Whether ``xml`` is the stenographic record of that sitting, with at least one intervention."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return False
    return (root.tag == "seduta"
            and root.get("legislatura") == str(period)
            and root.get("numero") == str(number)
            and root.find(".//intervento") is not None)


def _stored_entry(config, session: str) -> Optional[dict]:
    path = config.raw_archive_entry(session)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def is_complete(config, session: str) -> bool:
    """Whether a sitting is stored with its record and a page that links it."""
    entry = _stored_entry(config, session)
    return bool(entry and entry.get("record") and entry.get("recordLinked")
                and config.raw_event(session).exists() and config.raw_record(session).exists())


def fetch_sitting(config, session: str, entry: dict, webtv: _Source, documenti: _Source) -> bool:
    """Fetch the page and the record of one sitting. Returns whether anything was written."""
    period, number = parse_session_key(session)
    page = webtv.get(EVENT_PAGE.format(event=entry["event"]))
    if page_sitting(page) != (period, number):
        logger.warning(f"[{session}] event {entry['event']} is not the page of this sitting, skipped")
        return False
    if _PAGE_LIVE_RE.search(page):
        logger.info(f"[{session}] the sitting is still live, skipped")
        return False
    record = documenti.get(RECORD_XML.format(number=number, period=period))
    has_record = is_record_of(record, period, number)
    links_record = bool(_PAGE_LINKS_RECORD_RE.search(page))

    for directory in ("media", "proceedings"):
        config.dir(directory, create=True)
    config.raw_event(session).write_bytes(page)
    if has_record:
        config.raw_record(session).write_bytes(record)
    config.raw_archive_entry(session).write_text(json.dumps(
        {"session": session, **entry, "record": has_record, "recordLinked": links_record},
        indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"[{session}] fetched the page ({len(page)} bytes)"
                + (f" and the record ({len(record)} bytes)" if has_record else "; no record published yet")
                + ("" if links_record else "; the page does not link the record yet"))
    return True


def fetch_originals(config, args) -> None:
    if args.period is None:
        args.period = max(load_manifest(PARLIAMENT_ID)["periods"])
    period = int(args.period)
    retry_count = getattr(args, "retry_count", None) or 10
    force = bool(getattr(args, "force", False))
    webtv = _Source("webtv.camera.it", retry_count)
    documenti = _Source("documenti.camera.it", retry_count)

    named = list(getattr(args, "it_session", None) or [])
    sittings: dict[str, dict] = {}
    if named:
        for session in named:
            key_period, number = parse_session_key(session)
            if not in_scope(args, session):
                logger.info(f"[{session}] outside period {period} or --limit-session, skipped")
            elif not force and is_complete(config, session):
                logger.debug(f"[{session}] already fetched")
            else:
                entry = find_sitting(webtv, key_period, number)
                if entry is None:
                    logger.warning(f"[{session}] the WebTV archive lists no such sitting")
                else:
                    sittings[session] = entry
    else:
        whole_period = force or bool(getattr(args, "limit_session", ""))
        known = (lambda session: False) if whole_period else (lambda session: is_complete(config, session))
        for session, entry in list_sittings(webtv, period, known=known).items():
            if in_scope(args, session) and (force or not is_complete(config, session)):
                sittings[session] = entry

    written = 0
    for session in sorted(sittings):
        written += fetch_sitting(config, session, sittings[session], webtv, documenti)
    logger.info(f"IT fetch: {written} of {len(sittings)} sitting(s) written; "
                f"{webtv.requests} request(s) to {webtv.name}, {documenti.requests} to {documenti.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=None)
    parser.add_argument("--session", dest="it_session", action="append", default=[],
                        help="session key to fetch, e.g. 190731 (repeatable)")
    parser.add_argument("--limit-session", default="", help="regex of the session keys to fetch")
    parser.add_argument("--force", action="store_true", help="fetch again what is already there")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    fetch_originals(load_config(PARLIAMENT_ID, args.data_dir), args)


if __name__ == "__main__":
    main()
