"""Turn the WebTV page of an IT sitting into ``original/media/{session}-media.json``.

One record per intervention the page lists: the speaker with role and group, the window of the recording it fills, the title it stands under, and the id of the record intervention it starts with, where the page gives one. The windows follow each other without a gap, so together they cover the sitting. ``meta`` carries the recording's MP4 parts and the titles as a tree (agenda item, subject, phase).

Run on its own: ``python -m optv.parliaments.IT.parsers.media2json <data_dir> --session 190731``.
"""

import argparse
import datetime
import json
import logging
import re
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from lxml import html

from optv.shared.cli import load_config, setup_logging
from optv.shared.meta import now_iso

from ..common import in_scope, parse_session_key

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

ROME = ZoneInfo("Europe/Rome")
EVENT_PAGE = "https://webtv.camera.it/evento/{event}"

_PHOTO_ID_RE = re.compile(r"/d(\d+)\.jpg")


def _text(element) -> str:
    return " ".join(element.text_content().split())


def local_time(value: Optional[str]) -> Optional[str]:
    """The WebTV's ``2026-10-22 09:56:17`` (Rome time) as ISO 8601 with its offset."""
    if not value:
        return None
    try:
        moment = datetime.datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return moment.replace(tzinfo=ROME).isoformat(timespec="seconds")


def _seconds(value: Optional[str]) -> Optional[int]:
    value = (value or "").strip()
    return int(value) if value.isdigit() else None


def split_link_text(text: str, name: str) -> tuple[Optional[str], Optional[str]]:
    """Role and group from an entry's link text: ``Deputata PICCOLOTTI Elisabetta (ALLEANZA VERDI E SINISTRA)`` with the name ``PICCOLOTTI Elisabetta`` gives ``("Deputata", "ALLEANZA VERDI E SINISTRA")``."""
    if not name or name not in text:
        return None, None
    before, after = text.split(name, 1)
    role = before.strip() or None
    after = after.strip()
    group = after[1:-1].strip() if after.startswith("(") and after.endswith(")") else None
    return role, group or None


def record_id(steno_map: Optional[str]) -> Optional[str]:
    """The page's ``tit00040_sub00030_int00180`` as the record writes it, ``tit00040.sub00030.int00180``."""
    steno_map = (steno_map or "").strip()
    return steno_map.replace("_", ".") if steno_map else None


def _person_ids(page) -> dict[str, str]:
    """The Chamber's person id per entry, read from the portrait the index of interventions shows."""
    ids = {}
    for link in page.xpath('//a[starts-with(@id, "ora_indice_")]'):
        item = link.xpath("ancestor::li[1]")
        for src in (item[0].xpath(".//img/@src") if item else []):
            m = _PHOTO_ID_RE.search(src)
            if m:
                ids[link.get("id").rsplit("_", 1)[-1]] = m.group(1)
                break
    return ids


def _titles(page) -> dict[str, dict]:
    """The titles of the page's tree by id, each with its parent's id."""
    titles = {}
    for link in page.xpath('//a[@data-name="tit_href"]'):
        title_id = (link.get("id") or "").replace("tit_txt_", "")
        if title_id:
            titles[title_id] = {"title": _text(link), "parent": link.get("data-parent_title") or None}
    return titles


def _parts(page) -> list[dict]:
    """The recording's MP4 files in playing order."""
    parts = []
    for part in page.xpath('//div[starts-with(@id, "vod_")][@data-dwn_url]'):
        parts.append({"id": part.get("id").replace("vod_", ""),
                      "seq": int(part.get("data-seq") or 0),
                      "url": part.get("data-dwn_url")})
    return sorted(parts, key=lambda p: p["seq"])


def entry_record(link, person_ids: dict[str, str]) -> dict:
    entry_id = link.get("id").rsplit("_", 1)[-1]
    name = " ".join((link.get("data-etichetta") or "").split())
    role, group = split_link_text(_text(link), name)
    return {
        "id": entry_id,
        "recordId": record_id(link.get("data-steno_map")),
        "part": link.get("data-c_id"),
        "start": _seconds(link.get("data-tc_start")),
        "end": _seconds(link.get("data-tc_end")),
        "clock": local_time(link.get("data-dataora_c")),
        "name": name or None,
        "role": role,
        "group": group,
        "personId": person_ids.get(entry_id),
        "titleId": link.get("data-tit_id") or None,
    }


def parse_page(page_html: bytes, archive_entry: dict, session: str) -> dict:
    """The media document of one sitting from its WebTV page and its archive entry."""
    page = html.fromstring(page_html.decode("utf-8", errors="replace"))
    period, number = parse_session_key(session)
    person_ids = _person_ids(page)
    entries = [entry_record(link, person_ids)
               for link in page.xpath('//a[starts-with(@id, "ora_resoc_")]')]
    usable = [e for e in entries if e["start"] is not None and e["part"]]
    if len(usable) < len(entries):
        logger.warning(f"[{session}] {len(entries) - len(usable)} entr(ies) without an offset or a part, left out")
    parts = _parts(page)
    known_parts = {p["id"] for p in parts}
    stray = sorted({e["part"] for e in usable} - known_parts)
    if stray:
        logger.warning(f"[{session}] entries refer to part(s) the page has no file for: {stray}")
    return {
        "meta": {
            "session": session,
            "legislature": period,
            "sitting": number,
            "event": archive_entry["event"],
            "title": archive_entry.get("title"),
            "dateStart": local_time(archive_entry.get("dateStart")),
            "dateEnd": local_time(archive_entry.get("dateEnd")),
            "sourcePage": EVENT_PAGE.format(event=archive_entry["event"]),
            "parts": parts,
            "titles": _titles(page),
            "processing": {"parse_media": now_iso()},
        },
        "data": usable,
    }


def parse_session(config, session: str) -> Path:
    archive_entry = json.loads(config.raw_archive_entry(session).read_text(encoding="utf-8"))
    doc = parse_page(config.raw_event(session).read_bytes(), archive_entry, session)
    out = config.file(session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    linked = sum(1 for e in doc["data"] if e["recordId"])
    logger.info(f"[{session}] {len(doc['data'])} interventions on the page, {linked} linked to the record")
    return out


def parse_media(config, args) -> None:
    """Parse the WebTV page of every session in scope whose parsed file is missing or older."""
    for session in config.sessions():
        raw, entry = config.raw_event(session), config.raw_archive_entry(session)
        out = config.file(session, "media")
        if not in_scope(args, session) or not entry.exists():
            continue
        newest = max(raw.stat().st_mtime, entry.stat().st_mtime)
        if out.exists() and out.stat().st_mtime >= newest and not getattr(args, "force", False):
            continue
        parse_session(config, session)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", action="append", default=[], help="session key, e.g. 190731 (repeatable); default: all")
    parser.add_argument("--force", action="store_true", help="parse again what is already parsed")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    args.limit_to_period = False
    args.limit_session = "^(" + "|".join(re.escape(s) for s in args.session) + ")$" if args.session else ""
    parse_media(load_config(PARLIAMENT_ID, args.data_dir), args)


if __name__ == "__main__":
    main()
