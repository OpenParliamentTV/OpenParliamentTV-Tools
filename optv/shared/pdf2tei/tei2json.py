"""Read a PDF→TEI session into proceedings **text turns** for the spine join.

The PDF tier follows the PT/ES pattern: the media spine is the record source,
and the proceedings supply the verbatim text matched onto each clip. So this
reader emits a lean list of speaker turns — each with a ``matchKey`` (surname)
and sentence-split text — not a full Stage-2 speech document. It reads the TEI
dialect :mod:`optv.shared.pdf2tei.pdf2tei` emits plus its ``listPerson`` /
``listOrg`` registries, and is fully self-contained (no dependency on any
parliament's parser).

Turn shape (mirrors ``optv/parliaments/PT/parsers/proceedings2json.py``)::

    {"index", "speaker", "surname", "matchKey", "role", "party", "isChair",
     "originTextID", "agendaTitle", "sentences": [{"text": …}, …]}
"""
from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import Callable, Optional

from lxml import etree

from ..lang.de import match_key_surname

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
_T = f"{{{TEI_NS}}}"
_XMLID = f"{{{XML_NS}}}id"


def _regex_sentencize(text: str) -> list[str]:
    """German sentence split: break after .!?… when followed by an uppercase
    start. Abbreviation-naive but adequate for matching/alignment."""
    text = (text or "").strip()
    if not text:
        return []
    return [s.strip() for s in re.split(r"(?<=[.!?…])\s+(?=[A-ZÄÖÜ„\"»])", text)
            if s.strip()]


def _load_registries(person_root, org_root) -> tuple[dict, dict]:
    """Return (who_id -> {forename, surname, faction_slug, is_gov},
              faction_slug -> display label)."""
    org_label: dict[str, str] = {}
    if org_root is not None:
        for org in org_root.iter(f"{_T}org"):
            slug = (org.get(_XMLID) or "").replace("parliamentaryGroup.", "")
            full = next((o.text for o in org.findall(f"{_T}orgName")
                         if o.get("full") == "yes"), None)
            if slug and full:
                org_label[slug] = full
    persons: dict[str, dict] = {}
    if person_root is not None:
        for p in person_root.iter(f"{_T}person"):
            pid = p.get(_XMLID)
            if not pid:
                continue
            pn = p.find(f"{_T}persName")
            forename = surname = ""
            if pn is not None:
                forename = (pn.findtext(f"{_T}forename") or "").strip()
                surname = (pn.findtext(f"{_T}surname") or "").strip()
            slug = None
            is_gov = False
            for aff in p.findall(f"{_T}affiliation"):
                ref = aff.get("ref") or ""
                if ref.startswith("#parliamentaryGroup."):
                    slug = ref.replace("#parliamentaryGroup.", "")
                elif ref.startswith("#government"):
                    is_gov = True
            persons[pid] = {"forename": forename, "surname": surname,
                            "faction_slug": slug, "is_gov": is_gov}
    return persons, org_label


def tei_to_turns(data_root, person_root=None, org_root=None, *,
                 sentencize: Optional[Callable[[str], list[str]]] = None
                 ) -> list[dict]:
    """Convert a parsed TEI ``<text>`` tree into proceedings turns."""
    sentencize = sentencize or _regex_sentencize
    persons, org_label = _load_registries(person_root, org_root)
    turns: list[dict] = []
    idx = 0
    for div in data_root.iter(f"{_T}div"):
        if div.get("type") != "debateSection":
            continue
        head = div.findtext(f"{_T}head")
        agenda = (head or "").strip()
        # <note type="speaker"> precedes each <u>; walk children in order.
        pending_label = None
        for el in div:
            tag = etree.QName(el).localname
            if tag == "note" and el.get("type") == "speaker":
                pending_label = (el.text or "").strip()
            elif tag == "u":
                idx += 1
                who = (el.get("who") or "").lstrip("#")
                is_chair = el.get("ana") == "#chair"
                pinfo = persons.get(who, {})
                label = pending_label or " ".join(
                    t for t in (pinfo.get("forename"), pinfo.get("surname")) if t)
                # Prefer the registry name for the match key (clean surname);
                # fall back to the spoken label.
                name_for_key = " ".join(
                    t for t in (pinfo.get("forename"), pinfo.get("surname")) if t) or label
                slug = pinfo.get("faction_slug")
                party = org_label.get(slug, slug or "")
                text = " ".join((seg.text or "").strip()
                                for seg in el.findall(f"{_T}seg")).strip()
                turns.append({
                    "index": idx,
                    "speaker": label,
                    "surname": (pinfo.get("surname") or "").strip(),
                    "matchKey": match_key_surname(name_for_key),
                    "role": "" if not pinfo.get("is_gov") else "government",
                    "party": "" if is_chair else party,
                    "isChair": is_chair,
                    "originTextID": el.get(_XMLID) or "",
                    "agendaTitle": agenda,
                    "sentences": [{"text": s} for s in sentencize(text)],
                })
                pending_label = None
    return turns


def _parse_xml(path: Path):
    return etree.parse(str(path)).getroot() if Path(path).exists() else None


def parse_tei_dir(tei_dir: Path, sid: str, parliament: str, *,
                  session: Optional[str] = None,
                  sentencize: Optional[Callable[[str], list[str]]] = None) -> dict:
    """Read ``<tei_dir>/<sid>-data.xml`` + the listPerson/listOrg registries and
    return a ``{"meta": …, "data": [turns]}`` proceedings document."""
    tei_dir = Path(tei_dir)
    data_root = etree.parse(str(tei_dir / f"{sid}-data.xml")).getroot()
    person_root = _parse_xml(tei_dir / f"ParlaMint-{parliament}-listPerson.xml") \
        or _parse_xml(tei_dir / "ParlaMint-DE-listPerson.xml")
    org_root = _parse_xml(tei_dir / f"ParlaMint-{parliament}-listOrg.xml") \
        or _parse_xml(tei_dir / "ParlaMint-DE-listOrg.xml")
    turns = tei_to_turns(data_root, person_root, org_root, sentencize=sentencize)
    return {
        "meta": {
            "session": session or sid,
            "parliament": parliament,
            "processing": {
                "parse_proceedings":
                    datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": turns,
    }
