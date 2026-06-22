#! /usr/bin/env python3
"""Parse an Eduskunta PTK plenary-minutes XML into Stage-2-shaped proceedings.

Input:  ``original/proceedings/{session}-ptk.xml`` (the VaskiData document)
Output: ``original/proceedings/{session}-proceedings.json``
        ``{"meta": {...}, "data": [<speech>, ...]}`` — per-speech entries with
        ``people`` / ``textContents`` / ``agendaItem`` but **no ``media``**;
        the merger grafts video onto these from the broadcast feed.

PTK structure (namespaces stripped to local names):

    Asiakohta                          (agenda item; has KohtaNumero + NimekeTeksti)
      PuheenvuoroToimenpide            (one per speech)
        @puheenvuoroAloitusHetki       (local-time start — join key to the video)
        Toimija/Henkilo @muuTunnus     (= personNumber, the relational join key)
          EtuNimi / SukuNimi           (speaker name)
          LisatietoTeksti              (group abbreviation, e.g. "ps", or a role)
        PuheenvuoroOsa @kieliKoodi     (per-speech language fi/sv → originalLanguage)
          KohtaSisalto/KappaleKooste   (speech paragraphs)
            PuheenjohtajaRepliikki     (interleaved chair remarks — excluded)

Sentence segmentation uses the Finnish spaCy model from the manifest. Editorial
brackets (``[Hälinää]``, ``[Puhemies koputtaa]`` — non-spoken stage directions)
are stripped before sentencizing so they don't pollute alignment.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Optional

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.FI.parsers"

import lxml.etree as ET

from optv.parliaments.FI.common import Config, parse_session_str
from optv.shared.agenda_types import annotate_agenda_item, classify_fi
from optv.shared.sentence_split import split_long_sentences
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

PARLIAMENT_CODE = "FI"
SPEECH_CREATOR = _get_rights("FI", stream="proceedings")["creator"]
SPEECH_LICENSE = _get_rights("FI", stream="proceedings")["license"]
# Known parliamentary-group abbreviations; anything else in LisatietoTeksti is
# treated as a ministerial/role string rather than a faction.
_GROUP_CODES = {"ps", "kok", "sd", "sdp", "kesk", "vihr", "vas", "r", "rkp", "kd", "liik", "li"}

_BRACKET_RE = re.compile(r"\[[^\]]*\]")
_WS_RE = re.compile(r"\s+")


def _ln(el) -> str:
    return ET.QName(el).localname


def _findall_local(root, name: str) -> list:
    return root.xpath(f".//*[local-name()=$n]", n=name)


def _first_text(root, name: str) -> Optional[str]:
    els = _findall_local(root, name)
    for el in els:
        t = (el.text or "").strip()
        if t:
            return t
    return None


def _has_ancestor(el, name: str) -> bool:
    p = el.getparent()
    while p is not None:
        if _ln(p) == name:
            return True
        p = p.getparent()
    return False


def _clean_text(s: str) -> str:
    s = _BRACKET_RE.sub(" ", s or "")
    return _WS_RE.sub(" ", s).strip()


def _speaker_paragraphs(osa) -> list[str]:
    """KappaleKooste text under this speech turn, excluding chair remarks."""
    paras: list[str] = []
    for kk in _findall_local(osa, "KappaleKooste"):
        if _has_ancestor(kk, "PuheenjohtajaRepliikki"):
            continue
        text = _clean_text("".join(kk.itertext()))
        if text:
            paras.append(text)
    return paras


def split_sentences(nlp, text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []
    doc = nlp(text)
    sents = [s.text.strip() for s in doc.sents if s.text.strip()]
    # Generic (punctuation-only) length-gated split of over-long sentences.
    return split_long_sentences(sents)


def _agenda_for(toimenpide, default_title: str) -> tuple[dict, Optional[str], Optional[str]]:
    """Return (agendaItem dict, kohta_number, native/core via classify_fi)."""
    p = toimenpide.getparent()
    kohta = None
    while p is not None:
        if _ln(p) in ("Asiakohta", "MuuAsiakohta"):
            kohta = p
            break
        p = p.getparent()
    if kohta is not None:
        number = _first_text(kohta, "KohtaNumero")
        title = (_first_text(kohta, "NimekeTeksti")
                 or _first_text(kohta, "OtsikkoTeksti")
                 or default_title)
    else:
        number = None
        title = default_title
    native, core = classify_fi(title)
    agenda: dict[str, Any] = {"officialTitle": title, "title": title}
    if number:
        agenda["id"] = f"kohta-{number}"
    annotate_agenda_item(agenda, native, core)
    return agenda, number, title


def _attr_local(el, name: str) -> Optional[str]:
    """Read an attribute by local name (Eduskunta XML namespaces every attr)."""
    if el is None:
        return None
    for k, v in el.attrib.items():
        if k.split("}")[-1] == name:
            return v
    return None


def _person(toimenpide) -> dict:
    henkilo = (_findall_local(toimenpide, "Henkilo") or [None])[0]
    first = last = group = ""
    person_number = ""
    if henkilo is not None:
        first = (_first_text(henkilo, "EtuNimi") or "").strip()
        last = (_first_text(henkilo, "SukuNimi") or "").strip()
        group = (_first_text(henkilo, "LisatietoTeksti") or "").strip()
        person_number = _attr_local(henkilo, "muuTunnus") or ""
    label = f"{first} {last}".strip() or "Tuntematon"
    person: dict[str, Any] = {
        "type": "memberOfParliament",
        "label": label,
        "context": "main-speaker",
    }
    if first:
        person["firstname"] = first
    if last:
        person["lastname"] = last
    if person_number:
        person["originPersonID"] = str(person_number)
    if group:
        if group.lower() in _GROUP_CODES:
            person["faction"] = {"label": group}
        else:
            # Ministerial / office title — not a party. Keep as role; no faction.
            person["role"] = group
    return person, str(person_number or "")


def speech_record(toimenpide, speech_index: int, nlp, default_title: str) -> Optional[dict]:
    osas = _findall_local(toimenpide, "PuheenvuoroOsa")
    if not osas:
        return None
    osa = osas[0]
    language = (_attr_local(osa, "kieliKoodi") or "fi").lower()
    start = _attr_local(toimenpide, "puheenvuoroAloitusHetki")
    end = _attr_local(osa, "puheenvuoroLopetusHetki")
    origin_id = _attr_local(osa, "muuTunnus") or ""

    person, person_number = _person(toimenpide)
    agenda, _kohta, _title = _agenda_for(toimenpide, default_title)

    paragraphs = _speaker_paragraphs(osa)
    sentences = [{"text": s} for para in paragraphs for s in split_sentences(nlp, para)]

    record: dict[str, Any] = {
        "parliament": PARLIAMENT_CODE,
        "agendaItem": agenda,
        "speechIndex": speech_index,
        "originID": str(origin_id or ""),
        "originalLanguage": language,
        "people": [person],
        "textContents": [{
            "type": "proceedings",
            "language": language,
            "originTextID": str(origin_id or ""),
            "creator": SPEECH_CREATOR,
            "license": SPEECH_LICENSE,
            "textBody": [{
                "type": "speech",
                "speaker": person["label"],
                "speakerstatus": person.get("role"),
                "sentences": sentences,
            }],
        }],
        "debug": {
            "personNumber": person_number,
            "ptkStart": start,
            "ptkEnd": end,
        },
    }
    if start:
        record["dateStart"] = start
    if end:
        record["dateEnd"] = end
    return record


def parse_ptk(xml_bytes: bytes, spacy_model: str, year: int, number: int) -> dict:
    import spacy
    logger.info(f"Loading spaCy model {spacy_model}")
    nlp = spacy.load(spacy_model)

    root = ET.fromstring(xml_bytes)
    default_title = "Täysistunto"
    toimenpiteet = _findall_local(root, "PuheenvuoroToimenpide")

    speeches: list[dict] = []
    for tp in toimenpiteet:
        rec = speech_record(tp, len(speeches) + 1, nlp, default_title)
        if rec is not None:
            speeches.append(rec)

    dates = sorted(s["dateStart"] for s in speeches if s.get("dateStart"))
    date_start = dates[0] if dates else None
    date_end = max((s.get("dateEnd") or s.get("dateStart")) for s in speeches) if speeches else None

    return {
        "meta": {
            "session": f"{year}-{number:03d}",
            "sourceLabel": f"PTK {number}/{year} vp",
            "processing": {
                "parse_proceedings": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
            "dateStart": date_start,
            "dateEnd": date_end,
        },
        "data": speeches,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 2026-058")
    parser.add_argument("--spacy-model", default=None)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    spacy_model = args.spacy_model
    if spacy_model is None:
        from optv.parliaments import get_locale
        spacy_model = get_locale("FI")["spacy_model"]

    config = Config(args.data_dir)
    year, number = parse_session_str(args.session)
    xml_path = config.raw_ptk(args.session)
    if not xml_path.exists():
        sys.exit(f"PTK XML not found: {xml_path}")
    doc = parse_ptk(xml_path.read_bytes(), spacy_model, year, number)
    out = config.file(args.session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} speeches)")


if __name__ == "__main__":
    main()
