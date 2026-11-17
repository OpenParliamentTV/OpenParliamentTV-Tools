#! /usr/bin/env python3
"""Parse the speech records of an FI sitting into Stage-2-shaped proceedings.

Input:  ``original/proceedings/{session}-speeches.json`` — the ``puheenvuoro``
        records of the open data API search (``scraper/fetch_proceedings.py``)
Output: ``original/proceedings/{session}-proceedings.json``
        ``{"meta": {...}, "data": [<speech>, ...]}`` — per-speech entries with
        ``people`` / ``textContents`` / ``agendaItem`` but **no ``media``**;
        the merger grafts video onto these from the broadcast feed.

Record fields used:

    id                                  speech id, e.g. "PUH 58/2026/6/1/1"
    puhuja.henkilonro                   person number (join key to the video)
    puhuja.etunimi / sukunimi           speaker name
    puhuja.asema                        office (ministers), else empty
    puhuja.lisatieto                    party abbreviation (empty for ministers)
    puhuja.eduskuntaryhma_tunnus        parliamentary group, e.g. "KOK01~…"
    poytakirjanasiankohta.fi            agenda item (kohtanumero, nimeketeksti)
    aloitushetki / lopetushetki         UTC start / end
    puheenvuoro                         speech text (one string; the chair's
                                        interjections are not included)
    tila                                record status (text revision state)

Neither the records nor the minutes mark the language of a speech, so Swedish
speeches are detected from the text (:func:`detect_language`). Sentence
segmentation uses the Finnish spaCy model from the manifest for every speech.
Editorial brackets (``[Hälinää]``, ``[Puhemies koputtaa]`` — non-spoken stage
directions) are stripped before sentencizing so they don't pollute alignment.
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
from zoneinfo import ZoneInfo

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.FI.parsers"

from optv.parliaments.FI.common import Config, parse_session_str
from optv.shared.agenda_types import annotate_agenda_item, classify_fi
from optv.shared.sentence_split import split_long_sentences
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

PARLIAMENT_CODE = "FI"
SPEECH_CREATOR = _get_rights("FI", stream="proceedings")["creator"]
SPEECH_LICENSE = _get_rights("FI", stream="proceedings")["license"]
HELSINKI = ZoneInfo("Europe/Helsinki")
DEFAULT_TITLE = "Täysistunto"

# Known party abbreviations in ``puhuja.lisatieto``.
_GROUP_CODES = {"ps", "kok", "sd", "sdp", "kesk", "vihr", "vas", "r", "rkp", "kd", "liik", "li"}
# Parliamentary-group code (prefix of ``eduskuntaryhma_tunnus``) → party
# abbreviation, for speakers without ``lisatieto`` (ministers).
_GROUP_PREFIX = {
    "KOK01": "kok", "KESK01": "kesk", "KD01": "kd", "LIIK01": "liik", "PS01": "ps",
    "R01": "r", "SD01": "sd", "VAS01": "vas", "VIHR01": "vihr",
}

_BRACKET_RE = re.compile(r"\[[^\]]*\]")
_WS_RE = re.compile(r"\s+")

# Function words for telling Swedish from Finnish speeches. Words that exist in
# both languages ("en": Finnish "I don't", Swedish "a") are left out.
_SV_WORDS = frozenset((
    "och att det som är inte för på med jag vi har ett av till om den de så "
    "kan ska också men eller vara här nu när från detta denna måste mycket "
    "bara alla även eftersom hur vad vilket där sig oss er ni man skulle"
).split())
_FI_WORDS = frozenset((
    "ja on että ei se ole me tämä mutta kun myös niin joka kuin ovat tässä "
    "siitä olla tai sen nyt vaan jos minä te sitä kanssa pitää siis kiitos "
    "kiitoksia näistä meidän tätä vain sitten koska jotka mitä ollut mikä "
    "tämän hyvin aivan"
).split())
_WORD_RE = re.compile(r"[a-zåäöéü]+")
# Minimum share of Swedish function words for a Swedish speech.
_SV_MIN_RATIO = 0.05


def detect_language(text: str) -> str:
    """``"sv"`` when Swedish function words outnumber Finnish ones, else ``"fi"``."""
    words = _WORD_RE.findall((text or "").lower())
    if not words:
        return "fi"
    sv = sum(w in _SV_WORDS for w in words)
    fi = sum(w in _FI_WORDS for w in words)
    if sv > fi and sv / len(words) >= _SV_MIN_RATIO:
        return "sv"
    return "fi"


def _clean_text(s: str) -> str:
    s = _BRACKET_RE.sub(" ", s or "")
    return _WS_RE.sub(" ", s).strip()


def split_sentences(nlp, text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []
    doc = nlp(text)
    sents = [s.text.strip() for s in doc.sents if s.text.strip()]
    # Generic (punctuation-only) length-gated split of over-long sentences.
    return split_long_sentences(sents)


def to_helsinki(utc_iso: Optional[str]) -> Optional[str]:
    """UTC ISO timestamp → Europe/Helsinki ISO with offset (seconds precision)."""
    if not utc_iso:
        return None
    try:
        dt = datetime.datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(HELSINKI).isoformat(timespec="seconds")


def _faction(puhuja: dict) -> Optional[str]:
    group = (puhuja.get("lisatieto") or "").strip()
    if group.lower() in _GROUP_CODES:
        return group
    prefix = (puhuja.get("eduskuntaryhma_tunnus") or "").split("~", 1)[0].strip()
    return _GROUP_PREFIX.get(prefix)


def _person(puhuja: dict) -> tuple[dict, str]:
    first = (puhuja.get("etunimi") or "").strip()
    last = (puhuja.get("sukunimi") or "").strip()
    person_number = str(puhuja.get("henkilonro") or "").strip()
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
        person["originPersonID"] = person_number
    faction = _faction(puhuja)
    if faction:
        person["faction"] = {"label": faction}
    role = (puhuja.get("asema") or "").strip()
    if role:
        person["role"] = role
    return person, person_number


def _agenda(rec: dict) -> dict:
    kohta = (rec.get("poytakirjanasiankohta") or {}).get("fi") or {}
    number = (kohta.get("kohtanumero") or "").strip()
    title = (kohta.get("nimeketeksti") or "").strip() or DEFAULT_TITLE
    native, core = classify_fi(title)
    agenda: dict[str, Any] = {"officialTitle": title, "title": title}
    if number:
        agenda["id"] = f"kohta-{number}"
    annotate_agenda_item(agenda, native, core)
    return agenda


def speech_record(rec: dict, speech_index: int, nlp) -> Optional[dict]:
    """One Stage-2-shaped speech from an API record; ``None`` when it has no text."""
    text = _clean_text(rec.get("puheenvuoro") or "")
    sentences = [{"text": s} for s in split_sentences(nlp, text)]
    if not sentences:
        return None
    language = detect_language(text)
    speech_id = (rec.get("id") or "").strip()
    person, person_number = _person(rec.get("puhuja") or {})

    debug: dict[str, Any] = {"personNumber": person_number}
    if rec.get("tila"):
        debug["tila"] = rec["tila"]
    speech_type = (rec.get("puheenvuorotyyppikoodi") or "").strip()
    if speech_type:
        debug["speechType"] = speech_type

    record: dict[str, Any] = {
        "parliament": PARLIAMENT_CODE,
        "agendaItem": _agenda(rec),
        "speechIndex": speech_index,
        "originID": speech_id,
        "originalLanguage": language,
        "people": [person],
        "textContents": [{
            "type": "proceedings",
            "language": language,
            "originTextID": speech_id,
            "creator": SPEECH_CREATOR,
            "license": SPEECH_LICENSE,
            "textBody": [{
                "type": "speech",
                "speaker": person["label"],
                "speakerstatus": person["context"],
                "sentences": sentences,
            }],
        }],
        "debug": debug,
    }
    start = to_helsinki(rec.get("aloitushetki"))
    end = to_helsinki(rec.get("lopetushetki"))
    if start:
        record["dateStart"] = start
    if end:
        record["dateEnd"] = end
    return record


def _date_range(speeches: list[dict]) -> tuple[Optional[str], Optional[str]]:
    def parse(s):
        return datetime.datetime.fromisoformat(s)
    starts = [s["dateStart"] for s in speeches if s.get("dateStart")]
    ends = [s.get("dateEnd") or s.get("dateStart") for s in speeches
            if s.get("dateEnd") or s.get("dateStart")]
    return (min(starts, key=parse) if starts else None,
            max(ends, key=parse) if ends else None)


def parse_speeches(records: list[dict], spacy_model: str, year: int, number: int,
                   *, nlp=None) -> dict:
    if nlp is None:
        import spacy
        logger.info(f"Loading spaCy model {spacy_model}")
        nlp = spacy.load(spacy_model)

    speeches: list[dict] = []
    for rec in records:
        speech = speech_record(rec, len(speeches) + 1, nlp)
        if speech is not None:
            speeches.append(speech)

    date_start, date_end = _date_range(speeches)
    return {
        "meta": {
            "session": f"{year}-{number:03d}",
            "sourceLabel": f"PTK {number}/{year} vp",
            "processing": {
                "parse_proceedings": datetime.datetime.now(datetime.timezone.utc)
                                     .isoformat(timespec="seconds"),
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
    raw_path = config.raw_speeches(args.session)
    if not raw_path.exists():
        sys.exit(f"Speech records not found: {raw_path}")
    records = json.loads(raw_path.read_text(encoding="utf-8"))
    doc = parse_speeches(records, spacy_model, year, number)
    out = config.file(args.session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} speeches)")


if __name__ == "__main__":
    main()
