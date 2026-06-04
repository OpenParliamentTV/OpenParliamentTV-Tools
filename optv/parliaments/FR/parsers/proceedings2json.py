#! /usr/bin/env python3
"""Parse an Assemblée nationale Syceron compte rendu into Stage-2 proceedings.

Input:  ``original/proceedings/{session}-cr.xml`` (modern namespaced Syceron CR)
Output: ``original/proceedings/{session}-proceedings.json``
        ``{"meta": {...}, "data": [<speech>, ...]}`` — per-speech entries with
        ``people`` / ``textContents`` / ``agendaItem`` and a per-speech
        ``debug.stime`` (video offset, seconds). **No ``media``** — the merger
        grafts the séance video onto each speech using ``stime``.

Compte-rendu structure (namespace stripped):

    compteRendu / contenu
      point @nivpoint @code_grammaire         (agenda heading; <texte> = title)
        paragraphe @id_acteur @code_grammaire @roledebat @code_style
          orateurs/orateur/{nom,id,qualite}   (speaker)
          texte @stime                        (spoken text; stime = video offset s)

Only ``<paragraphe>`` elements are speeches. Consecutive paragraphes by the same
``id_acteur`` within one agenda point are grouped into a single speech; editorial
paragraphes (``code_style`` ≠ NORMAL, e.g. vote tallies and "(Applaudissements…)")
become ``comment`` text bodies on the surrounding speech. Each speaker's
parliamentary group is looked up from ``metadata/acteurs.json`` (built by
``scraper/build_entity_dump.py``) because the compte rendu names the speaker but
not their groupe.
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
    __package__ = "optv.parliaments.FR.parsers"

import lxml.etree as ET

from optv.parliaments.FR.common import Config, parse_session
from optv.shared.agenda_types import annotate_agenda_item, classify_fr
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

NS = "{http://schemas.assemblee-nationale.fr/referentiel}"
PARLIAMENT_CODE = "FR"
SPEECH_CREATOR = _get_rights("FR", stream="proceedings")["creator"]
SPEECH_LICENSE = _get_rights("FR", stream="proceedings")["license"]
PARIS_TZ = datetime.timezone(datetime.timedelta(hours=2))  # CEST; séances sit Apr–Jul/Oct

_CIVILITE_RE = re.compile(r"^(?:MM?\.|Mme|Mlle|M\.)\s+", re.I)
_WS_RE = re.compile(r"\s+")


def _ln(el) -> str:
    return ET.QName(el).localname


def _text(el) -> str:
    if el is None:
        return ""
    return _WS_RE.sub(" ", "".join(el.itertext())).strip()


def _point_title(point) -> str:
    """A point's title is its first direct <texte> child (no stime)."""
    t = point.find(NS + "texte")
    return _text(t) if t is not None else ""


class _AgendaTracker:
    """Track the agenda hierarchy during a document-order walk.

    Syceron ``<point>`` elements are flat siblings whose ``nivpoint`` encodes the
    logical level (1 = rubric, 2 = item, 99 = procedural insert), so the parent
    rubric of a deep point is the most recent shallower point — not an XML
    ancestor. We keep a ``{level: (title, point_id)}`` map and clear deeper
    levels whenever a new point is seen.
    """

    def __init__(self):
        self._levels: dict[int, tuple[str, str]] = {}

    def push_point(self, point) -> None:
        title = _point_title(point)
        if not title:
            return
        try:
            niv = int(point.get("nivpoint") or 1)
        except ValueError:
            niv = 1
        pid = point.get("id_syceron") or point.get("ordre_absolu_seance") or title
        self._levels = {lvl: v for lvl, v in self._levels.items() if lvl < niv}
        self._levels[niv] = (title, str(pid))

    def current(self) -> tuple[str, str, str]:
        """Return (title, parent_title, agenda_key)."""
        if not self._levels:
            return "", "", ""
        levels = sorted(self._levels)
        title, key = self._levels[levels[-1]]
        parent = self._levels[levels[0]][0] if len(levels) > 1 else ""
        return title, parent, key


def _clean_name(nom: str) -> tuple[str, Optional[str]]:
    """Split a Syceron speaker string into (name, role).

    "M. Jean-Noël Barrot, ministre de l'Europe" → ("Jean-Noël Barrot",
    "ministre de l'Europe"); "Mme Clémence Guetté" → ("Clémence Guetté", None);
    "M. le président" → ("M. le président", None) — kept verbatim for chairs.
    """
    nom = (nom or "").strip()
    role = None
    if "," in nom:
        nom, role = nom.split(",", 1)
        nom = nom.strip()
        role = role.strip() or None
    low = nom.lower()
    if "le président" in low or "la présidente" in low or "le rapporteur" in low:
        return nom, role
    return _CIVILITE_RE.sub("", nom).strip() or nom, role


def _load_acteurs(config: Config) -> dict[str, dict]:
    path = config.dir("nel_data") / "acteurs.json"
    if not path.exists():
        logger.warning("metadata/acteurs.json missing — speakers will have no groupe")
        return {}
    return json.loads(path.read_text()).get("acteurs") or {}


class _Sentencizer:
    """spaCy French sentence segmentation with a regex fallback."""

    def __init__(self, spacy_model: Optional[str]):
        self._nlp = None
        if spacy_model:
            try:
                import spacy
                self._nlp = spacy.load(spacy_model, disable=["ner", "lemmatizer"])
                logger.info(f"loaded spaCy model {spacy_model}")
            except Exception as e:  # noqa: BLE001
                logger.warning(f"spaCy/{spacy_model} unavailable ({e}); "
                               "using regex sentence splitter")

    def __call__(self, text: str) -> list[str]:
        text = (text or "").strip()
        if not text:
            return []
        if self._nlp is not None:
            return [s.text.strip() for s in self._nlp(text).sents if s.text.strip()]
        return [s.strip() for s in re.split(r"(?<=[.!?…])\s+(?=[A-ZÀ-ÖØ-Ý])", text) if s.strip()]


_CHAIR_RE = re.compile(r"\ble\s+président\b|\bla\s+présidente\b", re.I)


def _is_chair(para, nom: str) -> bool:
    return para.get("roledebat") == "president" or bool(_CHAIR_RE.search(nom or ""))


def _person(para, orateur, acteurs: dict[str, dict]) -> dict:
    pa = para.get("id_acteur") or ""
    nom = _text(orateur.find(NS + "nom")) if orateur is not None else ""
    qualite = _text(orateur.find(NS + "qualite")) if orateur is not None else ""
    is_chair = _is_chair(para, nom)

    info = acteurs.get(pa) or {}
    label = info.get("label") or ""
    role = None
    if not label:
        label, role = _clean_name(nom)
    if not role:
        role = qualite or None

    person: dict[str, Any] = {
        "type": "presidencyOfParliament" if is_chair else "memberOfParliament",
        "label": label or nom or "Inconnu",
        "context": "president" if is_chair else "main-speaker",
    }
    if info.get("firstname"):
        person["firstname"] = info["firstname"]
    if info.get("lastname"):
        person["lastname"] = info["lastname"]
    if pa:
        person["originPersonID"] = pa
    if role and not is_chair:
        person["role"] = role
    group = info.get("groupAbbrev")
    if group and not is_chair:
        person["faction"] = {"label": group}
    return person


def _comment_body(text: str) -> dict:
    return {"type": "comment", "sentences": [{"text": text}]}


def _seance_start(root) -> Optional[datetime.datetime]:
    raw = root.findtext(".//" + NS + "dateSeance")
    if not raw or len(raw) < 14:
        return None
    try:
        dt = datetime.datetime.strptime(raw[:14], "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return dt.replace(tzinfo=PARIS_TZ)


def parse_cr(xml_bytes: bytes, session: str, acteurs: dict[str, dict],
             spacy_model: Optional[str]) -> dict:
    root = ET.fromstring(xml_bytes)
    sentencize = _Sentencizer(spacy_model)
    seance_start = _seance_start(root)
    contenu = root.find(".//" + NS + "contenu")
    if contenu is None:
        contenu = root

    agenda = _AgendaTracker()
    speeches: list[dict] = []
    current: Optional[dict] = None

    def flush():
        nonlocal current
        if current is not None:
            speeches.append(_finalize(current, sentencize))
            current = None

    # Walk contenu in document order so flat sibling <point> headings update the
    # agenda hierarchy before the <paragraphe> speeches that follow them.
    for el in contenu.iter():
        tag = _ln(el)
        if tag == "point":
            agenda.push_point(el)
            continue
        if tag != "paragraphe":
            continue
        para = el
        texte = para.find(NS + "texte")
        body = _text(texte)
        stime = texte.get("stime") if texte is not None else None
        code_style = (para.get("code_style") or "").upper()
        orateur = para.find(NS + "orateurs/" + NS + "orateur")
        pa = para.get("id_acteur") or ""
        editorial = (code_style != "NORMAL") or not pa or orateur is None

        if editorial:
            if body and current is not None:
                current["comments"].append((current["pending_after"], body))
            continue
        if not body:
            continue

        title, parent, agenda_key = agenda.current()
        same = (current is not None and current["id_acteur"] == pa
                and current["agenda_key"] == agenda_key)
        if same:
            current["paras"].append(body)
            current["pending_after"] = len(current["paras"])
        else:
            flush()
            person = _person(para, orateur, acteurs)
            current = {
                "id_acteur": pa,
                "agenda_key": agenda_key,
                "title": title,
                "parent": parent,
                "person": person,
                "paras": [body],
                "comments": [],          # list of (after_para_index, text)
                "pending_after": 1,
                "stime": stime,
                "origin_id": para.get("id_syceron") or "",
                "is_chair": person["context"] == "president",
            }

    flush()

    for idx, sp in enumerate(speeches, start=1):
        sp["speechIndex"] = idx
        if seance_start is not None and sp.get("_stime") is not None:
            sp["dateStart"] = (seance_start + datetime.timedelta(
                seconds=sp["_stime"])).isoformat()
        sp.pop("_stime", None)

    # Prefer the first/last speech offsets (present once the séance video has been
    # synced and carries stime); otherwise fall back to the séance start so the
    # session envelope always has a date even for not-yet-synced séances.
    seance_iso = seance_start.isoformat() if seance_start else None
    date_start = next((s.get("dateStart") for s in speeches if s.get("dateStart")),
                      seance_iso)
    date_end = next((s.get("dateStart") for s in reversed(speeches)
                     if s.get("dateStart")), date_start)

    return {
        "meta": {
            "session": session,
            "sourceLabel": f"Compte rendu {session}",
            "parliament": PARLIAMENT_CODE,
            "dateStart": date_start,
            "dateEnd": date_end,
            "processing": {
                "parse_proceedings": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": speeches,
    }


def _finalize(c: dict, sentencize) -> dict:
    sentences = [{"text": s} for para in c["paras"] for s in sentencize(para)]
    text_body: list[dict] = [{
        "type": "speech",
        "speaker": c["person"]["label"],
        "speakerstatus": c["person"].get("role"),
        "sentences": sentences,
    }]
    for _after, text in c["comments"]:
        text_body.append(_comment_body(text))

    stime_val = None
    if c["stime"]:
        try:
            stime_val = float(c["stime"])
        except ValueError:
            stime_val = None

    record: dict[str, Any] = {
        "parliament": PARLIAMENT_CODE,
        "agendaItem": _agenda_item(c),
        "originID": str(c["origin_id"]),
        "originalLanguage": "fr",
        "people": [c["person"]],
        "textContents": [{
            "type": "proceedings",
            "language": "fr",
            "originTextID": str(c["origin_id"]),
            "creator": SPEECH_CREATOR,
            "license": SPEECH_LICENSE,
            "textBody": text_body,
        }],
        "debug": {
            "stime": stime_val,
            "idActeur": c["id_acteur"],
            "agendaKey": c["agenda_key"],
        },
        "_stime": stime_val,
    }
    return record


def _agenda_item(c: dict) -> dict:
    title = c["title"] or c["parent"] or "Séance publique"
    agenda: dict[str, Any] = {"officialTitle": title, "title": title}
    if c["parent"] and c["parent"] != title:
        agenda["additionalInformation"] = {"parentTitle": c["parent"]}
    # Classify on the parent rubric when present (the procedural category lives
    # at the top level, e.g. "Questions au gouvernement"), else the item title.
    native, core = classify_fr(c["parent"] or title)
    if core == "regular":
        native, core = classify_fr(title)
    annotate_agenda_item(agenda, native, core)
    return agenda


def parse_proceedings_for_session(config: Config, session: str,
                                  spacy_model: Optional[str]) -> dict:
    xml_path = config.raw_cr(session)
    if not xml_path.exists():
        raise FileNotFoundError(f"[{session}] compte rendu XML missing: {xml_path}")
    acteurs = _load_acteurs(config)
    return parse_cr(xml_path.read_bytes(), session, acteurs, spacy_model)


def parse_proceedings_directory(config: Config, args) -> None:
    spacy_model = getattr(args, "spacy_model", None)
    for session in config.sessions():
        if getattr(args, "fr_session", None) and session not in args.fr_session:
            continue
        if getattr(args, "limit_session", None):
            try:
                if not re.match(args.limit_session, session):
                    continue
            except re.error:
                if args.limit_session != session:
                    continue
        out = config.file(session, "proceedings")
        raw = config.raw_cr(session)
        if (out.exists() and not args.force
                and out.stat().st_mtime > raw.stat().st_mtime):
            logger.debug(f"[{session}] proceedings cached")
            continue
        logger.info(f"[{session}] parsing compte rendu")
        doc = parse_proceedings_for_session(config, session, spacy_model)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"[{session}] wrote {out.name} ({len(doc['data'])} speeches)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 2026O1N232")
    parser.add_argument("--spacy-model", default=None)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spacy_model = args.spacy_model
    if spacy_model is None:
        from optv.parliaments import get_locale
        spacy_model = get_locale("FR")["spacy_model"]
    config = Config(args.data_dir)
    parse_session(args.session)  # validate key
    doc = parse_proceedings_for_session(config, args.session, spacy_model)
    out = config.file(args.session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} speeches)")


if __name__ == "__main__":
    main()
