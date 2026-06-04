#! /usr/bin/env python3
"""Merge the av intervention spine with the DAR text into Stage 2 JSON.

**The av interventions are the spine** (one intervention = one OPTV speech, with
its video clip + offsets). The DAR ``?sft=true`` text supplies the verbatim
``textContents``. The two streams are joined by a **speaker-sequence alignment**
(Needleman-Wunsch on a per-turn match key: the speaker's surname for deputies,
or a canonical role for the chair/secretary/government), because the text is
finer-grained than the av list (it interleaves chair interjections the av list
does not enumerate). This is the DE two-source pattern; only the join key shape
differs.

Inputs::

    original/media/{session}-media.json          (media parser; the spine)
    original/proceedings/{session}-proceedings.json  (text turns; optional)

Output: ``cache/merged/{session}-merged.json`` (validates against
``optv/shared/schema/stage2-full.schema.json``). A speech whose av intervention
finds no text match gets ``textContents: []`` — align/NER skip it, exactly like a
not-yet-transcribed session, so the media half always publishes.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Optional

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.PT.merger"

from optv.parliaments.PT.common import (
    Config, parse_session, save_if_changed, session_number_int, source_label,
)
from optv.shared.agenda_types import annotate_agenda_item, classify_pt
from optv.shared.speech_id import normalize_speech_originid
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

PARLIAMENT = "PT"
CREATOR = _get_rights("PT", stream="media")["creator"]
LICENSE = _get_rights("PT", stream="media")["license"]
_WS_RE = re.compile(r"\s+")

# av roles that map to a canonical chair/officer/government match key (so they
# align to the text's "O Sr. Presidente:" / "O Sr. Secretário (…):" turns rather
# than by the office-holder's surname).
_ROLE_KEYS = {
    "presidente": "presidente",
    "vice presidente": "presidente",
    "secretario": "secretario",
    "secretaria": "secretario",
}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return _WS_RE.sub(" ", s.lower().replace("-", " ")).strip()


def _surname(name: str) -> str:
    n = _norm(name)
    return n.split(" ")[-1] if n else ""


def _av_match_key(intervention: dict) -> str:
    """Match key for an av intervention: canonical role, else speaker surname."""
    role = _norm(intervention.get("role") or "")
    for prefix, key in _ROLE_KEYS.items():
        if role == prefix or role.startswith(prefix + " "):
            return key
    if role.startswith("ministr") or role.startswith("secretario de estado") \
            or role.startswith("primeir"):
        return "ministro"
    return _surname(intervention.get("speaker") or "")


# --------------------------------------------------------------------------- #
# Needleman-Wunsch sequence alignment of av keys vs text-turn keys
# --------------------------------------------------------------------------- #

def align_speeches(av_keys: list[str], text_keys: list[str],
                   *, match: int = 2, mismatch: int = -1, gap: int = -1
                   ) -> dict[int, int]:
    """Return ``{av_index: text_index}`` for diagonally-matched, equal-key pairs.

    Classic global alignment; only pairs whose keys are equal (and non-empty)
    are reported as matches. Unmatched av speeches (gaps) get no text; surplus
    text turns (chair interjections etc.) are simply dropped.
    """
    m, n = len(av_keys), len(text_keys)
    if m == 0 or n == 0:
        return {}
    # score matrix (m+1) x (n+1)
    score = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        score[i][0] = i * gap
    for j in range(1, n + 1):
        score[0][j] = j * gap
    for i in range(1, m + 1):
        ai = av_keys[i - 1]
        for j in range(1, n + 1):
            equal = ai and ai == text_keys[j - 1]
            diag = score[i - 1][j - 1] + (match if equal else mismatch)
            score[i][j] = max(diag, score[i - 1][j] + gap, score[i][j - 1] + gap)
    # traceback
    mapping: dict[int, int] = {}
    i, j = m, n
    while i > 0 and j > 0:
        ai = av_keys[i - 1]
        equal = ai and ai == text_keys[j - 1]
        if score[i][j] == score[i - 1][j - 1] + (match if equal else mismatch):
            if equal:
                mapping[i - 1] = j - 1
            i, j = i - 1, j - 1
        elif score[i][j] == score[i - 1][j] + gap:
            i -= 1
        else:
            j -= 1
    return mapping


# --------------------------------------------------------------------------- #
# Stage 2 assembly
# --------------------------------------------------------------------------- #

def _person(intervention: dict) -> dict:
    role = _norm(intervention.get("role") or "")
    is_chair = role.startswith("presidente") or role.startswith("vice presidente")
    label = (intervention.get("speaker") or "").strip() or "Desconhecido"
    person: dict[str, Any] = {
        "type": "presidencyOfParliament" if is_chair else "memberOfParliament",
        "label": label,
        "context": "president" if is_chair else "main-speaker",
    }
    src_role = (intervention.get("role") or "").strip()
    if src_role and not is_chair and src_role.lower() != "deputado":
        person["role"] = src_role
    affiliation = intervention.get("affiliation") or {}
    initials = (affiliation.get("initials") or "").strip()
    name = (affiliation.get("name") or "").strip()
    if initials and not is_chair:
        faction: dict[str, Any] = {"label": initials}
        if name:
            faction["labelAlternative"] = [name]
        person["faction"] = faction
    return person


def _media_block(intervention: dict) -> dict:
    start_offset = intervention.get("startOffset")
    duration = intervention.get("duration")
    block: dict[str, Any] = {
        "videoFileURI": intervention.get("videoFileURI") or "",
        "sourcePage": intervention.get("sourcePage") or "",
        "audioFileURI": intervention.get("audioFileURI") or "",
        "creator": CREATOR,
        "license": LICENSE,
        "aligned": False,
    }
    if duration is not None:
        block["duration"] = round(float(duration), 2)
    addinfo: dict[str, Any] = {}
    if start_offset is not None:
        addinfo["startOffset"] = round(float(start_offset), 2)
    if addinfo:
        block["additionalInformation"] = addinfo
    return block


def _agenda_item(intervention: dict, session_description: str) -> dict:
    title = (session_description or intervention.get("interventionType")
             or "Reunião plenária").strip()
    agenda: dict[str, Any] = {"officialTitle": title, "title": title}
    native, core = classify_pt(intervention.get("interventionType"))
    annotate_agenda_item(agenda, native, core)
    return agenda


def _text_contents(turn: Optional[dict], person_label: str) -> list[dict]:
    if not turn or not turn.get("sentences"):
        return []
    return [{
        "type": "proceedings",
        "language": "pt",
        "creator": CREATOR,
        "license": LICENSE,
        "textBody": [{
            "type": "speech",
            "speaker": person_label,
            "sentences": turn["sentences"],
        }],
    }]


def _date_plus(base_iso: Optional[str], offset_s: Optional[float]) -> Optional[str]:
    if not base_iso:
        return None
    try:
        base = datetime.datetime.fromisoformat(base_iso)
    except ValueError:
        return base_iso
    if offset_s:
        base = base + datetime.timedelta(seconds=float(offset_s))
    return base.isoformat()


def merge_session(session: str, config: Config, args=None) -> Path:
    media_path = config.file(session, "media")
    if not media_path.exists():
        raise FileNotFoundError(f"[{session}] media missing: {media_path}")
    media_doc = json.loads(media_path.read_text())
    interventions = media_doc.get("data") or []
    if not interventions:
        raise RuntimeError(f"[{session}] no interventions to merge")
    media_meta = media_doc.get("meta") or {}

    proc_path = config.file(session, "proceedings")
    turns: list[dict] = []
    if proc_path.exists():
        turns = (json.loads(proc_path.read_text()).get("data") or [])
    else:
        logger.warning(f"[{session}] no proceedings — emitting media-only Stage 2")

    av_keys = [_av_match_key(iv) for iv in interventions]
    text_keys = [t.get("matchKey") or "" for t in turns]
    mapping = align_speeches(av_keys, text_keys) if turns else {}
    logger.info(f"[{session}] matched {len(mapping)}/{len(interventions)} speeches "
                f"to text ({len(turns)} text turns available)")

    leg = int(getattr(args, "period", None) or media_meta.get("legislature") or 17)
    session_number = session_number_int(session)
    session_start = media_meta.get("dateStart") or (media_meta.get("eventDate") or None)
    description = media_meta.get("description") or ""

    records: list[dict] = []
    last_date = session_start
    for idx, iv in enumerate(interventions, start=1):
        person = _person(iv)
        turn = turns[mapping[idx - 1]] if (idx - 1) in mapping else None
        date_start = _date_plus(session_start, iv.get("startOffset")) or last_date
        last_date = date_start
        rec: dict[str, Any] = {
            "parliament": PARLIAMENT,
            "electoralPeriod": {"number": leg},
            "session": {"number": session_number, "dateStart": session_start},
            "speechIndex": idx,
            "agendaItem": _agenda_item(iv, description),
            "dateStart": date_start,
            "media": _media_block(iv),
            "people": [person],
            "textContents": _text_contents(turn, person["label"]),
            "originID": f"{session}-{iv.get('number')}",
            "originalLanguage": "pt",
            "debug": {
                "interventionType": iv.get("interventionType") or "",
                "avNumber": iv.get("number"),
                "matchKey": av_keys[idx - 1],
                "startOffset": iv.get("startOffset"),
                "textTurnIndex": (turn or {}).get("index"),
            },
        }
        records.append(rec)

    now = datetime.datetime.utcnow().isoformat(timespec="seconds")
    for _s in records:
        normalize_speech_originid(_s)
    out_doc = {
        "meta": {
            "session": session,
            "parliament": PARLIAMENT,
            "electoralPeriod": leg,
            "sourceLabel": source_label(session),
            "dateStart": session_start,
            "dateEnd": last_date,
            "lastUpdate": now,
            "lastProcessing": "merge",
            "processing": {
                **(media_meta.get("processing") or {}),
                "merge": now,
            },
        },
        "data": records,
    }
    out_path = config.file(session, "merged", create=True)
    if save_if_changed(out_doc, out_path):
        logger.info(f"[{session}] wrote {out_path.name} ({len(records)} speeches)")
    else:
        logger.info(f"[{session}] no content change; left {out_path.name} untouched")
    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("session", type=str, help="Session key, e.g. 17-1-059")
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parse_session(args.session)
    config = Config(args.data_dir)
    out = merge_session(args.session, config, args)
    print(out)


if __name__ == "__main__":
    main()
