#! /usr/bin/env python3
"""Merge the av intervention spine with the DAR text into Stage 2 JSON.

**The av interventions are the spine** (one intervention = one OPTV speech, with
its video clip + offsets). The DAR ``?sft=true`` text supplies the verbatim
``textContents``. This is the DE two-source pattern; only the join differs.

The Diário is finer-grained than the av list in two ways, so the turns are first
grouped into **speech units**:

- Interjections from the floor (*apartes*: "Muito bem!", "Oh!…") are speaker
  turns of their own. They are never av interventions.
- A speech interrupted by apartes continues under a repeated marker of the same
  speaker (often opening with ``…``). Those turns belong to one speech.

A unit is a run of one speaker's turns plus the apartes between them; a chair
turn ends a unit (the chair's remarks can be av interventions of their own).

The units are joined onto the av interventions by an equal-key
Needleman-Wunsch (:func:`optv.shared.sequence_align.align_equal_keys`). The key
is the speaker's surname (office holders resolved to their name), or the chair
key for the presiding officer. A pair is only compatible when:

- the unit's text is plausible for the clip (at least ``MIN_CHARS_PER_SECOND``
  per second of an intervention longer than ``PLAUSIBILITY_MIN_SECONDS``,
  counting the speaker's units that follow across chair remarks and apartes),
  which keeps a "Muito bem!" off a five-minute speech;
- when the open data Intervenções record gives the intervention's Diário pages,
  the unit lies on those pages.

After the join, each matched intervention absorbs the speaker's unmatched units
next to it (across unmatched chair remarks and apartes): a speech the chair
interrupted. The text body lists the speaker's turns as ``speech``
(``main-speaker``), chair remarks inside the clip as ``speech`` (``president``)
and apartes as ``comment`` blocks (``speaker``), which the aligner skips.

Agenda items come from the sitting description (items joined by `` / ``): each
intervention is assigned to an item by a monotone alignment of its summary (av
metadata, plus the open data debate where present) against the item titles.

Inputs::

    original/media/{session}-media.json              (media parser; the spine)
    original/proceedings/{session}-proceedings.json  (text turns; optional)

Output: ``cache/merged/{session}-merged.json``. A speech whose av intervention
finds no text gets ``textContents: []`` — align/NER skip it, exactly like a
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
from dataclasses import dataclass, field
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
from optv.shared.sequence_align import align_equal_keys
from optv.shared.speech_id import normalize_speech_originid
from optv.parliaments import get_rights as _get_rights
from optv.shared.meta import build_meta

logger = logging.getLogger(__name__)

PARLIAMENT = "PT"
CREATOR = _get_rights("PT", stream="media")["creator"]
LICENSE = _get_rights("PT", stream="media")["license"]
PROCEEDINGS_CREATOR = _get_rights("PT", stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights("PT", stream="proceedings")["license"]

CHAIR_KEY = "presidente"
_CHAIR_ROLES = ("presidente", "vice presidente")

# A turn by another speaker this short (characters) is an interjection, not a
# speech: it can sit inside a speech unit and never ends one.
APARTE_MAX_CHARS = 250
# How many units a speech may reach across (grouping and absorption).
GROUP_WINDOW = 12
# Text plausibility for an intervention longer than PLAUSIBILITY_MIN_SECONDS:
# Portuguese plenary speech runs at about 10–12 characters per second.
MIN_CHARS_PER_SECOND = 3.0
PLAUSIBILITY_MIN_SECONDS = 20.0
# Needleman-Wunsch scores: a high mismatch penalty keeps unequal pairs out.
NW_MATCH, NW_MISMATCH, NW_GAP = 2, -100, -1

# Agenda assignment: penalty for moving to the next item, and the bonus that
# ties a "Votações" intervention to the votes item.
AGENDA_SWITCH_PENALTY = 0.5
AGENDA_VOTE_BONUS = 5.0
_VOTE_TYPES = ("votacoes", "votacao")
_AGENDA_STOPWORDS = {
    "para", "pelo", "pela", "pelos", "pelas", "sobre", "como", "entre", "desde",
    "esta", "este", "estas", "estes", "mais", "seus", "suas", "outros", "outras",
    "projeto", "projetos", "proposta", "propostas", "resolucao", "governo",
    "recomenda", "relativo", "relativa", "ambito", "apreciacao", "conjunta",
    "debate", "discussao", "generalidade", "especialidade", "intervencao",
    "deputado", "deputada", "deputados", "pedido", "esclarecimento", "responde",
    "dirige", "apresenta", "encerra", "abre", "pergunta", "legislatura",
}

_WS_RE = re.compile(r"\s+")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return _WS_RE.sub(" ", s.lower().replace("-", " ")).strip()


def _surname(name: str) -> str:
    n = _norm(name)
    return n.split(" ")[-1] if n else ""


def _is_chair_role(role: str) -> bool:
    r = _norm(role)
    return any(r == c or r.startswith(c + " ") for c in _CHAIR_ROLES)


def _av_match_key(intervention: dict) -> str:
    """Join key of an av intervention: the chair key, else the speaker's surname."""
    if _is_chair_role(intervention.get("role") or ""):
        return CHAIR_KEY
    return _surname(intervention.get("speaker") or "")


# --------------------------------------------------------------------------- #
# Speech units
# --------------------------------------------------------------------------- #

@dataclass
class Unit:
    """One speaker's run of turns plus the interjections inside it."""
    key: str
    chair: bool
    turns: list[int] = field(default_factory=list)   # all member turns, in order
    own: list[int] = field(default_factory=list)     # the speaker's own turns
    chars: int = 0                                   # characters of own turns
    page: int = 0
    page_end: int = 0
    run_on_names: set[str] = field(default_factory=set)  # e.g. "filipesousa"
    names: set[str] = field(default_factory=set)         # normalised speaker names

    def add_own(self, index: int, turn: dict) -> None:
        self.turns.append(index)
        self.own.append(index)
        self.chars += int(turn.get("chars") or 0)
        p0, p1 = int(turn.get("page") or 0), int(turn.get("pageEnd") or 0)
        self.page = min(self.page, p0) if self.page else p0
        self.page_end = max(self.page_end, p1)
        # A one-token name is a name whose space the PDF extraction lost
        # ("FilipeSousa"); its surname key is the whole run-on name.
        name = turn.get("nameKey") or ""
        if name and name == turn.get("matchKey"):
            self.run_on_names.add(name)
        if not turn.get("isChair") and turn.get("speaker"):
            self.names.add(_norm(turn["speaker"]))


def _turn_key(turn: dict) -> str:
    return CHAIR_KEY if turn.get("isChair") else (turn.get("matchKey") or "")


def resolve_office_keys(turns: list[dict], interventions: list[dict]) -> None:
    """Give an office holder's turn whose name the Diário never stated the
    surname of the av intervention with the same role (in place)."""
    by_role: dict[str, str] = {}
    for iv in interventions:
        role = _norm(iv.get("role") or "")
        if role and not role.startswith("deputad") and not _is_chair_role(role):
            by_role.setdefault(role, iv.get("speaker") or "")
    for t in turns:
        if t.get("matchKey") or t.get("isChair") or not t.get("role"):
            continue
        name = by_role.get(_norm(t["role"]))
        if name:
            t["matchKey"] = _surname(name)
            t["speaker"] = name


def build_units(turns: list[dict]) -> list[Unit]:
    """Group turns into speech units (see the module docstring)."""
    units: list[Unit] = []
    for ti, t in enumerate(turns):
        key, chair = _turn_key(t), bool(t.get("isChair"))
        last = units[-1] if units else None
        if last and key and last.key == key and last.chair == chair:
            last.add_own(ti, t)
            continue
        if key and not chair:
            target = None
            for k in range(len(units) - 1, max(len(units) - 1 - GROUP_WINDOW, -1), -1):
                u = units[k]
                if u.key == key and not u.chair:
                    target = k
                    break
                if u.chair:
                    break
                if u.chars > APARTE_MAX_CHARS and not t.get("continuation"):
                    break
            if target is not None:
                u = units[target]
                for later in units[target + 1:]:
                    u.turns.extend(later.turns)
                del units[target + 1:]
                u.add_own(ti, t)
                continue
        unit = Unit(key=key, chair=chair)
        unit.add_own(ti, t)
        units.append(unit)
    return units


def _reach(units: list[Unit]) -> list[int]:
    """Characters of each unit plus the same speaker's units that follow it
    across chair remarks and apartes — the most text an intervention starting
    at this unit can end up with."""
    out = []
    for i, u in enumerate(units):
        total = u.chars
        if not u.chair:
            for v in units[i + 1:i + 1 + GROUP_WINDOW]:
                if v.key == u.key and not v.chair:
                    total += v.chars
                elif not (v.chair or v.chars <= APARTE_MAX_CHARS):
                    break
        out.append(total)
    return out


# --------------------------------------------------------------------------- #
# Join
# --------------------------------------------------------------------------- #

@dataclass
class _Av:
    key: str
    chair: bool
    duration: float
    pages: Optional[tuple[int, int]]
    name: str = ""


@dataclass
class _Cand:
    unit: Unit
    reach: int


def _av_desc(iv: dict) -> _Av:
    pages = ((iv.get("od") or {}).get("pages")) or None
    return _Av(key=_av_match_key(iv), chair=_is_chair_role(iv.get("role") or ""),
               duration=float(iv.get("duration") or 0),
               pages=(int(pages[0]), int(pages[1])) if pages else None,
               name=_norm(iv.get("speaker") or ""))


def _names_compatible(a: str, b: str) -> bool:
    """Same person by name, given equal surnames: a first name of one appears
    in the other ("Hugo Lopes Soares" / "Hugo Soares", "Maria Germana Rocha" /
    "Germana Rocha", but not "Pedro Pinto" / "Filipa Pinto"). One-token names
    (a run-on "FilipeSousa") are not checked."""
    ta, tb = a.split(), b.split()
    if len(ta) < 2 or len(tb) < 2:
        return True
    return ta[0] in tb or tb[0] in ta


def _same_speaker(a: _Av, u: Unit) -> bool:
    if a.chair or u.chair:
        return a.chair and u.chair
    if not a.key:
        return False
    if not (u.key == a.key or any(n.endswith(a.key) for n in u.run_on_names)):
        return False
    return not (a.name and u.names) or any(_names_compatible(a.name, n) for n in u.names)


def _compatible(a: _Av, c: _Cand) -> bool:
    if not _same_speaker(a, c.unit):
        return False
    if (not a.chair and a.duration > PLAUSIBILITY_MIN_SECONDS
            and c.reach < MIN_CHARS_PER_SECOND * a.duration):
        return False
    if a.pages and c.unit.page and (c.unit.page_end < a.pages[0] or c.unit.page > a.pages[1]):
        return False
    return True


def align_speeches(interventions: list[dict], units: list[Unit]) -> dict[int, list[int]]:
    """Return ``{av_index: [unit_index, …]}`` (a contiguous run of units)."""
    avs = [_av_desc(iv) for iv in interventions]
    cands = [_Cand(u, r) for u, r in zip(units, _reach(units))]
    pairs = align_equal_keys(avs, cands, match=NW_MATCH, mismatch=NW_MISMATCH,
                             gap=NW_GAP, equal=_compatible)
    spans = {a: [u] for a, u in pairs}
    owner = {u: a for a, u in pairs}

    def claim(a: int, idxs: list[int]) -> None:
        for x in idxs:
            owner[x] = a

    for a in sorted(spans):
        av = avs[a]
        # Forward: the speaker resumes after an unlisted chair remark or apartes.
        cur, buf = spans[a][-1] + 1, []
        while cur < len(units) and cur not in owner and cur - spans[a][-1] <= GROUP_WINDOW:
            v = units[cur]
            if _same_speaker(av, v):
                spans[a].extend(buf + [cur])
                claim(a, buf + [cur])
                buf = []
            elif v.chair or v.chars <= APARTE_MAX_CHARS:
                buf.append(cur)
            else:
                break
            cur += 1
        # Backward: the join landed on a later part of the speech. Only
        # substantial units are pulled back — a short one is the speaker's
        # aparte in the previous speech.
        cur, buf = spans[a][0] - 1, []
        while cur >= 0 and cur not in owner and spans[a][0] - cur <= GROUP_WINDOW:
            v = units[cur]
            if _same_speaker(av, v) and (av.chair or v.chars > APARTE_MAX_CHARS):
                spans[a][:0] = [cur] + buf[::-1]
                claim(a, [cur] + buf)
                buf = []
            elif v.chair or v.chars <= APARTE_MAX_CHARS:
                buf.append(cur)
            else:
                break
            cur -= 1
    return spans


# --------------------------------------------------------------------------- #
# Agenda items
# --------------------------------------------------------------------------- #

def agenda_items(description: str) -> list[str]:
    """The sitting description's items (joined by `` / ``)."""
    return [x.strip() for x in re.split(r"\s+/\s+", description or "") if x.strip()]


def _tokens(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z]{4,}", _norm(text)) if w not in _AGENDA_STOPWORDS}


def _is_vote(iv: dict) -> bool:
    return _norm(iv.get("interventionType") or "") in _VOTE_TYPES


def _evidence(iv: dict) -> str:
    od = iv.get("od") or {}
    return " ".join(x for x in (iv.get("summary"), od.get("debate"), od.get("summary"),
                                od.get("subject")) if x)


def assign_agenda(interventions: list[dict], items: list[str]) -> Optional[list[int]]:
    """Item index per intervention: a monotone path through the items
    maximising the token overlap of each intervention's summary with its item.

    None when there is nothing to decide by — a single item, or no summary
    shares a word with any non-votes item (e.g. the summaries are missing); the
    caller then gives every speech the whole description."""
    n, m = len(interventions), len(items)
    if m <= 1 or n == 0:
        return None
    item_tokens = [_tokens(x) for x in items]
    votes = {k for k, x in enumerate(items) if _norm(x).startswith("votac")}
    emit = []
    evidence = 0.0
    for iv in interventions:
        ev = _tokens(_evidence(iv))
        row = [float(len(ev & item_tokens[k])) for k in range(m)]
        evidence += sum(v for k, v in enumerate(row) if k not in votes)
        if votes and _is_vote(iv):
            for k in votes:
                row[k] += AGENDA_VOTE_BONUS
        emit.append(row)
    if not evidence:
        return None
    best = [emit[0][:]]
    back: list[list[int]] = [[k for k in range(m)]]
    for i in range(1, n):
        row, brow = [], []
        run_k, run_v = 0, best[i - 1][0]          # best predecessor among k' < k
        for k in range(m):
            stay = best[i - 1][k]
            if k > 0:
                if best[i - 1][k - 1] > run_v:
                    run_k, run_v = k - 1, best[i - 1][k - 1]
                move = run_v - AGENDA_SWITCH_PENALTY
            else:
                move = float("-inf")
            if stay > move:     # on a tie, move as late as possible
                row.append(stay + emit[i][k])
                brow.append(k)
            else:
                row.append(move + emit[i][k])
                brow.append(run_k)
        best.append(row)
        back.append(brow)
    k = max(range(m), key=lambda j: (best[-1][j], -j))
    path = [k]
    for i in range(n - 1, 0, -1):
        k = back[i][k]
        path.append(k)
    return path[::-1]


def _agenda_item(intervention: dict, title: str) -> dict:
    title = (title or intervention.get("interventionType") or "Reunião plenária").strip()
    agenda: dict[str, Any] = {"officialTitle": title, "title": title}
    native, core = classify_pt(intervention.get("interventionType"))
    annotate_agenda_item(agenda, native, core)
    return agenda


# --------------------------------------------------------------------------- #
# Stage 2 assembly
# --------------------------------------------------------------------------- #

def _person(intervention: dict) -> dict:
    is_chair = _is_chair_role(intervention.get("role") or "")
    label = (intervention.get("speaker") or "").strip() or "Desconhecido"
    person: dict[str, Any] = {
        "type": "presidencyOfParliament" if is_chair else "memberOfParliament",
        "label": label,
        "context": "president" if is_chair else "main-speaker",
    }
    src_role = (intervention.get("role") or "").strip()
    if src_role and not is_chair and src_role.lower() not in ("deputado", "deputada"):
        person["role"] = src_role
    affiliation = intervention.get("affiliation") or {}
    initials = (affiliation.get("initials") or "").strip()
    name = (affiliation.get("name") or "").strip()
    if initials and not is_chair:
        faction: dict[str, Any] = {"label": initials}
        if name:
            faction["labelAlternative"] = [name]
        person["faction"] = faction
    person_id = ((intervention.get("od") or {}).get("personID") or "").strip()
    if person_id:
        person["originPersonID"] = person_id
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


def _turn_text(turn: dict) -> str:
    return " ".join(s.get("text", "") for s in turn.get("sentences") or []).strip()


def _text_body(turns: list[dict], member: list[int], own: set[int],
               person: dict) -> list[dict]:
    """Blocks for the turns of one speech, consecutive same-kind turns merged."""
    main_status = person["context"]
    blocks: list[dict] = []
    for ti in member:
        t = turns[ti]
        if not t.get("sentences"):
            continue
        if ti in own:
            kind, speaker, status = "speech", person["label"], main_status
            sentences = list(t["sentences"])
        elif t.get("isChair"):
            kind, speaker, status = "speech", t.get("speaker") or "Presidente", "president"
            sentences = list(t["sentences"])
        else:
            kind, speaker, status = "comment", t.get("speaker") or "", "speaker"
            party = f" [{t['party']}]" if t.get("party") else ""
            sentences = [{"text": f"({speaker}{party}: {_turn_text(t)})"}]
        prev = blocks[-1] if blocks else None
        if (prev and kind == "speech" and prev["type"] == "speech"
                and prev["speaker"] == speaker and prev["speakerstatus"] == status):
            prev["sentences"].extend(sentences)
            continue
        blocks.append({"type": kind, "speaker": speaker, "speakerstatus": status,
                       "sentences": sentences})
    return blocks


def _text_contents(body: list[dict]) -> list[dict]:
    if not any(b["type"] == "speech" for b in body):
        return []
    return [{
        "type": "proceedings",
        "language": "pt",
        "creator": PROCEEDINGS_CREATOR,
        "license": PROCEEDINGS_LICENSE,
        "textBody": body,
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

    resolve_office_keys(turns, interventions)
    units = build_units(turns)
    spans = align_speeches(interventions, units) if units else {}
    logger.info(f"[{session}] matched {len(spans)}/{len(interventions)} speeches "
                f"to text ({len(turns)} turns in {len(units)} units)")

    description = (media_meta.get("description") or "").strip()
    items = agenda_items(description)
    item_of = assign_agenda(interventions, items)
    titles = ([items[k] for k in item_of] if item_of is not None
              else [description] * len(interventions))

    leg = int(getattr(args, "period", None) or media_meta.get("legislature") or 17)
    session_number = session_number_int(session)
    session_start = media_meta.get("dateStart") or None

    records: list[dict] = []
    last_date = session_start
    for idx, iv in enumerate(interventions):
        person = _person(iv)
        span = spans.get(idx) or []
        member = [ti for u in span for ti in units[u].turns]
        own = {ti for u in span if _same_speaker(_av_desc(iv), units[u])
               for ti in units[u].own}
        body = _text_body(turns, member, own, person)
        date_start = _date_plus(session_start, iv.get("startOffset")) or last_date
        last_date = date_start
        od = iv.get("od") or {}
        own_turns = [turns[ti]["index"] for ti in member if ti in own]
        debug: dict[str, Any] = {
            "interventionType": iv.get("interventionType") or "",
            "avNumber": iv.get("number"),
            "matchKey": _av_match_key(iv),
            "startOffset": iv.get("startOffset"),
        }
        if own_turns:
            debug["proceedingIndex"] = own_turns[0]
            debug["textTurnIndex"] = own_turns
        if od:
            debug["odID"] = od.get("id")
            if od.get("pages"):
                debug["odPages"] = od["pages"]
        rec: dict[str, Any] = {
            "parliament": PARLIAMENT,
            "electoralPeriod": {"number": leg},
            "session": {"number": session_number, "dateStart": session_start},
            "speechIndex": idx + 1,
            "agendaItem": _agenda_item(iv, titles[idx]),
            "dateStart": date_start,
            "media": _media_block(iv),
            "people": [person],
            "textContents": _text_contents(body),
            "originID": f"{session}-{iv.get('number')}",
            "originalLanguage": "pt",
            "debug": debug,
        }
        records.append(rec)

    now = datetime.datetime.utcnow().isoformat(timespec="seconds")
    for _s in records:
        normalize_speech_originid(_s)
    out_doc = {
        "meta": build_meta(
            PARLIAMENT,
            session=session,
            electoral_period=leg,
            date_start=session_start,
            date_end=last_date,
            last_update=now,
            processing={
                **(media_meta.get("processing") or {}),
                "merge": now,
            },
            extra={"sourceLabel": source_label(session)},
        ),
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
