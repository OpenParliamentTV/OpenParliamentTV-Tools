"""Turn the report XML of an NL sitting into ``original/proceedings/{session}-proceedings.json``.

The report (VLOS schema) nests agenda items (``activiteit``), their parts (``activiteithoofd``) and floor turns (``activiteitdeel`` of the kind ``Spreekbeurt``). Inside a turn every passage of the speaker (``woordvoerder``), every interruption (``interrumpant``) and every inserted record text (``draadboekfragment``) is its own element with start and end clock times.

One speech of the output is one floor turn, with three adjustments that the video makes necessary:

- A turn shorter than ``MIN_SPEECH_SECONDS`` is no speech of its own. It joins the speech before it, or the one after it when nothing precedes it. The order of business and the question hour consist of such turns ("Steun."), and the stream cannot be cut finer than a few seconds.
- A turn longer than ``MAX_SPEECH_SECONDS`` is cut into parts where the speaker takes the floor again after an interruption, so that every part can be aligned.
- A run of voting items has no speaker of its own in the report; it becomes one speech of the chair.

Text the report inserts without anyone saying it (suspensions, references, the wording of an amended motion) is kept as ``comment`` blocks.

Run on its own: ``python -m optv.parliaments.NL.parsers.proceedings2json <data_dir> --session 2026-005``.
"""

import argparse
import datetime
import json
import logging
import math
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from optv.shared.agenda_types import annotate_agenda_item, classify_nl
from optv.shared.cli import load_config, setup_logging
from optv.shared.meta import now_iso
from optv.shared.sentence_split import split_long_sentences

from ..common import in_scope, parse_session_key, session_year_label

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

AMSTERDAM = ZoneInfo("Europe/Amsterdam")

# A turn below this length joins a neighbouring speech.
MIN_SPEECH_SECONDS = 15
# A turn above this length is cut into parts. Below the aligner's default
# limit of 2400 s, with room for the short turns that join a part.
MAX_SPEECH_SECONDS = 1800
# Turns further apart than this are not joined: the time between them is not theirs.
GAP_SECONDS = 30

REPORT_PAGE = "https://www.tweedekamer.nl/kamerstukken/plenaire_verslagen/detail/{session_year}/{number}"

# Agenda item parts that are items of their own on the agenda: one question of
# the question hour, the votes on one debate.
_PARTS_WITH_OWN_TITLE = {"Vraag", "Stemming"}

# Surname particles the report puts behind the name ("Dijk van", "Abassi El").
_PARTICLES = {"van", "de", "den", "der", "het", "'t", "ten", "ter", "te", "in", "op", "el", "al",
              "la", "le", "du", "von"}

_CHAIR_LINE_RE = re.compile(r"^Voorzitter:\s*(.+)$")
_WS_RE = re.compile(r"\s+")


# --------------------------------------------------------------------------- #
# XML access
# --------------------------------------------------------------------------- #

def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child(el, name: str):
    return next((c for c in el if _local(c.tag) == name), None)


def _children(el, name: str) -> list:
    return [c for c in el if _local(c.tag) == name]


def _text(el, name: str) -> str:
    child = _child(el, name) if el is not None else None
    return _WS_RE.sub(" ", "".join(child.itertext())).strip() if child is not None else ""


def _time(el, *names: str) -> Optional[datetime.datetime]:
    """The first of the named child times, as a time in the Netherlands. The report writes local times without a zone."""
    for name in names:
        value = _text(el, name)
        if value:
            try:
                return datetime.datetime.fromisoformat(value).replace(tzinfo=AMSTERDAM)
            except ValueError:
                continue
    return None


def _iso(moment: Optional[datetime.datetime]) -> Optional[str]:
    return moment.isoformat(timespec="seconds") if moment else None


# --------------------------------------------------------------------------- #
# Speakers
# --------------------------------------------------------------------------- #

def natural_surname(inverted: str) -> str:
    """``"Dijk van"`` → ``"van Dijk"``: the report keeps a surname's particles behind it."""
    words = inverted.split()
    particles: list[str] = []
    while len(words) > 1 and words[-1].lower() in _PARTICLES:
        particles.insert(0, words.pop())
    return " ".join(particles + words)


def person_from(spreker) -> dict:
    """A Stage 2 person from a ``spreker`` element."""
    first = _text(spreker, "voornaam")
    last = natural_surname(_text(spreker, "achternaam")) or _text(spreker, "verslagnaam")
    person = {
        "type": "memberOfParliament" if spreker.get("soort") == "Tweede Kamerlid" else "guest",
        "label": f"{first} {last}".strip() or _text(spreker, "verslagnaam"),
    }
    if first:
        person["firstname"] = first
    if last:
        person["lastname"] = last
    role = _text(spreker, "functie")
    if role:
        person["role"] = role
    faction = _text(spreker, "fractie")
    if faction:
        person["faction"] = {"label": faction}
    if spreker.get("objectid"):
        person["originPersonID"] = spreker.get("objectid")
    return person


ANONYMOUS_CHAIR = {"type": "memberOfParliament", "label": "De voorzitter", "role": "voorzitter"}


# --------------------------------------------------------------------------- #
# Text
# --------------------------------------------------------------------------- #

def _item_text(item) -> str:
    return _WS_RE.sub(" ", "".join(item.itertext())).strip()


def _is_speaker_line(item) -> bool:
    """The line a contribution opens with: the speaker's name in bold and a colon ("Mevrouw **Abdi** (PRO):")."""
    bold = any(_local(c.tag) == "nadruk" and c.get("type") == "Vet" for c in item)
    return bold and _item_text(item).endswith(":")


def _paragraphs(container, *, strip_speaker_line: bool = False) -> tuple[list[str], bool]:
    """The paragraphs directly in ``container`` and whether a leading speaker line was removed.

    A paragraph is an ``alinea``; its lines (``alineaitem``, also inside a list) are joined with spaces.
    """
    paragraphs: list[str] = []
    stripped = False
    first = True
    for alinea in _children(container, "alinea"):
        items = [el for el in alinea.iter() if _local(el.tag) == "alineaitem"]
        if first and strip_speaker_line and items and _is_speaker_line(items[0]):
            items = items[1:]
            stripped = True
        first = False
        text = " ".join(t for t in (_item_text(i) for i in items) if t)
        if text:
            paragraphs.append(text)
    return paragraphs, stripped


@dataclass
class Block:
    """The text of one report element: a passage, an interruption, an inserted record text."""
    kind: str                                   # "speech" or "comment"
    paragraphs: list[str]
    speaker: Optional[dict] = None
    chair: bool = False
    start: Optional[datetime.datetime] = None
    origin_id: str = ""
    resumes: bool = False                       # the turn's speaker takes the floor again here

    @property
    def spoken(self) -> bool:
        return self.kind == "speech" and self.speaker is not None


@dataclass
class Turn:
    """What the report holds together as one unit: a floor turn, a run of votes, an announcement of the chair."""
    kind: str                                   # "speech", "voting" or "announcement"
    origin_id: str
    start: datetime.datetime
    end: datetime.datetime
    speaker: Optional[dict]
    chair: bool
    agenda: dict
    activity_id: str
    blocks: list[Block] = field(default_factory=list)
    part: Optional[tuple[int, int]] = None      # (n, of) for a part of a turn that was cut

    @property
    def seconds(self) -> float:
        return (self.end - self.start).total_seconds()


class _State:
    """What carries over from one element to the next while a report is read."""

    def __init__(self, root):
        self.chair: Optional[dict] = None
        # verslagnaam -> person, for "Voorzitter: Van Campen" lines.
        self.by_report_name: dict[str, dict] = {}
        for el in root.iter():
            if _local(el.tag) == "spreker":
                self.by_report_name.setdefault(_text(el, "verslagnaam"), person_from(el))

    def note_chair_line(self, paragraphs: list[str]) -> None:
        for paragraph in paragraphs:
            m = _CHAIR_LINE_RE.match(paragraph)
            if m and m.group(1).strip() in self.by_report_name:
                self.chair = self.by_report_name[m.group(1).strip()]


def _fragment_blocks(fragment, speaker: Optional[dict], state: _State) -> list[Block]:
    """The blocks of an inserted record text (``draadboekfragment``).

    Spoken: the wording of a motion its mover reads out, and what the chair says (the report marks it with a speaker line). Everything else is record text.
    """
    tekst = _child(fragment, "tekst")
    if tekst is None:
        return []
    start = _time(fragment, "markeertijdbegin")
    origin_id = fragment.get("objectid") or ""
    blocks: list[Block] = []
    plain, by_chair = _paragraphs(tekst, strip_speaker_line=True)
    if plain:
        state.note_chair_line(plain)
        if by_chair:
            blocks.append(Block("speech", plain, state.chair or ANONYMOUS_CHAIR, True, start, origin_id))
        else:
            blocks.append(Block("comment", plain, start=start, origin_id=origin_id))
    for group in _children(tekst, "alineagroep"):
        paragraphs, _ = _paragraphs(group)
        if not paragraphs:
            continue
        read_out = (group.get("type") == "Motietekst" and fragment.get("soort") == "Motie indiening"
                    and speaker is not None)
        if read_out:
            blocks.append(Block("speech", paragraphs, speaker, False, start, origin_id))
        else:
            blocks.append(Block("comment", paragraphs, start=start, origin_id=origin_id))
    return blocks


def _turn_blocks(deel, speaker: Optional[dict], state: _State) -> tuple[list[Block], bool]:
    """The blocks of a floor turn in spoken order, and whether its speaker is in the chair."""
    blocks: list[Block] = []
    speaker_in_chair = False

    def visit(el, who: Optional[dict], chair: bool, passage_of_speaker: bool) -> None:
        for child in el:
            name = _local(child.tag)
            if name == "tekst":
                paragraphs, _ = _paragraphs(child, strip_speaker_line=True)
                for group in _children(child, "alineagroep"):
                    paragraphs += _paragraphs(group)[0]
                if paragraphs:
                    blocks.append(Block("speech", paragraphs, who, chair,
                                        _time(el, "markeertijdbegin"), el.get("objectid") or "",
                                        resumes=passage_of_speaker))
            elif name in ("woordvoerder", "interrumpant"):
                spreker = _child(child, "spreker")
                person = person_from(spreker) if spreker is not None else who
                in_chair = _text(child, "isvoorzitter") == "true"
                if in_chair and person is not None:
                    state.chair = person
                visit(child, person, in_chair, name == "woordvoerder")
            elif name == "draadboekfragment":
                blocks.extend(_fragment_blocks(child, who, state))
            elif name == "activiteititem":
                visit(child, who, chair, False)

    visit(deel, speaker, False, False)
    if speaker is not None:
        own = [b for b in blocks if b.speaker is not None
               and b.speaker.get("originPersonID") == speaker.get("originPersonID")]
        speaker_in_chair = bool(own) and all(b.chair for b in own)
    return blocks, speaker_in_chair


def _voting_blocks(deel, state: _State) -> list[Block]:
    """One voting item: what is put to the vote (record text) and the chair's finding."""
    blocks: list[Block] = []
    start = _time(deel, "markeertijdbegin")
    tekst = _child(deel, "tekst")
    if tekst is not None:
        paragraphs, _ = _paragraphs(tekst)
        if paragraphs:
            blocks.append(Block("comment", paragraphs, start=start, origin_id=deel.get("objectid") or ""))
    for item in _children(deel, "activiteititem"):
        tekst = _child(item, "tekst")
        if tekst is None:
            continue
        paragraphs, _ = _paragraphs(tekst, strip_speaker_line=True)
        if paragraphs:
            blocks.append(Block("speech", paragraphs, state.chair or ANONYMOUS_CHAIR, True,
                                _time(item, "markeertijdbegin") or start, item.get("objectid") or ""))
    return blocks


# --------------------------------------------------------------------------- #
# Agenda
# --------------------------------------------------------------------------- #

def _agenda(activity, part=None) -> dict:
    """The agenda item of a turn: the activity, or the activity's part where parts are agenda items of their own."""
    source = part if part is not None and part.get("soort") in _PARTS_WITH_OWN_TITLE else activity
    title = _text(source, "titel") or _text(activity, "titel") or activity.get("soort") or ""
    official = _text(source, "onderwerp") or title
    if source is not activity and official.lower() not in title.lower():
        # "Vragen Wilders" and the subject of the question; the title of a
        # voting part already names its subject.
        official = f"{title}: {official}"
    elif source is not activity:
        official = title
    item = {"officialTitle": official, "title": title, "id": source.get("objectid") or ""}
    native, core = classify_nl(activity.get("soort"), _text(activity, "titel"))
    annotate_agenda_item(item, native, core)
    return item


# --------------------------------------------------------------------------- #
# From report elements to turns
# --------------------------------------------------------------------------- #

BREAK = object()


def _activity_sequence(activity, state: _State) -> list:
    """The turns of an agenda item in order, with the record texts between them (``Block``) and the suspensions (``BREAK``)."""
    sequence: list = []
    activity_id = activity.get("objectid") or ""

    def walk(container, part) -> None:
        run: Optional[Turn] = None       # the voting items collected so far

        def close_run() -> None:
            nonlocal run
            if run is not None and run.blocks:
                sequence.append(run)
            run = None

        for child in container:
            name = _local(child.tag)
            if name == "tekst":
                # The heading the report gives an agenda item or a part of it,
                # without the lines that only repeat its title.
                paragraphs, _ = _paragraphs(child)
                for group in _children(child, "alineagroep"):
                    paragraphs += _paragraphs(group)[0]
                state.note_chair_line(paragraphs)
                titles = {_text(container, "titel"), _text(activity, "titel")}
                paragraphs = [p for p in paragraphs if p not in titles]
                if paragraphs:
                    sequence.append(Block("comment", paragraphs, origin_id="heading"))
            elif name == "activiteithoofd":
                close_run()
                walk(child, child)
            elif name == "activiteitdeel":
                start = _time(child, "markeertijdbegin")
                end = _time(child, "markeertijdeind") or start
                if start is None:
                    logger.warning(f"turn {child.get('objectid')} has no start time, skipped")
                    continue
                if child.get("soort") == "Stemming item":
                    if run is None:
                        run = Turn("voting", child.get("objectid") or "", start, end,
                                   state.chair or ANONYMOUS_CHAIR, True, _agenda(activity, part), activity_id)
                    run.blocks.extend(_voting_blocks(child, state))
                    run.end = max(run.end, end)
                    continue
                close_run()
                spreker = _child(child, "spreker")
                speaker = person_from(spreker) if spreker is not None else None
                blocks, in_chair = _turn_blocks(child, speaker, state)
                sequence.append(Turn("speech", child.get("objectid") or "", start, max(end, start),
                                     speaker, in_chair, _agenda(activity, part), activity_id, blocks))
            elif name == "draadboekfragment":
                blocks = _fragment_blocks(child, None, state)
                spoken = [b for b in blocks if b.kind == "speech"]
                if run is not None:
                    run.blocks.extend(blocks)
                elif spoken:
                    start = _time(child, "markeertijdbegin")
                    end = _time(child, "markeertijdeind") or start
                    if start is not None:
                        sequence.append(Turn("announcement", child.get("objectid") or "", start, max(end, start),
                                             state.chair or ANONYMOUS_CHAIR, True,
                                             _agenda(activity, part), activity_id, blocks))
                else:
                    sequence.extend(blocks)
                if child.get("soort") == "Schorsing":
                    close_run()
                    sequence.append(BREAK)
        close_run()

    walk(activity, None)
    return sequence


def split_long_turn(turn: Turn, max_seconds: float = MAX_SPEECH_SECONDS) -> list[Turn]:
    """Cut a turn longer than ``max_seconds`` into parts of about equal length, at points where its speaker resumes."""
    if turn.kind != "speech" or turn.seconds <= max_seconds:
        return [turn]
    wanted = math.ceil(turn.seconds / max_seconds)
    own = _person_key(turn.speaker) if turn.speaker else None
    cuts = [i for i, b in enumerate(turn.blocks)
            if i > 0 and b.resumes and b.start is not None
            and b.speaker is not None and _person_key(b.speaker) == own]
    chosen: list[int] = []
    for n in range(1, wanted):
        target = turn.start + datetime.timedelta(seconds=turn.seconds * n / wanted)
        later = [i for i in cuts if not chosen or i > chosen[-1]]
        if not later:
            break
        chosen.append(min(later, key=lambda i: abs((turn.blocks[i].start - target).total_seconds())))
    if not chosen:
        return [turn]
    bounds = [0, *chosen, len(turn.blocks)]
    parts = []
    for n in range(len(bounds) - 1):
        blocks = turn.blocks[bounds[n]:bounds[n + 1]]
        start = turn.start if n == 0 else blocks[0].start
        end = turn.end if n == len(bounds) - 2 else turn.blocks[bounds[n + 1]].start
        parts.append(Turn(turn.kind, turn.origin_id if n == 0 else blocks[0].origin_id, start, end,
                          turn.speaker, turn.chair, turn.agenda, turn.activity_id, blocks,
                          part=(n + 1, len(bounds) - 1)))
    return parts


@dataclass
class Speech:
    """Turns that go into one Stage 2 speech; ``lead`` is the turn that gives it its speaker."""
    lead: Turn
    turns: list[Turn]
    comments_before: list[Block] = field(default_factory=list)

    @property
    def start(self) -> datetime.datetime:
        return self.turns[0].start

    @property
    def end(self) -> datetime.datetime:
        return max(t.end for t in self.turns)

    def blocks(self) -> list[Block]:
        return [*self.comments_before, *(b for t in self.turns for b in t.blocks)]


def group_turns(sequence: list, *, min_seconds: float = MIN_SPEECH_SECONDS,
                max_seconds: float = MAX_SPEECH_SECONDS, gap_seconds: float = GAP_SECONDS) -> list[Speech]:
    """The speeches of one agenda item.

    A turn of at least ``min_seconds`` starts a speech. Shorter turns join the speech before them; where none precedes, they wait for the next one and open it. A suspension, a pause longer than ``gap_seconds`` and a change of agenda item end the joining.
    """
    speeches: list[Speech] = []
    current: Optional[Speech] = None
    waiting: list[Turn] = []             # short turns with no speech before them
    headings: list[Block] = []           # record text for the speech that comes next

    def start_speech(lead: Turn, turns: list[Turn]) -> Speech:
        nonlocal headings
        speech = Speech(lead, turns, headings)
        headings = []
        speeches.append(speech)
        return speech

    def flush_waiting() -> None:
        nonlocal waiting
        if waiting:
            start_speech(max(waiting, key=lambda t: t.seconds), waiting)
            waiting = []

    for item in sequence:
        if item is BREAK:
            flush_waiting()
            current = None
            continue
        if isinstance(item, Block):
            if item.origin_id == "heading" or current is None:
                if waiting and item.origin_id != "heading":
                    waiting[-1].blocks.append(item)
                else:
                    headings.append(item)
            else:
                current.turns[-1].blocks.append(item)
            continue
        turn: Turn = item
        last = waiting[-1] if waiting else (current.turns[-1] if current else None)
        if last is not None and (turn.agenda["id"] != last.agenda["id"]
                                 or (turn.start - last.end).total_seconds() > gap_seconds):
            flush_waiting()
            current = None
        if turn.seconds >= min_seconds:
            current = start_speech(turn, [*waiting, turn])
            waiting = []
        elif current is not None and (turn.end - current.start).total_seconds() <= max_seconds:
            current.turns.append(turn)
        else:
            current = None
            waiting.append(turn)
    flush_waiting()
    if headings and speeches:
        # A heading after the last turn (the closing time of the sitting).
        speeches[-1].turns[-1].blocks.extend(headings)
    return speeches


# --------------------------------------------------------------------------- #
# Stage 2 shape
# --------------------------------------------------------------------------- #

def _sentencize(nlp, paragraphs: list[str]) -> list[list[str]]:
    result = []
    for doc in nlp.pipe(paragraphs):
        sentences = [s.text.strip() for s in doc.sents if s.text.strip()]
        result.append(split_long_sentences(sentences) or [doc.text.strip()])
    return result


def _person_key(person: dict) -> str:
    return person.get("originPersonID") or person["label"]


def speech_record(speech: Speech, index: int, nlp) -> dict:
    """One speech in the shape the merger completes: people, text blocks, times and the ids it is joined by."""
    lead = speech.lead
    main = lead.speaker or ANONYMOUS_CHAIR
    main_key = _person_key(main)
    main_status = "president" if lead.chair else "main-speaker"

    people: dict[str, dict] = {main_key: {**main, "context": main_status}}
    body: list[dict] = []
    blocks = speech.blocks()
    for block, sentences in zip(blocks, _split_blocks(nlp, blocks)):
        if not block.spoken:
            speaker_label, status = None, None
        else:
            key = _person_key(block.speaker)
            if key == main_key:
                status = main_status
            else:
                status = "president" if block.chair else "speaker"
                people.setdefault(key, {**block.speaker, "context": status})
            speaker_label = block.speaker["label"]
        for paragraph_sentences in sentences:
            body.append({
                "type": "speech" if block.spoken else "comment",
                "speaker": speaker_label,
                "speakerstatus": status,
                "sentences": [{"text": s} for s in paragraph_sentences],
            })

    debug = {"turns": len(speech.turns), "kind": lead.kind}
    if lead.part:
        debug["part"] = f"{lead.part[0]}/{lead.part[1]}"
    return {
        "speechIndex": index,
        "originTextID": lead.origin_id,
        "activityId": lead.activity_id,
        "agendaItem": dict(lead.agenda),
        "dateStart": _iso(speech.start),
        "dateEnd": _iso(speech.end),
        "people": list(people.values()),
        "textBody": body,
        "debug": debug,
    }


def _split_blocks(nlp, blocks: list[Block]) -> list[list[list[str]]]:
    """The sentences of every paragraph of every block, split in one pass over the model.

    A paragraph of record text stays one sentence: the aligner gives a comment block the time between its neighbours, and it does so for the block's first sentence only.
    """
    spoken = [p for b in blocks if b.spoken for p in b.paragraphs]
    sentences = iter(_sentencize(nlp, spoken))
    return [[next(sentences) if b.spoken else [p] for p in b.paragraphs] for b in blocks]


def load_nlp(spacy_model: str):
    """The sentence splitter: the Dutch model without the parts sentence boundaries do not need."""
    import spacy
    logger.info(f"Loading spaCy model {spacy_model}")
    return spacy.load(spacy_model, exclude=["ner", "lemmatizer"])


def parse_report(xml: bytes, session: str, nlp) -> dict:
    """The proceedings document of one sitting from its report XML."""
    root = ET.fromstring(xml)
    sitting = next((el for el in root.iter() if _local(el.tag) == "vergadering"), None)
    if sitting is None:
        raise ValueError(f"[{session}] the report has no vergadering element")
    state = _State(root)

    speeches: list[Speech] = []
    turns = 0
    for activity in _children(sitting, "activiteit"):
        sequence = []
        for item in _activity_sequence(activity, state):
            if isinstance(item, Turn):
                turns += 1
                sequence.extend(split_long_turn(item))
            else:
                sequence.append(item)
        speeches.extend(group_turns(sequence))

    records = [speech_record(s, i, nlp) for i, s in enumerate(speeches, start=1)]
    year, number = parse_session_key(session)
    start = _time(sitting, "aanvangstijd")
    end = _time(sitting, "sluiting")
    if records:
        end = end or datetime.datetime.fromisoformat(records[-1]["dateEnd"])
    logger.info(f"[{session}] {turns} turns in the report, {len(records)} speeches")
    return {
        "meta": {
            "session": session,
            "sourceLabel": f"Handelingen {session_year_label(year)}, nr. {number}",
            "sourceURI": REPORT_PAGE.format(session_year=session_year_label(year), number=number),
            "report": {"kind": root.get("soort"), "status": root.get("status"), "timestamp": root.get("Timestamp")},
            "dateStart": _iso(start),
            "dateEnd": _iso(end),
            "processing": {"parse_proceedings": now_iso()},
        },
        "data": records,
    }


def parse_session(config, session: str, nlp) -> Path:
    doc = parse_report(config.raw_report(session).read_bytes(), session, nlp)
    out = config.file(session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def parse_proceedings(config, args) -> None:
    """Parse the report of every session in scope whose parsed file is missing or older than the report."""
    nlp = None
    for session in config.sessions():
        raw = config.raw_report(session)
        out = config.file(session, "proceedings")
        if not in_scope(args, session) or not raw.exists():
            continue
        if out.exists() and out.stat().st_mtime >= raw.stat().st_mtime and not getattr(args, "force", False):
            continue
        if nlp is None:
            from optv.parliaments import get_locale
            nlp = load_nlp(get_locale(PARLIAMENT_ID)["spacy_model"])
        parse_session(config, session, nlp)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", action="append", default=[], help="session key, e.g. 2026-005 (repeatable); default: all")
    parser.add_argument("--force", action="store_true", help="parse again what is already parsed")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    args.limit_to_period = False
    args.limit_session = "^(" + "|".join(re.escape(s) for s in args.session) + ")$" if args.session else ""
    parse_proceedings(load_config(PARLIAMENT_ID, args.data_dir), args)


if __name__ == "__main__":
    main()
