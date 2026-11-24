"""Merge the WebTV page and the stenographic record of one IT sitting into Stage 2.

The WebTV page is the spine: each intervention it lists becomes one speech, addressed as a time range of the sitting's recording (``…mp4#t=start,end``) and linked to the page that opens the player at it.

The text is joined by id. The page names, for most of its interventions, the record intervention each one starts with; those are the anchors. The record has more interventions than the page, because it also sets apart what the chair says in between (``Concluda.``, giving the floor, a vote) and what a speaker says after being interrupted. They lie between two anchors in the record and between the same two in the recording, so they are distributed over the page's interventions there in order: one the page does not anchor opens with the first fitting intervention of its own speaker, and of the possible distributions the one that puts the least text on a speech of another speaker is taken. Nothing is matched by content.

A page intervention that receives no spoken text stays a clip without text (the reading of the minutes, which the record only mentions). A suspension of the sitting is no speech and is left out. A speech whose text does not fit its time range is marked with ``debug.confidence`` below 1.

Run for one session: ``python -m optv.parliaments.IT.merger.merge_session <session> <data_dir>``.
"""

import datetime
import json
import logging
import re
from pathlib import Path
from typing import Optional

from optv.parliaments import get_language, get_rights
from optv.shared.agenda_types import annotate_agenda_item, classify_it
from optv.shared.cli import session_cli
from optv.shared.meta import build_meta, fill_original_language, now_iso
from optv.shared.publish import save_if_changed
from optv.shared.speech_id import normalize_speech_originid

from ..common import parse_session_key
from ..parsers.names import person, person_key

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

# Leaving out an intervention of the page that has a record intervention of its
# own speaker to open with costs more than any amount of misplaced text.
_UNOPENED = 10 ** 9

# The roles of the page that are the chair's, and what the others say about a person.
_CHAIR_ROLES = {"presidente": "president", "vice presidente": "vice-president", "vicepresidente": "vice-president"}
_MEMBER_ROLE_RE = re.compile(r"^(deputat[oa]|presidente|vice ?presidente|segretari[oa]|questore)$", re.I)

# A title under which the sitting is suspended.
_SUSPENSION_RE = re.compile(r"\bsospes[ao]\b", re.I)

# Text that cannot have been spoken in the time, and a long time with almost no text.
MAX_CHARS_PER_SECOND = 30
MIN_CHARS_FOR_RATE = 400
MIN_CHARS_PER_SECOND = 2
MIN_SECONDS_FOR_RATE = 120


# --------------------------------------------------------------------------- #
# Distributing the record over the page
# --------------------------------------------------------------------------- #

def same_person(entry: dict, intervention: dict) -> bool:
    """Whether a page intervention and a record intervention are of one speaker: by the Chamber's person id, else by name."""
    speaker = intervention.get("speaker") or {}
    if entry.get("personId") and speaker.get("personId"):
        return entry["personId"] == speaker["personId"]
    return bool(entry.get("name")) and person_key(entry["name"]) == person_key(speaker.get("name") or "")


def find_anchors(entries: list[dict], interventions: list[dict]) -> list[tuple[int, int]]:
    """``(entry index, intervention index)`` for every page intervention that names its record intervention, in rising order of both."""
    position = {item["id"]: n for n, item in enumerate(interventions)}
    anchors: list[tuple[int, int]] = []
    last = -1
    for index, entry in enumerate(entries):
        at = position.get(entry.get("recordId"))
        if at is None:
            continue
        if at <= last:
            logger.warning(f"Entry {entry['id']} points back to {entry['recordId']}, its link is ignored")
            continue
        anchors.append((index, at))
        last = at
    return anchors


def _spread(head: Optional[dict], others: list[dict], items: list[dict]) -> list[list[int]]:
    """Distribute ``items`` in order over ``head`` and the ``others`` after it.

    Returns one list of item indexes for the head and one per other entry. The head takes what precedes the first entry that opens; without a head that text has no clip. An entry opens only with an item of its own speaker.
    """
    slots = [head, *others]
    count = len(items)
    can_open = [False] + [any(same_person(entry, item) for item in items) for entry in others]

    def misplaced(slot: int, item: dict) -> int:
        entry = slots[slot]
        if entry is not None and same_person(entry, item):
            return 0
        return item["chars"] + (1 if entry is None else 0)

    infinity = float("inf")
    cost = [[infinity] * len(slots) for _ in range(count + 1)]
    came_from: list[list[Optional[int]]] = [[None] * len(slots) for _ in range(count + 1)]
    cost[0][0] = 0
    for n, item in enumerate(items):
        for slot in range(len(slots)):
            here = cost[n][slot]
            if here == infinity:
                continue
            stay = here + misplaced(slot, item)
            if stay < cost[n + 1][slot]:
                cost[n + 1][slot], came_from[n + 1][slot] = stay, slot
            skipped = 0
            for later in range(slot + 1, len(slots)):
                if same_person(slots[later], item) and here + skipped < cost[n + 1][later]:
                    cost[n + 1][later], came_from[n + 1][later] = here + skipped, slot
                skipped += _UNOPENED if can_open[later] else 0

    def total(slot: int) -> float:
        return cost[count][slot] + sum(_UNOPENED for later in range(slot + 1, len(slots)) if can_open[later])

    slot = min(range(len(slots)), key=total)
    placed: list[list[int]] = [[] for _ in slots]
    for n in range(count, 0, -1):
        placed[slot].append(n - 1)
        slot = came_from[n][slot]
    return [sorted(indexes) for indexes in placed]


def distribute(entries: list[dict], interventions: list[dict]) -> tuple[list[list[int]], list[int]]:
    """The indexes of the record interventions each page intervention carries, and those no page intervention covers."""
    anchors = find_anchors(entries, interventions)
    carried: list[list[int]] = [[] for _ in entries]
    uncovered: list[int] = []
    bounds = [(-1, -1), *anchors, (len(entries), len(interventions))]
    for (entry_at, item_at), (next_entry, next_item) in zip(bounds, bounds[1:]):
        head = entries[entry_at] if entry_at >= 0 else None
        others = entries[entry_at + 1:next_entry]
        items = interventions[item_at + 1:next_item]
        placed = _spread(head, others, items)
        offset = item_at + 1
        if head is not None:
            carried[entry_at] = [item_at, *(offset + n for n in placed[0])]
        else:
            uncovered.extend(offset + n for n in placed[0])
        for n, indexes in enumerate(placed[1:]):
            carried[entry_at + 1 + n] = [offset + i for i in indexes]
    return carried, uncovered


# --------------------------------------------------------------------------- #
# Stage 2 shape
# --------------------------------------------------------------------------- #

def chair_context(role: Optional[str]) -> Optional[str]:
    """``president`` or ``vice-president`` for a role of the chair, ``None`` for any other role."""
    return _CHAIR_ROLES.get(" ".join((role or "").lower().split()))


def entry_person(entry: dict) -> dict:
    """The speaker of a page intervention."""
    record = person(entry.get("name") or "", person_id=entry.get("personId"),
                    role=entry.get("role"), group=entry.get("group"))
    if not record["label"]:
        record["label"] = entry.get("role") or "Unknown"
    if _MEMBER_ROLE_RE.match((entry.get("role") or "").strip()):
        record = {"type": "memberOfParliament", **record}
    return record


def same_identity(a: dict, b: dict) -> bool:
    """Whether two Stage 2 persons are one: by the Chamber's person id where both have one, else by name."""
    if a.get("originPersonID") and b.get("originPersonID"):
        return a["originPersonID"] == b["originPersonID"]
    return a["label"] == b["label"]


def _find(people: list[dict], wanted: dict) -> Optional[dict]:
    return next((p for p in people if same_identity(p, wanted)), None)


def agenda_item(entry: dict, titles: dict[str, dict]) -> tuple[dict, Optional[str]]:
    """The agenda item of a page intervention and the phase it belongs to.

    The page's titles form a tree of up to three levels: the item of the order of business, its subject (the bill, the single interpellation) and the phase. The subject, where there is one, is the agenda item, under the item's title as official title.
    """
    chain: list[tuple[str, str]] = []
    title_id = entry.get("titleId")
    while title_id and title_id in titles and len(chain) < 6:
        chain.append((title_id, titles[title_id]["title"]))
        title_id = titles[title_id].get("parent")
    chain.reverse()
    if not chain:
        item = {"id": "senza-titolo", "officialTitle": "(senza titolo)", "title": "(senza titolo)"}
        annotate_agenda_item(item, *classify_it(None))
        return item, None
    root_id, root = chain[0]
    subject_id, subject = chain[1] if len(chain) > 1 else (root_id, root)
    phase = chain[-1][1] if len(chain) > 2 else None
    item = {"id": subject_id, "officialTitle": root, "title": subject}
    annotate_agenda_item(item, *classify_it(root, phase, subject if len(chain) > 1 else None))
    return item, phase


def text_body(items: list[dict], main: dict, main_status: str, known: list[dict]) -> tuple[list[dict], list[dict], int, int]:
    """The text blocks of a speech, the people who speak in it besides the main speaker, and the count of spoken characters, in all and of the main speaker."""
    others: list[dict] = []
    body: list[dict] = []
    chars = own_chars = 0

    def comment(text: str) -> None:
        body.append({"type": "comment", "speaker": None, "speakerstatus": None, "sentences": [{"text": text}]})

    for item in items:
        speaker = item.get("speaker") or {}
        speaking = person(speaker.get("name") or "", person_id=speaker.get("personId"),
                          role=speaker.get("role"), group=speaker.get("group"))
        speaking = (_find(known, speaking) or speaking) if speaking["label"] else None
        own = speaking is not None and same_identity(speaking, main)
        if speaking is None:
            label, status = None, None
        elif own:
            label, status = main["label"], main_status
        else:
            status = speaker.get("chairContext") if speaker.get("chair") else "speaker"
            label = speaking["label"]
            if _find(others, speaking) is None:
                others.append({**speaking, "context": status})
        for note in item.get("notesBefore") or []:
            comment(note)
        for block in item["blocks"]:
            if block["kind"] == "speech":
                body.append({"type": "speech", "speaker": label, "speakerstatus": status,
                             "sentences": [{"text": s} for s in block["sentences"]]})
            else:
                for sentence in block["sentences"]:
                    comment(sentence)
        for note in item.get("notesAfter") or []:
            comment(note)
        chars += item["chars"]
        if own:
            own_chars += item["chars"]
    return body, others, chars, own_chars


def text_confidence(chars: int, own_chars: int, seconds: Optional[float]) -> tuple[float, Optional[str]]:
    """``(confidence, reason)``: below 1 when none of the text is the speaker's own (the record only mentions the reading of the minutes), when the text is more than could be spoken in the time, or when a long time has almost no text."""
    if not own_chars:
        return 0.5, "no-own-text"
    if not seconds or seconds <= 0:
        return 1.0, None
    rate = chars / seconds
    if chars >= MIN_CHARS_FOR_RATE and rate > MAX_CHARS_PER_SECOND:
        return 0.5, "text-too-long"
    if seconds >= MIN_SECONDS_FOR_RATE and rate < MIN_CHARS_PER_SECOND:
        return 0.5, "text-too-short"
    return 1.0, None


def _moment(value: Optional[str]) -> Optional[datetime.datetime]:
    return datetime.datetime.fromisoformat(value) if value else None


def _iso(moment: datetime.datetime) -> str:
    return moment.isoformat(timespec="seconds")


def _offset(seconds: float) -> str:
    return f"{int(seconds)}" if float(seconds).is_integer() else f"{seconds:g}"


def speech_record(entry: dict, items: list[dict], first_item: Optional[int], *, media_meta: dict,
                  record_page: str, period: int, number: int, known: list[dict],
                  session_dates: tuple[str, str]) -> Optional[dict]:
    """One Stage 2 speech from a page intervention and the record interventions it carries, ``None`` for an entry that is no speech."""
    parts = {p["id"]: p["url"] for p in media_meta.get("parts") or []}
    video = parts.get(entry["part"])
    start_clock = _moment(entry.get("clock"))
    if not video or start_clock is None:
        return None
    start, end = entry["start"], entry.get("end")
    if end is None:
        # The last intervention runs to the end of the sitting.
        left = (_moment(session_dates[1]) - start_clock).total_seconds()
        end = start + int(left) if left > 0 else None
    seconds = float(end - start) if end is not None and end > start else None

    agenda, phase = agenda_item(entry, media_meta.get("titles") or {})
    main = entry_person(entry)
    group = (_find(known, main) or {}).get("faction")
    if group and "faction" not in main:
        # The page names no group where a member speaks in an office (as secretary, as rapporteur).
        main["faction"] = dict(group)
    main_status = chair_context(entry.get("role")) or "main-speaker"
    body, others, chars, own_chars = text_body(items, main, main_status, known)
    if not chars:
        if _SUSPENSION_RE.search(agenda["officialTitle"]):
            return None
        body, others = [], []

    media_rights = get_rights(PARLIAMENT_ID, period, "media")
    text_rights = get_rights(PARLIAMENT_ID, period, "proceedings")
    media = {
        "videoFileURI": f"{video}#t={_offset(start)}" + (f",{_offset(end)}" if seconds else ""),
        "sourcePage": f"{media_meta['sourcePage']}/{entry['id']}",
        "originMediaID": entry["id"],
        "aligned": False,
        "creator": media_rights.get("creator", ""),
        "license": media_rights.get("license", ""),
        "additionalInformation": {"startOffset": start},
    }
    if seconds:
        media["duration"] = seconds
        media["additionalInformation"]["endOffset"] = end

    debug: dict = {"anchored": bool(entry.get("recordId")) and bool(items) and items[0]["id"] == entry["recordId"]}
    if phase:
        debug["phase"] = phase
    text_contents = []
    if body:
        text_contents.append({
            "type": "proceedings",
            "language": get_language(PARLIAMENT_ID),
            "originTextID": items[0]["id"],
            "sourceURI": f"{record_page}#sed{number:04d}.stenografico.{items[0]['id']}",
            "creator": text_rights.get("creator", ""),
            "license": text_rights.get("license", ""),
            "textBody": body,
        })
        confidence, reason = text_confidence(chars, own_chars, seconds)
        debug.update({
            "proceedingIndex": first_item,
            "recordInterventions": [item["id"] for item in items],
            "confidence": confidence,
        })
        if reason:
            debug["confidenceReason"] = reason

    return {
        "parliament": PARLIAMENT_ID,
        "electoralPeriod": {"number": period},
        "session": {"number": number, "dateStart": session_dates[0], "dateEnd": session_dates[1]},
        "agendaItem": agenda,
        "dateStart": _iso(start_clock),
        "dateEnd": _iso(start_clock + datetime.timedelta(seconds=seconds or 0)),
        "people": [{**main, "context": main_status}, *others],
        "textContents": text_contents,
        "media": media,
        "documents": [],
        "debug": debug,
    }


def known_people(entries: list[dict], interventions: list[dict]) -> list[dict]:
    """What the sitting's sources say about each person: the page's entry that names a group, else the person's first entry, with the record's group abbreviation where the page names no group."""
    known: list[dict] = []
    for entry in entries:
        if not entry.get("name"):
            continue
        record = entry_person(entry)
        current = _find(known, record)
        if current is None:
            known.append(record)
        elif "faction" not in current and "faction" in record:
            known[known.index(current)] = record
    for item in interventions:
        speaker = item.get("speaker") or {}
        if not speaker.get("group"):
            continue
        current = _find(known, person(speaker.get("name") or "", person_id=speaker.get("personId")))
        if current is not None and "faction" not in current:
            current["faction"] = {"label": speaker["group"]}
    return known


def _session_dates(media: dict) -> tuple[str, str]:
    """Start and end of the sitting: its first intervention's time and the end the archive states, extended to the last intervention's start."""
    clocks = [_moment(e["clock"]) for e in media["data"] if e.get("clock")]
    start = min(clocks) if clocks else _moment(media["meta"]["dateStart"])
    end = max([m for m in (_moment(media["meta"].get("dateEnd")), *clocks) if m is not None])
    return _iso(start), _iso(end)


def merge_documents(media: dict, proceedings: Optional[dict], session: str) -> dict:
    """The Stage 2 document of a sitting from its parsed media document and, when the record is there, its parsed proceedings."""
    period, number = parse_session_key(session)
    media_meta = media["meta"]
    entries = media["data"]
    interventions = (proceedings or {}).get("data") or []
    carried, uncovered = distribute(entries, interventions)
    session_dates = _session_dates(media)
    known = known_people(entries, interventions)

    speeches = []
    left_out = 0
    for entry, indexes in zip(entries, carried):
        record = speech_record(entry, [interventions[i] for i in indexes], indexes[0] if indexes else None,
                               media_meta=media_meta,
                               record_page=((proceedings or {}).get("meta") or {}).get("sourceURI", ""),
                               period=period, number=number, known=known, session_dates=session_dates)
        if record is None:
            left_out += 1
            continue
        record["speechIndex"] = len(speeches) + 1
        if "confidence" in record["debug"]:
            record["debug"]["linkedMediaIndexes"] = [record["speechIndex"]]
        normalize_speech_originid(record)
        speeches.append(record)
    fill_original_language(speeches, PARLIAMENT_ID)

    with_text = sum(1 for s in speeches if s["textContents"])
    doubtful = sum(1 for s in speeches if s["debug"].get("confidence", 1) < 1)
    logger.info(f"[{session}] merged {len(speeches)} speeches, {with_text} with text"
                + (f", {doubtful} marked as doubtful" if doubtful else "")
                + (f", {left_out} page entr(ies) left out" if left_out else "")
                + (f", {len(uncovered)} record intervention(s) without a clip" if uncovered else ""))
    return {
        "meta": build_meta(
            PARLIAMENT_ID,
            session=session,
            electoral_period=period,
            date_start=session_dates[0],
            date_end=session_dates[1],
            processing={
                **media_meta.get("processing", {}),
                **((proceedings or {}).get("meta") or {}).get("processing", {}),
                "merge": now_iso(),
            },
            extra={
                "sourceLabel": f"Seduta n. {number}",
                "sourceURI": media_meta.get("sourcePage"),
            },
        ),
        "data": speeches,
    }


def merge_session(session: str, config, options=None) -> Path:
    """Write ``cache/merged/{session}-merged.json`` and return its path. An unchanged merge leaves the file untouched."""
    media_file = config.file(session, "media")
    if not media_file.exists():
        raise FileNotFoundError(f"[{session}] parsed file missing: {media_file}")
    proceedings_file = config.file(session, "proceedings")
    proceedings = (json.loads(proceedings_file.read_text(encoding="utf-8"))
                   if proceedings_file.exists() else None)
    if proceedings is None:
        logger.info(f"[{session}] no record yet, the speeches are merged without text")
    doc = merge_documents(json.loads(media_file.read_text(encoding="utf-8")), proceedings, session)
    out = config.file(session, "merged", create=True)
    if save_if_changed(doc, out):
        logger.info(f"[{session}] wrote {out.name}")
    else:
        logger.info(f"[{session}] no change, {out.name} left as it is")
    return out


if __name__ == "__main__":
    session_cli(PARLIAMENT_ID, merge_session, description=__doc__, example="190731")
