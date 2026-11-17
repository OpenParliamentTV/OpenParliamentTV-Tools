#! /usr/bin/env python3
"""Merge per-Sitzung proceedings + media into Stage 2 JSON.

Both streams share the same per-speech ordering — every video-list-item
on the SP page contributes one entry to proceedings (parsed for speaker,
party, role, TOP, clip anchor, transcript URLs) and to media (parsed for MP4
URLs and duration via the AJAX endpoint). The join key is the standard
player-id; there is no Needleman-Wunsch alignment because the DOM order
guarantees a 1:1 correspondence.

This merger also performs the transcript fetch (the URL exactly as the SP
page links it) because the text is what completes a Stage 2 record. Fetched
HTML is cached under ``original/proceedings/transcripts/{session}/{speaker_id}.html``
so re-merges are free.

A clip's transcript is the Stenografischer Bericht text of that clip: one
or more contributions, each opened by a speaker line (``Präsident Tobias
Rausch:``, ``Daniel Roi (AfD):``). Each contribution becomes its own
``textBody`` item with its speaker; everyone who speaks goes into
``people[]`` (main speaker first).

Speech times are German wall-clock with their Europe/Berlin UTC offset:
the sitting's start from the protocol (09:00 until the protocol is
published) plus the cumulative clip durations, with the clock moved to the
resumption time at the clip each protocol break ended with.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from lxml import html as lxml_html
from spacy.lang.de import German

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.merger"

from ..parsers.common import (
    name_key, normalize_ws, parse_speaker_line, role_to_context, strip_honorifics,
)
from ..parsers import proceedings2json as _proceedings
from ..parsers.proceedings2json import letters
from ..scraper.common import fetch_text
from optv.parliaments import get_rights as _get_rights
from optv.shared.speech_id import normalize_speech_originid
from optv.shared.meta import build_meta, now_iso

logger = logging.getLogger(__name__)

MEDIA_CREATOR = _get_rights("DE-ST", stream="media")["creator"]
MEDIA_LICENSE = _get_rights("DE-ST", stream="media")["license"]
PROCEEDINGS_CREATOR = _get_rights("DE-ST", stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights("DE-ST", stream="proceedings")["license"]

BERLIN = ZoneInfo("Europe/Berlin")
# Start of a sitting whose protocol is not published yet.
DEFAULT_START = "09:00"

_nlp = German()
_nlp.add_pipe("sentencizer")


def split_sentences(text: str) -> list:
    return [{"text": str(s).strip()} for s in _nlp(text).sents if str(s).strip()]


# ---------------------------------------------------------------------------
# Transcript fetch + paragraphs
# ---------------------------------------------------------------------------

def _transcript_cache_path(transcripts_dir: Path, session: str, speaker_id: str) -> Path:
    return transcripts_dir / session / f"{speaker_id}.html"


def _fetch_transcript(*, url: str, speaker_id: str, session: str,
                      transcripts_dir: Path, retry_count: int = 10,
                      force: bool = False) -> str | None:
    cache_path = _transcript_cache_path(transcripts_dir, session, speaker_id)
    if cache_path.exists() and not force:
        return cache_path.read_text(encoding="utf-8", errors="replace")
    if not speaker_id or not url:
        return None
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        body = fetch_text(url, retry_count=retry_count)
    except RuntimeError as e:
        logger.warning(f"Transcript fetch failed for {session}/{speaker_id}: {e}")
        return None
    cache_path.write_text(body, encoding="utf-8")
    return body


def extract_paragraphs(transcript_html: str) -> list[str]:
    """Return [paragraph_text, ...] from the transcript AJAX response."""
    tree = lxml_html.fromstring(transcript_html)
    paragraphs = []
    for p in tree.xpath('.//div[contains(@class, "transcript-wrapper")]//p'):
        txt = normalize_ws(p.text_content())
        if txt:
            paragraphs.append(txt)
    if not paragraphs:
        # Fallback: any <p> in the document.
        for p in tree.xpath('.//p'):
            txt = normalize_ws(p.text_content())
            if txt:
                paragraphs.append(txt)
    return paragraphs


# ---------------------------------------------------------------------------
# Speaker turns
# ---------------------------------------------------------------------------

_TOP_LINE_RE = re.compile(r"^Tagesordnungspunkt \d+[a-z]?$")
_SECTION_HEADING_RE = re.compile(
    r"^(?:(?:Erste|Zweite|Dritte)\s+)?Beratung$|^Aktuelle Debatte$|^Abstimmung\b"
    r"|^Er[öo]ffnung\b|^Schlussbemerkungen$|^Fortsetzung\b|^Wahl\b|^Vereidigung\b"
    r"|^Befragung der Landesregierung\b|^Fragestunde\b")
_ANY_END_RE = re.compile(r"[.?!:][\"“”‘’»«]*$")
_STATEMENT_END_RE = re.compile(r"[.:][\"“”‘’»«]*$")


_SENTENCE_BREAK_RE = re.compile(r"[.!?][\"“”]? +[A-Za-zÄÖÜäöü]")


def drop_headings(paragraphs: list[str], marker: str = "") -> list[str]:
    """Drop the agenda headings the protocol text carries between speeches.

    Headings are lines without final punctuation: a section word
    (``Erste Beratung``, ``Abstimmung``, ``Schlussbemerkungen``, …), the
    clip's heading (``marker``), a single word opening the clip, and a
    ``Tagesordnungspunkt N`` line with the agenda item's title and
    printed-paper lines after it (``Antrag Fraktion AfD - Drs. 8/6495``,
    titles may end in ``!`` or ``?``) up to the first line that reads as
    speech.
    """
    out: list[str] = []
    in_top = False
    for i, para in enumerate(paragraphs):
        is_comment = para.startswith("(") and para.endswith(")")
        if not is_comment and not _ANY_END_RE.search(para):
            if _TOP_LINE_RE.match(para):
                in_top = True
                continue
            if (_SECTION_HEADING_RE.match(para) or (marker and para == marker)
                    or (i == 0 and " " not in para)):
                continue
        if (in_top and not is_comment and not _STATEMENT_END_RE.search(para)
                and not _SENTENCE_BREAK_RE.search(para)
                and parse_speaker_line(para) is None):
            continue
        in_top = False
        out.append(para)
    return out


def split_turns(paragraphs: list[str], marker: str = "") -> list[dict]:
    """Split a clip's paragraphs into contributions at the speaker lines.

    Returns ``[{"speaker": parse_speaker_line(...) | None, "paragraphs": [...]}]``
    after dropping agenda headings. Text before the first speaker line has no
    speaker of its own (``speaker: None``): the protocol continues the
    contribution of whoever spoke last, usually the presiding officer at the
    end of the previous clip.
    """
    turns: list[dict] = []
    lead: list[str] = []
    for para in drop_headings(paragraphs, marker):
        speaker = parse_speaker_line(para)
        if speaker is not None:
            turns.append({"speaker": speaker, "paragraphs": []})
        elif turns:
            turns[-1]["paragraphs"].append(para)
        else:
            lead.append(para)
    if lead:
        turns.insert(0, {"speaker": None, "paragraphs": lead})
    return turns


def _set_name(person: dict, label: str) -> None:
    person["label"] = label
    parts = strip_honorifics(label).split()
    if parts:
        person["firstname"] = parts[0]
        person["lastname"] = " ".join(parts[1:]) if len(parts) > 1 else ""


def _person_from_line(speaker: dict, context: str) -> dict:
    person: dict = {"type": "memberOfParliament", "context": context}
    _set_name(person, speaker["name"])
    if speaker.get("faction"):
        person["faction"] = {"label": speaker["faction"]}
    if speaker.get("role"):
        person["role"] = speaker["role"]
    return person


def _line_context(speaker: dict) -> str:
    if speaker.get("presidium"):
        return role_to_context(speaker.get("role")) or "speaker"
    return "speaker"


def _carried(person: dict, *, main: bool) -> dict:
    """A copy of the previous clip's last speaker, with its context here."""
    out = {k: v for k, v in person.items() if k not in ("context", "wid")}
    ctx = role_to_context(person.get("role"))
    out["context"] = ctx or ("main-speaker" if main else "speaker")
    return out


def resolve_people(slot_person: dict, turns: list[dict], *,
                   previous: dict | None = None,
                   fallback_label: str = "") -> tuple[list[dict], list[dict]]:
    """Return ``people[]`` (main speaker first) and the person of each turn.

    The main speaker is the slot heading's person. A procedural slot
    (``Abstimmung``, ``Wahl``) has none, so the speaker of its first
    contribution — the presiding officer — takes the role: ``previous`` (the
    last speaker of the previous clip) when the clip opens without a speaker
    line, else the first speaker line's person. A turn without a speaker
    line continues ``previous`` (the slot's speaker when there is none).
    Every further speaker is added once, in order of first appearance, with
    ``context`` president / vice-president / interim-president for the
    presidium and ``speaker`` otherwise.
    """
    main = dict(slot_person)
    speakers = [t["speaker"] for t in turns if t["speaker"]]
    if not main.get("label"):
        if turns and turns[0]["speaker"] is None and previous is not None:
            main = _carried(previous, main=True)
        elif speakers:
            first = speakers[0]
            _set_name(main, first["name"])
            if first.get("role"):
                main["role"] = first["role"]
            if first.get("faction"):
                main["faction"] = {"label": first["faction"]}
            main["context"] = role_to_context(first.get("role")) or "main-speaker"
        elif previous is not None:
            main = _carried(previous, main=True)
        else:
            main["label"] = fallback_label or "Unbekannt"

    main_keys = {name_key(main["label"])}
    if speakers and not main_keys & {name_key(s["name"]) for s in speakers}:
        alt = next((s for s in speakers if not s["presidium"]), None)
        if alt is not None:
            logger.warning(f"Slot speaker {main['label']!r} has no speaker line in the "
                           f"transcript; taking {alt['name']!r} as the main speaker")
            main_keys.add(name_key(alt["name"]))
    for s in speakers:
        if name_key(s["name"]) in main_keys:
            if s.get("role") and not main.get("role"):
                main["role"] = s["role"]
            if s.get("faction") and not main.get("faction"):
                main["faction"] = {"label": s["faction"]}
            break

    people = [main]
    by_key = {k: main for k in main_keys}
    turn_people: list[dict] = []
    for t in turns:
        s = t["speaker"]
        if s is None:
            if previous is None:
                turn_people.append(main)
                continue
            key = name_key(previous["label"])
            person = by_key.get(key)
            if person is None:
                person = _carried(previous, main=False)
                by_key[key] = person
                people.append(person)
            turn_people.append(person)
            continue
        key = name_key(s["name"])
        person = by_key.get(key)
        if person is None:
            person = _person_from_line(s, _line_context(s))
            by_key[key] = person
            people.append(person)
        turn_people.append(person)
    return people, turn_people


def _classify_paragraph(text: str) -> str:
    """Heuristic: bracketed lines like '(Beifall im ganzen Hause)' are comments."""
    stripped = text.strip()
    if stripped.startswith("(") and stripped.endswith(")"):
        return "comment"
    return "speech"


def build_textbody(turns: list[dict], turn_people: list[dict], speech_id: str) -> list[dict]:
    """One speech item per run of a contribution's paragraphs, comments apart."""
    body: list[dict] = []

    def flush(paragraphs: list[str], person: dict) -> None:
        if not paragraphs:
            return
        text = "\n".join(paragraphs)
        body.append({
            "speech_id": speech_id,
            "type": "speech",
            "speaker": person["label"],
            "speakerstatus": person.get("context") or "speaker",
            "text": text,
            "sentences": split_sentences(text),
        })

    for turn, person in zip(turns, turn_people):
        pending: list[str] = []
        for para in turn["paragraphs"]:
            if _classify_paragraph(para) == "comment":
                flush(pending, person)
                pending = []
                body.append({
                    "speech_id": speech_id,
                    "type": "comment",
                    "speaker": None,
                    "speakerstatus": None,
                    "text": para,
                    "sentences": [{"text": para}],
                })
            else:
                pending.append(para)
        flush(pending, person)
    return body


# ---------------------------------------------------------------------------
# Order of the sitting
# ---------------------------------------------------------------------------

PROBE_LETTERS = 50


def sitting_order(body: str | None, slot_paragraphs: list[list[str]]) -> list[int]:
    """Indexes of the clips in the order the sitting took them.

    The SP page lists the clips by agenda item, but a sitting can take items
    out of order (part of TOP 6 after TOP 8). The protocol body (letters
    only, see ``protocol_body_letters``) has the real order: each clip is
    placed where its first speech paragraph occurs in it. A text found
    several times takes the first occurrence after the clip before it on the
    page; a clip not found stays right after that clip. Without a protocol
    the page order stands.
    """
    if not body:
        return list(range(len(slot_paragraphs)))
    positions: list[float] = []
    prev = -1.0
    for paragraphs in slot_paragraphs:
        pos = None
        for para in paragraphs:
            if parse_speaker_line(para) or (para.startswith("(") and para.endswith(")")):
                continue
            probe = letters(para)[:PROBE_LETTERS]
            if len(probe) < 30:
                continue
            hits = []
            k = body.find(probe)
            while k != -1:
                hits.append(k)
                k = body.find(probe, k + 1)
            if hits:
                pos = float(next((h for h in hits if h >= prev), hits[0]))
                break
        if pos is None:
            pos = prev + 0.5
        positions.append(pos)
        prev = pos
    return sorted(range(len(slot_paragraphs)), key=lambda i: (positions[i], i))


# ---------------------------------------------------------------------------
# Times
# ---------------------------------------------------------------------------

def _local(date: str, hm: str) -> datetime:
    """``("2026-10-14", "10:00")`` → aware datetime in Europe/Berlin."""
    return datetime.strptime(f"{date}T{hm}", "%Y-%m-%dT%H:%M").replace(tzinfo=BERLIN)


def _iso(dt: datetime) -> str:
    return dt.isoformat("T", "seconds")


def speech_windows(date: str, sitting_times: dict | None,
                   slots: list[tuple[int, str]]) -> list[tuple[datetime, datetime]]:
    """Start/end of each clip, given ``[(duration_s, transcript_letters)]``.

    The clock starts at the sitting's start and runs on by each clip's
    duration. At a protocol break it jumps to the resumption time: at the
    clip whose transcript contains the break's anchor (the text the sitting
    resumed with), or — for a break without a matching clip — at the first
    clip that would start at or after the interruption.
    """
    cursor = _local(date, (sitting_times or {}).get("begin") or DEFAULT_START)
    at_slot: dict[int, list[dict]] = {}
    unplaced: list[dict] = []
    search_from = 0
    for br in (sitting_times or {}).get("breaks", []):
        anchor = br.get("anchor")
        idx = next((i for i in range(search_from, len(slots))
                    if anchor and anchor in slots[i][1]), None)
        if idx is None:
            logger.warning(f"{date}: break {br['stop']}–{br['resume']} matches no clip; "
                           f"placing it by clock time")
            unplaced.append(br)
        else:
            at_slot.setdefault(idx, []).append(br)
            search_from = idx + 1  # breaks are in time order; a clip resumes once

    windows = []
    for i, (duration, _) in enumerate(slots):
        for br in at_slot.get(i, []):
            cursor = max(cursor, _local(date, br["resume"]))
        for br in list(unplaced):
            if cursor >= _local(date, br["stop"]):
                cursor = max(cursor, _local(date, br["resume"]))
                unplaced.remove(br)
        end = cursor + timedelta(seconds=max(int(duration or 0), 1))
        windows.append((cursor, end))
        cursor = end
    return windows


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

def merge_session(session: str, config, options) -> Path:
    """Merge media + proceedings for one Sitzung, fetching transcripts inline."""
    media_path = config.file(session, "media")
    proceedings_path = config.file(session, "proceedings")
    if not media_path.exists():
        logger.warning(f"No media file for {session} at {media_path}")
        return config.file(session, "merged", create=True)
    if not proceedings_path.exists():
        logger.warning(f"No proceedings file for {session} at {proceedings_path}")
        return config.file(session, "merged", create=True)

    with media_path.open() as f:
        media_doc = json.load(f)
    with proceedings_path.open() as f:
        proceedings_doc = json.load(f)

    pmeta = proceedings_doc.get("meta", {})
    sp_number = pmeta.get("sitzungsperiode")
    sp_url = pmeta.get("sourceURI", "")
    date = pmeta.get("date")
    if sp_number is None or not sp_url or not date:
        logger.error(f"{session}: proceedings lack meta.sitzungsperiode/sourceURI/date — "
                     f"re-run the parse stage")
        return config.file(session, "merged", create=True)
    sitting_times = pmeta.get("sittingTimes")

    transcripts_dir = config.dir('proceedings') / "transcripts"
    media_by_pid: dict = media_doc.get("data", {})

    joined: list[tuple[dict, dict, list[str]]] = []
    for speech in proceedings_doc.get("data", []):
        debug = speech.get("debug") or {}
        pid = debug.get("stdPlayerId")
        if not pid or pid not in media_by_pid:
            logger.warning(f"{session} speech {speech.get('speechIndex')}: "
                           f"no media for player-id {pid!r}; skipping")
            continue
        transcript_html = _fetch_transcript(
            url=debug.get("transcriptURL", ""),
            speaker_id=debug.get("transcriptSpeakerId", ""),
            session=session,
            transcripts_dir=transcripts_dir,
            retry_count=getattr(options, "retry_count", 10),
        )
        paragraphs = extract_paragraphs(transcript_html) if transcript_html else []
        joined.append((speech, media_by_pid[pid], paragraphs))

    # Times and the carried-over speaker follow the order of the sitting;
    # speechIndex and the ids keep the page order.
    body = None
    protocol_pdf = config.dir('proceedings') / f"{session}.pdf"
    if sitting_times and protocol_pdf.exists():
        text = _proceedings.protocol_text_from_pdf(protocol_pdf)
        body = _proceedings.protocol_body_letters(text) if text else None
    order = sitting_order(body, [p for _, _, p in joined])
    chrono_windows = speech_windows(date, sitting_times, [
        (int(joined[i][1].get("duration") or 0), letters(" ".join(joined[i][2]))) for i in order])
    windows: list = [None] * len(joined)
    for k, i in enumerate(order):
        windows[i] = chrono_windows[k]
    if sitting_times:
        session_start, session_end = _local(date, sitting_times["begin"]), _local(date, sitting_times["end"])
    elif chrono_windows:
        session_start, session_end = chrono_windows[0][0], chrono_windows[-1][1]
    else:
        session_start, session_end = _local(date, "00:00"), _local(date, "23:59")
    times_source = "protocol" if sitting_times else "default"

    # Factions the slot headings of this sitting show, to complete speakers
    # whose own line names only their office (``Sven Schulze (Ministerpräsident):``).
    factions: dict[str, dict] = {}
    for speech in proceedings_doc.get("data", []):
        p = (speech.get("people") or [{}])[0]
        if p.get("label") and p.get("faction"):
            factions.setdefault(name_key(p["label"]), p["faction"])

    resolved: list = [None] * len(joined)
    previous: dict | None = None  # last speaker of the previous clip of the sitting
    for i in order:
        speech, _, paragraphs = joined[i]
        debug = speech.get("debug") or {}
        turns = split_turns(paragraphs, marker=debug.get("h3Label", ""))
        people, turn_people = resolve_people(
            speech.get("people", [{}])[0], turns, previous=previous,
            fallback_label=debug.get("h3Label", ""))
        for person in people:
            if not person.get("faction") and name_key(person["label"]) in factions:
                person["faction"] = dict(factions[name_key(person["label"])])
        if turn_people:
            previous = turn_people[-1]
        resolved[i] = (turns, people, turn_people)
    sitting_rank = {i: k + 1 for k, i in enumerate(order)}

    merged_speeches: list[dict] = []
    for idx, ((speech, media_entry, paragraphs), (start, end)) in enumerate(zip(joined, windows)):
        debug = speech.get("debug") or {}
        pid = debug["stdPlayerId"]
        turns, people, turn_people = resolved[idx]

        speech_id = speech["originID"]
        text_contents = [{
            "type": "proceedings",
            "language": "de",
            "originTextID": speech_id,
            "sourceURI": debug.get("transcriptPageURL") or sp_url,
            "creator": PROCEEDINGS_CREATOR,
            "license": PROCEEDINGS_LICENSE,
            "textBody": build_textbody(turns, turn_people, speech_id),
        }]

        duration = int(media_entry.get("duration") or 0)
        media: dict = {
            "videoFileURI": media_entry.get("videoFileURI", ""),
            # The Sitzungsperiode page hosts every speech's clip; the clip's
            # own anchor opens it (and keeps sourcePage unique per speech,
            # which the platform's import keys speech identity on).
            "sourcePage": f"{sp_url}{debug.get('videoAnchor', '')}",
            "creator": MEDIA_CREATOR,
            "license": MEDIA_LICENSE,
            "originMediaID": str(media_entry.get("video_id") or pid),
            "duration": duration,
        }
        if media_entry.get("preview_image_url"):
            media["thumbnailURI"] = media_entry["preview_image_url"]
            media["thumbnailCreator"] = MEDIA_CREATOR
            media["thumbnailLicense"] = media["license"]
        if media_entry.get("sources_by_quality"):
            media["additionalInformation"] = {
                "sourcesByQuality": media_entry["sources_by_quality"],
                "playerId": pid,
            }

        merged_speeches.append({
            "parliament": "DE-ST",
            "electoralPeriod": speech["electoralPeriod"],
            "session": {
                "number": speech["session"]["number"],
                "dateStart": _iso(session_start),
                "dateEnd": _iso(session_end),
            },
            "dateStart": _iso(start),
            "dateEnd": _iso(end),
            "speechIndex": speech["speechIndex"],
            "originID": speech_id,
            "originTextID": speech_id,
            "originalLanguage": "de",
            "agendaItem": speech["agendaItem"],
            "people": people,
            "media": media,
            "textContents": text_contents,
            "documents": [],
            "debug": {
                **debug,
                "proceedingIndex": speech["speechIndex"],
                "proceedingIndexes": [speech["speechIndex"]],
                "mediaIndex": speech["speechIndex"],
                "confidence": 1.0,
                "transcriptParagraphCount": len(paragraphs),
                "sittingTimesSource": times_source,
                **({"sittingOrder": sitting_rank[idx]} if body else {}),
            },
        })

    for _s in merged_speeches:
        normalize_speech_originid(_s)
    period = (proceedings_doc["data"][0]["electoralPeriod"] if proceedings_doc.get("data")
              else {"number": int(session[:2])})
    extra = {"sitzungsperiode": sp_number, "sourceURI": sp_url}
    if pmeta.get("protocolURL"):
        extra["protocolURL"] = pmeta["protocolURL"]
    doc = {
        "meta": build_meta(
            "DE-ST",
            session=session,
            electoral_period=period,
            date_start=_iso(session_start),
            date_end=_iso(session_end),
            processing={
                **pmeta.get("processing", {}),
                **media_doc["meta"].get("processing", {}),
                "merge": now_iso(),
            },
            extra=extra,
        ),
        "data": merged_speeches,
    }
    return config.save_data(doc, session, "merged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session", help="Session ID e.g. 09002")
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    from ..common import Config
    config = Config(args.data_dir)
    merge_session(args.session, config, args)
