#! /usr/bin/env python3
"""Merge per-Sitzung proceedings + media into Stage 2 JSON.

Both streams share the same per-speech ordering — every video-list-item
on the SP page contributes one entry to proceedings (parsed for speaker,
party, role, TOP, transcript-id, cHash) and to media (parsed for MP4 URLs
and duration via the AJAX endpoint). The join key is the standard player-id;
there is no Needleman-Wunsch alignment because the DOM order guarantees a
1:1 correspondence.

This merger also performs the transcript fetch (per ``(speaker-id, cHash)``)
because the text is what completes a Stage 2 record. Fetched HTML is cached
under ``original/proceedings/transcripts/{session}/{speaker_id}.html`` so
re-merges are free.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from lxml import html as lxml_html
from spacy.lang.de import German

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.merger"

from ..parsers.common import normalize_ws, role_to_context, strip_honorifics
from ..scraper.common import LANDTAG_BASE, fetch_text
from optv.shared.speech_id import normalize_speech_originid

logger = logging.getLogger(__name__)

_nlp = German()
_nlp.add_pipe("sentencizer")


def split_sentences(text: str) -> list:
    return [{"text": str(s).strip()} for s in _nlp(text).sents if str(s).strip()]


def _transcript_cache_path(transcripts_dir: Path, session: str, speaker_id: str) -> Path:
    return transcripts_dir / session / f"{speaker_id}.html"


def _fetch_transcript(*, sp_number: int, speaker_id: str, c_hash: str,
                      session: str, transcripts_dir: Path,
                      retry_count: int = 10, force: bool = False) -> str | None:
    cache_path = _transcript_cache_path(transcripts_dir, session, speaker_id)
    if cache_path.exists() and not force:
        return cache_path.read_text(encoding="utf-8", errors="replace")
    if not speaker_id or not c_hash:
        return None
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    url = (f"{LANDTAG_BASE}/{sp_number}-sitzungsperiode"
           f"?transcriptSessions=lsaSessionsAjax"
           f"&tx_lsasessions_transcript%5Bspeaker%5D={speaker_id}&cHash={c_hash}")
    try:
        body = fetch_text(url, retry_count=retry_count)
    except RuntimeError as e:
        logger.warning(f"Transcript fetch failed for {session}/{speaker_id}: {e}")
        return None
    cache_path.write_text(body, encoding="utf-8")
    return body


_SPEAKER_PREFIX_RE = re.compile(
    r"^\s*(?:(?P<role>Präsident(?:in)?|Vizepräsident(?:in)?|Alterspräsident(?:in)?|"
    r"Ministerpräsident(?:in)?|Minister(?:in)?|Staatssekretär(?:in)?|"
    r"Staatsminister(?:in)?|Fraktionsvorsitzender|Fraktionsvorsitzende)\s+)?"
    r"(?P<name>[A-ZÄÖÜ][\w\. \-äöüß]+?)\s*:\s*$",
    re.UNICODE,
)


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


def parse_speaker_prefix(first_paragraph: str) -> dict:
    """Extract ``{role, name}`` from a line like ``Präsident Dr. X:``."""
    m = _SPEAKER_PREFIX_RE.match(first_paragraph)
    if not m:
        return {"role": None, "name": None}
    return {
        "role": (m.group("role") or "").strip() or None,
        "name": normalize_ws(m.group("name") or ""),
    }


def _classify_paragraph(text: str) -> str:
    """Heuristic: bracketed lines like '(Beifall im ganzen Hause)' are comments."""
    stripped = text.strip()
    if stripped.startswith("(") and stripped.endswith(")"):
        return "comment"
    return "speech"


def build_textbody(paragraphs: list[str], main_speaker_label: str, speech_id: str) -> list[dict]:
    """Convert paragraphs into Stage 2 textBody entries.

    The first paragraph is usually the speaker prefix line and gets dropped
    when it's just a name+colon. Subsequent paragraphs become one
    speech/comment textBody item each.
    """
    if not paragraphs:
        return []

    body = []
    speech_paragraphs = []
    for para in paragraphs[1:]:  # drop leading prefix
        kind = _classify_paragraph(para)
        if kind == "comment":
            # Flush accumulated speech first.
            if speech_paragraphs:
                text = "\n".join(speech_paragraphs)
                body.append({
                    "speech_id": speech_id,
                    "type": "speech",
                    "speaker": main_speaker_label,
                    "speakerstatus": "main-speaker",
                    "text": text,
                    "sentences": split_sentences(text),
                })
                speech_paragraphs = []
            body.append({
                "speech_id": speech_id,
                "type": "comment",
                "speaker": None,
                "speakerstatus": None,
                "text": para,
                "sentences": [{"text": para}],
            })
        else:
            speech_paragraphs.append(para)
    if speech_paragraphs:
        text = "\n".join(speech_paragraphs)
        body.append({
            "speech_id": speech_id,
            "type": "speech",
            "speaker": main_speaker_label,
            "speakerstatus": "main-speaker",
            "text": text,
            "sentences": split_sentences(text),
        })
    return body


def _sitzungsperiode_for_session(proceedings_doc: dict) -> int | None:
    return proceedings_doc.get("meta", {}).get("sitzungsperiode")


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

    sp_number = _sitzungsperiode_for_session(proceedings_doc)
    if sp_number is None:
        logger.error(f"{session}: proceedings missing meta.sitzungsperiode")
        return config.file(session, "merged", create=True)

    transcripts_dir = config.dir('proceedings') / "transcripts"
    media_by_pid: dict = media_doc.get("data", {})

    proceedings_data = proceedings_doc.get("data", [])
    sitzung_date = proceedings_doc["meta"]["dateStart"][:10]
    # Synthesise per-speech timestamps from cumulative duration so downstream
    # sorts are stable. Per-sentence alignment fills in real timing later.
    base = datetime.fromisoformat(f"{sitzung_date}T09:00:00")
    cursor = base

    merged_speeches: list[dict] = []
    for speech in proceedings_data:
        pid = (speech.get("debug") or {}).get("std-player-id")
        if not pid or pid not in media_by_pid:
            logger.warning(f"{session} speech {speech.get('speechIndex')}: "
                           f"no media for player-id {pid!r}; skipping")
            continue
        media_entry = media_by_pid[pid]
        duration = int(media_entry.get("duration") or 0)
        date_start = cursor.isoformat("T", "seconds")
        date_end = (cursor + timedelta(seconds=max(duration, 1))).isoformat("T", "seconds")
        cursor += timedelta(seconds=max(duration, 1))

        speaker_id = (speech.get("debug") or {}).get("transcript-speaker-id", "")
        c_hash = (speech.get("debug") or {}).get("transcript-cHash", "")
        transcript_html = _fetch_transcript(
            sp_number=sp_number,
            speaker_id=speaker_id,
            c_hash=c_hash,
            session=session,
            transcripts_dir=transcripts_dir,
            retry_count=getattr(options, "retry_count", 10),
        )
        paragraphs = extract_paragraphs(transcript_html) if transcript_html else []
        prefix = parse_speaker_prefix(paragraphs[0]) if paragraphs else {"role": None, "name": None}

        # Resolve speaker info: HTML h3 wins when it carries a real name;
        # otherwise fall back to the transcript prefix line.
        person = speech.get("people", [{}])[0]
        if not person.get("label") and prefix["name"]:
            person["label"] = prefix["name"]
            parts = strip_honorifics(person["label"]).split()
            if parts:
                person["firstname"] = parts[0]
                person["lastname"] = " ".join(parts[1:]) if len(parts) > 1 else ""
        if prefix["role"] and not person.get("role"):
            person["role"] = prefix["role"]
        # Map presidium roles to context enum
        ctx = role_to_context(person.get("role"))
        if ctx:
            person["context"] = ctx
        elif not person.get("context"):
            person["context"] = "main-speaker"
        if not person.get("label"):
            # Still no speaker — emit a placeholder so the schema is satisfied;
            # NEL will fail to link but the speech is at least visible.
            person["label"] = speech.get("debug", {}).get("h3-label") or "Unbekannt"

        speech_id = speech["originID"]
        text_body = build_textbody(paragraphs, person["label"], speech_id)

        text_contents = [{
            "type": "proceedings",
            "language": "de",
            "originTextID": speech_id,
            "sourceURI": speech["meta"]["sourceURI"] if "sourceURI" in speech else proceedings_doc["meta"].get("sourceURI", ""),
            "creator": "Landtag von Sachsen-Anhalt",
            "license": "© Landtag von Sachsen-Anhalt — Schriftdolmetschung",
            "textBody": text_body,
        }]

        media: dict = {
            "videoFileURI": media_entry.get("videoFileURI", ""),
            # The Sitzungsperiode page hosts every speech's clip, so append the
            # per-speech player-id: the platform keys speech identity on
            # sourcePage and a session-constant value would collapse all
            # speeches into one at import.
            "sourcePage": f"{LANDTAG_BASE}/{sp_number}-sitzungsperiode?player={pid}#section-video-1-1",
            "creator": "Landtag von Sachsen-Anhalt",
            "license": (
                "Landtag von Sachsen-Anhalt - social sharing and private use permitted; "
                "commercial use requires written consent"
            ),
            "originMediaID": str(media_entry.get("video_id") or pid),
            "duration": duration,
        }
        if media_entry.get("preview_image_url"):
            media["thumbnailURI"] = media_entry["preview_image_url"]
            media["thumbnailCreator"] = "Landtag von Sachsen-Anhalt"
            media["thumbnailLicense"] = media["license"]
        if media_entry.get("sources_by_quality"):
            media["additionalInformation"] = {
                "sourcesByQuality": media_entry["sources_by_quality"],
                "playerId": pid,
            }

        merged: dict = {
            "parliament": "DE-ST",
            "electoralPeriod": speech["electoralPeriod"],
            "session": speech["session"],
            "dateStart": date_start,
            "dateEnd": date_end,
            "speechIndex": speech["speechIndex"],
            "originID": speech_id,
            "originTextID": speech_id,
            "originalLanguage": "de",
            "agendaItem": speech["agendaItem"],
            "people": [person],
            "media": media,
            "textContents": text_contents,
            "documents": [],
            "debug": {
                **speech.get("debug", {}),
                "proceedingIndex": speech["speechIndex"],
                "proceedingIndexes": [speech["speechIndex"]],
                "mediaIndex": speech["speechIndex"],
                "confidence": 1.0,
                "transcript-paragraph-count": len(paragraphs),
            },
        }
        merged_speeches.append(merged)

    for _s in merged_speeches:
        normalize_speech_originid(_s)
    doc = {
        "meta": {
            "schemaVersion": "1.0",
            "parliament": "DE-ST",
            "electoralPeriod": proceedings_doc["data"][0]["electoralPeriod"] if proceedings_doc["data"] else {"number": 8},
            "session": session,
            "sitzungsperiode": sp_number,
            "dateStart": proceedings_doc["meta"]["dateStart"],
            "dateEnd": proceedings_doc["meta"]["dateEnd"],
            "sourceURI": proceedings_doc["meta"].get("sourceURI", ""),
            "processing": {
                **proceedings_doc["meta"].get("processing", {}),
                **media_doc["meta"].get("processing", {}),
                "merge": datetime.now().isoformat("T", "seconds"),
            },
            "lastUpdate": datetime.now().isoformat("T", "seconds"),
        },
        "data": merged_speeches,
    }
    return config.save_data(doc, session, "merged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session", help="Session ID e.g. 08105")
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
