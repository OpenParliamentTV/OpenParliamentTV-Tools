"""What the criteria read off one Stage 2 speech."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

# Characters spoken per second, for a duration guess from the text alone.
_CHARS_PER_SECOND = {"zh": 4.5}
_DEFAULT_CHARS_PER_SECOND = 15.0


def debug(speech: dict) -> dict:
    return speech.get("debug") or {}


def sentences(speech: dict) -> Iterator[dict]:
    for tc in speech.get("textContents") or []:
        for item in tc.get("textBody") or []:
            if not isinstance(item, dict):
                continue
            for sentence in item.get("sentences") or []:
                if isinstance(sentence, dict):
                    yield sentence


def text_chars(speech: dict) -> int:
    return sum(len(s.get("text") or "") for s in sentences(speech))


def has_text(speech: dict) -> bool:
    return any(s.get("text") for s in sentences(speech))


# ---- the platform's text import gate ----
#
# Mirrors OpenParliamentTV-Platform modules/import/media-writer.php: the importer
# checks debug.confidence only for a speech that also carries linkedMediaIndexes.
# A speech with confidence alone imports its text whatever the value.

def gate_state(speech: dict) -> str:
    """``gated``: both gate fields, the platform applies the check. ``confidence-only``: the platform ignores the confidence. ``ungated``: no confidence at all."""
    dbg = debug(speech)
    if dbg.get("confidence") is None:
        return "ungated"
    return "gated" if "linkedMediaIndexes" in dbg else "confidence-only"


def would_import_text(speech: dict) -> bool:
    """Whether the platform imports this speech's text."""
    if not speech.get("textContents"):
        return False
    if gate_state(speech) != "gated":
        return True
    dbg = debug(speech)
    return dbg["confidence"] == 1 and len(dbg.get("linkedMediaIndexes") or []) == 1


def _is_number(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def missing_import_fields(speech: dict) -> list[str]:
    """Fields without which the platform importer rejects a speech (``importMediaValidate``)."""
    missing = []
    if not speech.get("parliament"):
        missing.append("parliament")
    if not _is_number((speech.get("electoralPeriod") or {}).get("number")):
        missing.append("electoralPeriod.number")
    if not _is_number((speech.get("session") or {}).get("number")):
        missing.append("session.number")
    agenda = speech.get("agendaItem") or {}
    if not agenda.get("title") and not agenda.get("officialTitle"):
        missing.append("agendaItem.title")
    media = speech.get("media") or {}
    if not media.get("videoFileURI"):
        missing.append("media.videoFileURI")
    if not media.get("sourcePage"):
        missing.append("media.sourcePage")
    if not speech.get("dateStart"):
        missing.append("dateStart")
    return missing


# ---- duration of a speech's audio ----

def _positive(value) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if number > 0 else 0.0


def speech_duration(speech: dict, clip: Path | None = None) -> tuple[float, str]:
    """Seconds of audio of a speech and where the figure comes from, without network access.

    Sources in order: ``media.duration``; the ``#t=start,end`` fragment of the video URI; the stream offsets a session-stream parliament keeps in ``media.additionalInformation``; the cached audio clip, when ``clip`` is given; the last sentence end time; the text length. The last one is a guess.
    """
    from optv.shared.whisper_diff import declared_duration
    duration, source = declared_duration(speech)
    if duration:
        return duration, source

    if clip is not None and clip.exists():
        from optv.shared.whisper_diff import clip_seconds
        duration = clip_seconds(clip)
        if duration:
            return duration, "clip"

    last_end = max((_positive(s.get("timeEnd")) for s in sentences(speech)), default=0.0)
    if last_end:
        return last_end, "sentence-times"

    chars = text_chars(speech)
    if chars:
        rate = _CHARS_PER_SECOND.get(speech.get("originalLanguage"), _DEFAULT_CHARS_PER_SECOND)
        return chars / rate, "text-length"
    return 0.0, "unknown"


def clip_path(data_dir: Path, speech: dict) -> Path:
    """The audio clip the align stage caches for a speech, which Whisper transcribes."""
    from optv.shared.whisper_diff import audio_path_for
    return audio_path_for(data_dir, speech["electoralPeriod"]["number"],
                          speech["session"]["number"], speech["speechIndex"])
