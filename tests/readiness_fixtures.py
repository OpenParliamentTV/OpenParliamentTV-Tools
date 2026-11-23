"""Synthetic sample sessions for the readiness tests, built from the full-featured example document."""

import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = json.loads((ROOT / "optv" / "shared" / "docs" / "EXAMPLES" / "full-featured.json").read_text())

SENTENCE = "Das ist ein Satz aus dem Protokoll der Sitzung."


def make_speech(index: int, *, session_number: int = 19, sentences: int = 6, duration: float = 60.0,
                wid: str | None = "Q123", confidence=1, linked=(1,), timed: bool = True,
                agenda_type: str = "regular", ner: bool = True) -> dict:
    """One speech that validates against the full schema. ``confidence=None`` drops the field, ``linked=None`` drops ``linkedMediaIndexes``, ``sentences=0`` leaves the speech without text."""
    speech = copy.deepcopy(EXAMPLE["data"][0])
    speech["speechIndex"] = index
    speech["session"]["number"] = session_number
    speech["agendaItem"]["type"] = agenda_type
    speech["media"]["sourcePage"] = f"https://example.org/video/{session_number}/{index}"
    speech["media"]["duration"] = duration
    if wid is None:
        speech["people"][0].pop("wid", None)
    else:
        speech["people"][0]["wid"] = wid
    debug = speech["debug"]
    if confidence is None:
        debug.pop("confidence", None)
    else:
        debug["confidence"] = confidence
    if linked is None:
        debug.pop("linkedMediaIndexes", None)
    else:
        debug["linkedMediaIndexes"] = list(linked)
    if not ner:
        debug.pop("nerDuration", None)
    if not sentences:
        speech["textContents"] = []
        debug.pop("proceedingIndex", None)
        return speech
    step = duration / sentences
    body = []
    for n in range(sentences):
        sentence = {"text": f"{SENTENCE} Nummer {index}.{n}.", "entities": []}
        if timed:
            sentence["timeStart"] = f"{n * step:.3f}"
            sentence["timeEnd"] = f"{(n + 1) * step:.3f}"
        body.append(sentence)
    text_content = speech["textContents"][0]
    text_content["textBody"] = [{"type": "speech", "speaker": "Wolfgang Schäuble",
                                 "speakerstatus": "president",
                                 "text": " ".join(s["text"] for s in body), "sentences": body}]
    speech["textContents"] = [text_content]
    return speech


def make_session(session: str, speeches: list[dict]) -> dict:
    meta = copy.deepcopy(EXAMPLE["meta"])
    meta["session"] = session
    return {"meta": meta, "data": speeches}


def clean_session(session: str, session_number: int, count: int = 4, **speech) -> dict:
    return make_session(session, [make_speech(i, session_number=session_number, **speech)
                                  for i in range(1, count + 1)])


def write_session(data_dir: Path, session: str, doc: dict) -> Path:
    path = data_dir / "processed" / f"{session}-session.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    return path


def clip(data_dir: Path, speech: dict) -> Path:
    from optv.shared.whisper_diff import audio_path_for
    return audio_path_for(data_dir, speech["electoralPeriod"]["number"], speech["session"]["number"],
                          speech["speechIndex"])


def stage_clips(data_dir: Path, doc: dict) -> None:
    for speech in doc["data"]:
        path = clip(data_dir, speech)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"not really audio")


def speech_text(speech: dict) -> str:
    return " ".join(sentence["text"] for tc in speech["textContents"] for item in tc["textBody"]
                    for sentence in item["sentences"])


def write_transcripts(data_dir: Path, session: str, doc: dict, *, skip=(), text=None,
                      generated_at: str = "2026-01-01T10:00:00") -> Path:
    """A Whisper cache as ``whisper_diff transcribe`` leaves it, repeating each speech's own text unless ``text`` is given."""
    entries = []
    for speech in doc["data"]:
        if speech["speechIndex"] in skip:
            continue
        transcript = speech_text(speech) if text is None else text
        duration = float(speech["media"].get("duration") or 60.0)
        entries.append({"speechIndex": speech["speechIndex"], "ok": True,
                        "audio_file": clip(data_dir, speech).name, "transcribe_seconds": 0.8 * duration,
                        "text": transcript, "segments": [{"start": 0.0, "end": duration, "text": transcript}],
                        "language": "de", "language_probability": 1.0, "duration": duration,
                        "model": "large-v3"})
    path = data_dir / "cache" / "whisper" / f"{session}-whisper.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"session": session, "model": "large-v3", "language": "per speech",
                                "with_speaker_check": False, "generated_at": generated_at,
                                "speeches": entries}))
    return path
