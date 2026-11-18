"""The tiny DE fixtures, parsed and merged through the DE parser and merger functions."""

import json
from pathlib import Path

from optv.parliaments.DE.merger.merge_session import merge_data
from optv.parliaments.DE.parsers.media2json import parse_media_data
from optv.parliaments.DE.parsers.proceedings2json import parse_transcript

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "DE"


def parse_tiny_fixtures() -> tuple[dict, dict]:
    """``(proceedings, media)`` documents in the shape the merger takes."""
    proc_speeches = list(parse_transcript(str(FIXTURES / "tiny-proceedings.xml")))
    first = proc_speeches[0]
    session_id = f"{first['electoralPeriod']['number']}{str(first['session']['number']).zfill(3)}"
    proc = {
        "meta": {
            "session": session_id,
            "dateStart": first["session"]["dateStart"],
            "dateEnd": first["session"]["dateEnd"],
            "processing": {},
        },
        "data": proc_speeches,
    }
    media_raw = json.loads((FIXTURES / "tiny-media-input.json").read_text())
    return proc, parse_media_data(media_raw)


def merge_tiny_fixtures() -> dict:
    proc, media = parse_tiny_fixtures()
    return merge_data(proc, media, options=None)
