"""End-to-end smoke: parse fixtures → merge → validate against minimal schema.

This is the strongest single guard. It exercises every line of code the cron
job's `--merge-speeches` step touches (excluding align/NER/NEL which need
external services), and gates the result on the same validator the cron
runs at publish time."""

import json
from pathlib import Path

from optv.parliaments.DE.merger.merge_session import merge_data
from optv.parliaments.DE.parsers.media2json import parse_media_data
from optv.parliaments.DE.parsers.proceedings2json import parse_transcript
from optv.shared.validators import validate_stage2

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "DE"


def test_pipeline_produces_schema_valid_session():
    proc_speeches = list(parse_transcript(str(FIXTURES / "tiny-proceedings.xml")))
    first = proc_speeches[0]
    session_id = f"{first['electoralPeriod']['number']}{str(first['session']['number']).zfill(3)}"
    proc = {
        "meta": {"session": session_id,
                 "dateStart": first["session"]["dateStart"],
                 "dateEnd": first["session"]["dateEnd"],
                 "processing": {}},
        "data": proc_speeches,
    }
    media_raw = json.loads((FIXTURES / "tiny-media-input.json").read_text())
    media = parse_media_data(media_raw)

    merged = merge_data(proc, media, options=None)

    findings = validate_stage2(merged, schema="minimal", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    assert not errors, f"merged session has schema/semantic errors: {errors}"
