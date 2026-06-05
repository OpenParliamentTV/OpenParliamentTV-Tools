"""DE golden-master — the guardrail for the shared-consolidation refactor.

DE is the only parliament live in production (de.openparliament.tv). Every step
of the refactor must leave DE's Stage-2 output byte-identical. This test runs the
exact parse→merge path the cron's ``--merge-speeches`` step touches over the tiny
DE fixtures and asserts the serialized result matches a committed snapshot.

The snapshot is normalized only for the one genuinely volatile field
(``meta.processing.*`` timestamps); everything else — field order, values,
whitespace — is compared verbatim via ``json.dumps(indent=2, ensure_ascii=False)``,
matching the on-disk format the pipeline writes.

Regenerate intentionally (e.g. the sanctioned 3b ``DE-de``→``de`` change) with::

    UPDATE_GOLDEN=1 .venv/bin/pytest -q tests/test_de_golden.py

and review the resulting diff before committing.
"""

import json
import os
from pathlib import Path

from optv.parliaments.DE.merger.merge_session import merge_data
from optv.parliaments.DE.parsers.media2json import parse_media_data
from optv.parliaments.DE.parsers.proceedings2json import parse_transcript

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "DE"
GOLDEN = FIXTURES / "golden" / "tiny-session.json"

_TS_PLACEHOLDER = "<timestamp>"
_FIXTURES_PLACEHOLDER = "<FIXTURES>"


def _normalize(doc: dict) -> dict:
    """Blank the only non-deterministic field so the snapshot is stable."""
    processing = doc.get("meta", {}).get("processing")
    if isinstance(processing, dict):
        for key in processing:
            processing[key] = _TS_PLACEHOLDER
    return doc


def _produce_merged() -> dict:
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
    media = parse_media_data(media_raw)
    return merge_data(proc, media, options=None)


def _serialize(doc: dict) -> str:
    rendered = json.dumps(_normalize(doc), indent=2, ensure_ascii=False)
    # ``sourceURI`` records the absolute path the parser was handed, which is
    # machine-specific (``/Users/...`` locally, ``/home/runner/...`` in CI).
    # Replace the fixtures root with a stable token so the snapshot is portable.
    return rendered.replace(str(FIXTURES), _FIXTURES_PLACEHOLDER)


def test_de_merged_output_matches_golden():
    rendered = _serialize(_produce_merged())

    if os.environ.get("UPDATE_GOLDEN") or not GOLDEN.exists():
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN.write_text(rendered + "\n")
        if not os.environ.get("UPDATE_GOLDEN"):
            # First-ever run created the baseline; nothing to assert against yet.
            return

    expected = GOLDEN.read_text().rstrip("\n")
    assert rendered == expected, (
        "DE Stage-2 output diverged from the golden master. If this change is "
        "intentional, regenerate with UPDATE_GOLDEN=1 and review the diff."
    )
