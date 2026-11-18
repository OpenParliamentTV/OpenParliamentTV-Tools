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

from tests.de_fixtures import FIXTURES, merge_tiny_fixtures

GOLDEN = FIXTURES / "golden" / "tiny-session.json"

_TS_PLACEHOLDER = "<timestamp>"
_FIXTURES_PLACEHOLDER = "<FIXTURES>"


def _normalize(doc: dict) -> dict:
    """Blank the non-deterministic timestamp fields so the snapshot is stable."""
    meta = doc.get("meta", {})
    processing = meta.get("processing")
    if isinstance(processing, dict):
        for key in processing:
            processing[key] = _TS_PLACEHOLDER
    if "lastUpdate" in meta:
        meta["lastUpdate"] = _TS_PLACEHOLDER
    return doc


def _serialize(doc: dict) -> str:
    rendered = json.dumps(_normalize(doc), indent=2, ensure_ascii=False)
    # ``sourceURI`` records the absolute path the parser was handed, which is
    # machine-specific (``/Users/...`` locally, ``/home/runner/...`` in CI).
    # Replace the fixtures root with a stable token so the snapshot is portable.
    return rendered.replace(str(FIXTURES), _FIXTURES_PLACEHOLDER)


def test_de_merged_output_matches_golden():
    rendered = _serialize(merge_tiny_fixtures())

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
