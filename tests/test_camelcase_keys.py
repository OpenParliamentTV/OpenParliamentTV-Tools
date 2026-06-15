"""Guardrail: no published Stage-2 field key uses kebab/snake case.

Scans the DE golden master (the byte-exact production output snapshot). A few
keys are legitimately non-camel and allowlisted: the ``meta.processing`` stage
names, ``media.additionalInformation`` source-structural keys, and the legacy
``textBody.speech_id``.
"""

import json
from pathlib import Path

GOLDEN = Path(__file__).parent / "fixtures" / "DE" / "golden" / "tiny-session.json"

# Non-camel keys that are intentional (stage names / source-structural / legacy).
_ALLOW = {"parse_media", "parse_proceedings", "speech_id"}


def _bad_keys(obj, path="", under_addinfo=False):
    bad = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            here = f"{path}/{k}"
            if (("-" in k or "_" in k) and k not in _ALLOW and not under_addinfo):
                bad.append(here)
            bad += _bad_keys(v, here, under_addinfo or k == "additionalInformation")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            bad += _bad_keys(v, f"{path}/{i}", under_addinfo)
    return bad


def test_golden_has_no_kebab_or_snake_keys():
    doc = json.loads(GOLDEN.read_text())
    bad = _bad_keys(doc)
    assert not bad, f"non-camelCase keys in published output: {bad}"
