"""The example session JSONs under optv/shared/docs/EXAMPLES/ must validate
against the schemas they claim to demonstrate. Locks in doc/example drift
(such as the originID/originTextID rename that left examples behind)."""

import json
from pathlib import Path

import pytest

from optv.shared.validators import validate_stage2

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = REPO_ROOT / "optv" / "shared" / "docs" / "EXAMPLES"

EXAMPLES = sorted(EXAMPLES_DIR.glob("*.json"))


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_validates_against_full_schema(path):
    doc = json.loads(path.read_text())
    findings = validate_stage2(doc, schema="full", semantic=False)
    errors = [f for f in findings if f["severity"] == "error"]
    assert not errors, (
        f"{path.name} fails full schema validation: {errors}"
    )


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_validates_against_minimal_schema(path):
    doc = json.loads(path.read_text())
    findings = validate_stage2(doc, schema="minimal", semantic=False)
    errors = [f for f in findings if f["severity"] == "error"]
    assert not errors, (
        f"{path.name} fails minimal schema validation: {errors}"
    )


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_uses_canonical_origin_id_at_top_level(path):
    """Top-level key on a speech is `originID`. The legacy name `originTextID`
    only belongs inside textContents[]. Stale docs that still use the old
    top-level name fail this — caught the post-f9d9ea1 drift."""
    doc = json.loads(path.read_text())
    for i, speech in enumerate(doc["data"]):
        assert "originTextID" not in speech, (
            f"{path.name} data[{i}] uses legacy top-level 'originTextID'; "
            f"rename to 'originID' (the textContents[].originTextID is still correct)"
        )
