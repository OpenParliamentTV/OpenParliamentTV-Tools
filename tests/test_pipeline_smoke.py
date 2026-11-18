"""End-to-end smoke: parse fixtures → merge → validate against minimal schema.

This is the strongest single guard. It exercises every line of code the cron
job's `--merge-speeches` step touches (excluding align/NER/NEL which need
external services), and gates the result on the same validator the cron
runs at publish time."""

from optv.shared.validators import validate_stage2
from tests.de_fixtures import merge_tiny_fixtures


def test_pipeline_produces_schema_valid_session():
    merged = merge_tiny_fixtures()

    findings = validate_stage2(merged, schema="minimal", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    assert not errors, f"merged session has schema/semantic errors: {errors}"
