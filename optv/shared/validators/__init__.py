"""OPTV Stage 2 validators.

Public API:
    validate_stage2(doc, schema="full", semantic=True) -> list[Finding]
    Finding = {"severity": "error"|"warning", "rule": str, "path": str, "message": str}

`error` findings indicate the file is invalid for the given schema.
`warning` findings are data-quality flags; they do not block import.
"""

from .schema_validator import validate_schema
from .semantic_validator import validate_semantic


def validate_stage2(doc, schema="full", semantic=True):
    findings = validate_schema(doc, schema)
    if semantic:
        findings.extend(validate_semantic(doc))
    return findings


__all__ = ["validate_stage2", "validate_schema", "validate_semantic"]
