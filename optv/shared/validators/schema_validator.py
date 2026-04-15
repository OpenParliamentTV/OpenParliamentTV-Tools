"""JSON Schema validator for OPTV Stage 2."""

import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft7Validator

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schema"


@lru_cache(maxsize=None)
def _validator(schema_name):
    path = SCHEMA_DIR / f"stage2-{schema_name}.schema.json"
    with path.open() as f:
        schema = json.load(f)
    return Draft7Validator(schema)


def validate_schema(doc, schema_name="full"):
    v = _validator(schema_name)
    findings = []
    for e in v.iter_errors(doc):
        path = "/".join(str(p) for p in e.absolute_path) or "<root>"
        findings.append({
            "severity": "error",
            "rule": f"schema.{schema_name}.{e.validator}",
            "path": path,
            "message": e.message,
        })
    return findings
