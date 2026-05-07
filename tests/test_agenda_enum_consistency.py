"""Lock the schema's documented agendaItem.type vocabulary to CORE_TYPES.

If a new core type lands in agenda_types.py without updating the schema's enum
description, this test fails and tells the author what to add.
"""

import json
from pathlib import Path

from optv.shared.agenda_types import (
    CORE_TYPES,
    classify_de_native,
    classify_de_rp,
    classify_parlamint_de,
    classify_se,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = REPO_ROOT / "optv" / "shared" / "schema" / "stage2-full.schema.json"


def test_schema_documents_every_core_type():
    schema = json.loads(SCHEMA_FILE.read_text())
    description = schema["definitions"]["agendaItem"]["properties"]["type"]["description"]
    missing = sorted(t for t in CORE_TYPES if t not in description)
    assert not missing, (
        f"Schema description for agendaItem.type does not mention: {missing}. "
        f"Update {SCHEMA_FILE.relative_to(REPO_ROOT)} to list every value in CORE_TYPES."
    )


# Inputs known to exercise classifier branches across every parliament.
DE_NATIVE_INPUTS = [
    "Sitzungsende", "Befragung der Bundesregierung", "Fragestunde",
    "Aktuelle Stunde", "Regierungserklärung", "Wahl der Präsidentin",
    "Abstimmung über den Antrag", "Vereidigung", "Eröffnung der Sitzung",
    "Geschäftsordnung", "Würdigung", "Haushaltsgesetz",
    "Tagesordnungspunkt 5", "",
]
PARLAMINT_INPUTS = [
    "#DE-question_time", "#DE-current_affairs", "#DE-government_declaration",
    "#DE-election", "#DE-voting", "#DE-oath", "#DE-debate", "#DE-motion",
    "#DE-budget", "#DE-misc", "", None,
]
DE_RP_INPUTS = [
    "Fragestunde", "Aktuelle Debatte", "Regierungserklärung",
    "Wahl der Präsidentin", "Einzelplan 06", "Vereidigung", "",
]
SE_INPUTS = [
    "ärendedebatt", "frågestund", "interpellationsdebatt", "aktuell debatt",
    "regeringsförklaring", "votering", "val", "okänd-aktivitet", None, "",
]


def test_classifier_outputs_are_in_core_enum():
    for fn, inputs in [
        (classify_de_native, DE_NATIVE_INPUTS),
        (classify_parlamint_de, PARLAMINT_INPUTS),
        (classify_de_rp, DE_RP_INPUTS),
        (classify_se, SE_INPUTS),
    ]:
        for inp in inputs:
            _, core = fn(inp)
            assert core in CORE_TYPES, (
                f"{fn.__name__}({inp!r}) returned core={core!r}, "
                f"which is not in CORE_TYPES"
            )
