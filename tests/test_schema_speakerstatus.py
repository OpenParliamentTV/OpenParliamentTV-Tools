"""textBody[].speakerstatus is the platform speaker vocabulary (or null), never
a role label — the full schema enforces it."""

import pytest

from optv.shared.validators import validate_stage2


def _doc(status):
    return {
        "meta": {"session": "1", "dateStart": "2025-01-01T10:00:00+01:00"},
        "data": [{
            "parliament": "AT",
            "electoralPeriod": {"number": 27},
            "session": {"number": 1},
            "speechIndex": 1,
            "dateStart": "2025-01-01T10:00:00+01:00",
            "agendaItem": {"officialTitle": "TOP 1", "title": "TOP 1"},
            "media": {"videoFileURI": "https://example.org/v.mp4",
                      "sourcePage": "https://example.org/s"},
            "people": [{"label": "Max Beispiel", "context": "main-speaker"}],
            "textContents": [{
                "type": "proceedings", "language": "de",
                "textBody": [{"type": "speech", "speaker": "Max Beispiel",
                              "speakerstatus": status,
                              "sentences": [{"text": "Hallo."}]}],
            }],
        }],
    }


def _speakerstatus_errors(doc):
    return [f for f in validate_stage2(doc, schema="full", semantic=False)
            if f["severity"] == "error" and "speakerstatus" in f.get("path", "")]


@pytest.mark.parametrize("status", [
    None, "main-speaker", "speaker", "president", "vice-president", "interim-president"])
def test_vocabulary_values_pass(status):
    assert not _speakerstatus_errors(_doc(status))


@pytest.mark.parametrize("status", ["Abgeordneter", "ministre", "statsminister", ""])
def test_role_labels_fail(status):
    assert _speakerstatus_errors(_doc(status))
