"""Tests for the one-time processed/ in-place migration."""

from optv.shared.migrate_processed import migrate_doc


def _doc():
    return {
        "meta": {
            "session": "21001",
            "dateStart": "2025-01-01T10:00:00+01:00",
            "dateEnd": "2025-01-01T11:00:00+01:00",
            "schemaVersion": "1.0",
            "processing": {"merge": "2025-01-01T11:00:00", "ner": "2025-01-02T09:00:00"},
        },
        "data": [{
            "parliament": "DE",
            "electoralPeriod": {"number": 21},
            "session": {"number": 1},
            "media": {"videoFileURI": "x", "sourcePage": "y",
                      "creator": "Deutscher Bundestag", "license": "Public Domain"},
        }],
    }


def test_migration_adds_meta_and_language():
    changed, new = migrate_doc(_doc())
    assert changed
    meta = new["meta"]
    assert meta["schemaVersion"] == "1.0"
    # parliament/electoralPeriod live per-speech only; meta must not duplicate them.
    assert "parliament" not in meta
    assert "electoralPeriod" not in meta
    # lastProcessing derived from the latest processing timestamp (ner).
    assert meta["lastProcessing"] == "ner"
    assert "lastUpdate" in meta
    # originalLanguage filled from the manifest language_code.
    assert new["data"][0]["originalLanguage"] == "de"
    # processing history preserved.
    assert meta["processing"]["ner"] == "2025-01-02T09:00:00"


def test_migration_strips_meta_per_speech_duplicates():
    doc = _doc()
    doc["meta"]["parliament"] = "DE"
    doc["meta"]["electoralPeriod"] = {"number": 21}
    changed, new = migrate_doc(doc)
    assert changed
    assert "parliament" not in new["meta"]
    assert "electoralPeriod" not in new["meta"]


def test_migration_is_idempotent():
    _, once = migrate_doc(_doc())
    changed_again, _ = migrate_doc(once)
    assert changed_again is False


from optv.shared.migrate_processed import _rename_debug_keys


def test_debug_keys_renamed_to_camelcase():
    data = [{"debug": {"align-duration": 1.2, "ner-duration": 0.5,
                       "confidence_reason": "x", "ivod_id": "7", "confidence": 1}}]
    changed = _rename_debug_keys(data)
    assert changed
    d = data[0]["debug"]
    assert d == {"alignDuration": 1.2, "nerDuration": 0.5,
                 "confidenceReason": "x", "ivodId": "7", "confidence": 1}
    # idempotent
    assert _rename_debug_keys(data) is False


from optv.shared.migrate_processed import _backfill_rights


def test_backfill_rights_from_manifest():
    data = [{
        "parliament": "EU",
        "electoralPeriod": {"number": 10},
        "media": {"videoFileURI": "x", "sourcePage": "y", "creator": "European Parliament"},
        "textContents": [{"language": "en", "textBody": []}],
    }]
    _backfill_rights(data, "EU")
    media = data[0]["media"]
    tc = data[0]["textContents"][0]
    assert media["license"]            # filled from manifest
    assert tc["creator"] and tc["license"]
    # existing value is not overwritten
    assert media["creator"] == "European Parliament"


from optv.shared.migrate_processed import _fix_origin_media_id_placement


def test_origin_media_id_relocated_and_dropped():
    data = [{"originMediaID": "abc", "media": {"videoFileURI": "v", "sourcePage": "p"}}]
    _fix_origin_media_id_placement(data)
    assert "originMediaID" not in data[0]
    assert data[0]["media"]["originMediaID"] == "abc"   # relocated

def test_origin_media_id_dropped_when_media_already_has_it():
    data = [{"originMediaID": "abc", "media": {"originMediaID": "abc"}}]
    _fix_origin_media_id_placement(data)
    assert "originMediaID" not in data[0]
    assert data[0]["media"]["originMediaID"] == "abc"   # kept media's
