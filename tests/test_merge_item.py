"""Behavioural tests for optv/parliaments/DE/merger/merge_session.merge_item.

This file would have caught the recent originID/originTextID regression: the
merger reads first_proceeding['originID'] and writes output['originID'].
A parser that emits the wrong top-level key trips the first assertion below.
"""

import copy

import pytest

from optv.parliaments.DE.merger.merge_session import merge_item


def make_media_item():
    return {
        "parliament": "DE",
        "electoralPeriod": {"number": 21},
        "session": {"number": 999},
        "speechIndex": 1,
        "agendaItem": {
            "title": "Tagesordnungspunkt 1",
            "officialTitle": "Tagesordnungspunkt 1",
        },
        "dateStart": "2025-01-01T10:05:00+01:00",
        "dateEnd": "2025-01-01T10:10:00+01:00",
        "people": [
            {"label": "Max Beispiel", "context": "main-speaker",
             "faction": {"label": "SPD"}},
        ],
        "media": {
            "videoFileURI": "https://example.invalid/video.mp4",
            "sourcePage": "https://example.invalid/page",
            "duration": 300.0,
            "originMediaID": "9999001",
        },
        "debug": {"originalTitle": "Redebeitrag von Max Beispiel (SPD)..."},
    }


def make_proceeding_item():
    return {
        "parliament": "DE",
        "electoralPeriod": {"number": 21},
        "session": {
            "number": 999,
            "dateStart": "2025-01-01T10:00:00",
            "dateEnd": "2025-01-01T11:00:00",
        },
        "speechIndex": 1001,
        "originID": "ID29999900",
        "agendaItem": {"officialTitle": "Tagesordnungspunkt 1"},
        "debug": {},
        "people": [
            {"type": "memberOfParliament", "label": "Max Beispiel",
             "firstname": "Max", "lastname": "Beispiel",
             "context": "main-speaker", "faction": {"label": "SPD"}},
        ],
        "textContents": [{
            "type": "proceedings",
            "sourceURI": "https://example.invalid/proceedings.xml",
            "creator": "Deutscher Bundestag",
            "license": "Public Domain",
            "language": "DE-de",
            "originTextID": "ID29999900",
            "textBody": [
                {"speech_id": "ID29999900", "type": "speech",
                 "speaker": "Max Beispiel", "speakerstatus": "main-speaker",
                 "text": "Dies ist ein Test.",
                 "sentences": [{"text": "Dies ist ein Test."}]},
            ],
        }],
        "documents": [],
    }


def test_merge_item_copies_origin_id_from_proceeding():
    """Regression guard: merger must read 'originID' (not 'originTextID')."""
    media = make_media_item()
    proc = make_proceeding_item()

    merged = merge_item(media, [proc])

    assert merged["originID"] == "ID29999900"


def test_merge_item_raises_keyerror_when_proceeding_origin_id_missing():
    """If a parser stops emitting top-level originID, this fails loudly here
    instead of crashing the cron mid-session."""
    media = make_media_item()
    proc = make_proceeding_item()
    del proc["originID"]
    with pytest.raises(KeyError):
        merge_item(media, [proc])


def test_merge_item_top_level_keys_match_downstream_contract():
    """The merged speech must carry every key the rest of the pipeline reads."""
    merged = merge_item(make_media_item(), [make_proceeding_item()])
    required = {
        "parliament", "electoralPeriod", "session", "speechIndex",
        "originID", "agendaItem", "dateStart", "dateEnd",
        "people", "textContents", "documents", "media", "debug",
    }
    missing = required - set(merged)
    assert not missing, f"merged speech missing keys: {missing}"


def test_merge_item_session_dates_come_from_proceeding():
    merged = merge_item(make_media_item(), [make_proceeding_item()])
    assert merged["session"]["dateStart"] == "2025-01-01T10:00:00"
    assert merged["session"]["dateEnd"] == "2025-01-01T11:00:00"


def test_merge_item_debug_records_indexes_and_confidence():
    merged = merge_item(make_media_item(), [make_proceeding_item()])
    debug = merged["debug"]
    assert debug["mediaIndex"] == 1
    assert debug["proceedingIndex"] == 1001
    assert debug["proceedingIndexes"] == [1001]
    assert debug["confidence"] == 1


def test_merge_item_main_speaker_first_when_media_provides_one():
    media = make_media_item()
    proc = make_proceeding_item()
    proc["people"].append({
        "type": "memberOfParliament", "label": "Erika Mustermann",
        "context": "speaker",
    })
    merged = merge_item(media, [proc])
    assert merged["people"][0]["label"] == "Max Beispiel"
    assert merged["people"][0]["context"] == "main-speaker"


def test_merge_item_proceedings_source_carried_when_present():
    proc = make_proceeding_item()
    proc["debug"]["proceedings-source"] = "parlamint-de-beta"
    merged = merge_item(make_media_item(), [proc])
    assert merged["debug"]["proceedings-source"] == "parlamint-de-beta"


def test_merge_item_classifies_agenda_from_media_title():
    """Period 18+: the proceeding has no real title, classification runs on
    the media-derived title that's now sitting on output.agendaItem.title."""
    media = make_media_item()
    media["agendaItem"]["title"] = "Aktuelle Stunde zu wichtigen Themen"
    merged = merge_item(media, [make_proceeding_item()])
    assert merged["agendaItem"]["type"] == "current_affairs"
    assert merged["agendaItem"]["nativeType"] == "DE-current_affairs"


def test_merge_item_preserves_parlamint_classification():
    """Period 17: parser already set type/nativeType from the structured
    `ana` token. Merger must not clobber them."""
    proc = make_proceeding_item()
    proc["agendaItem"]["type"] = "qa"
    proc["agendaItem"]["nativeType"] = "DE-question_time"
    merged = merge_item(make_media_item(), [proc])
    assert merged["agendaItem"]["type"] == "qa"
    assert merged["agendaItem"]["nativeType"] == "DE-question_time"


def test_merge_item_does_not_mutate_inputs():
    media = make_media_item()
    proc = make_proceeding_item()
    media_snapshot = copy.deepcopy(media)
    proc_snapshot = copy.deepcopy(proc)
    merge_item(media, [proc])
    assert media == media_snapshot
    assert proc == proc_snapshot
