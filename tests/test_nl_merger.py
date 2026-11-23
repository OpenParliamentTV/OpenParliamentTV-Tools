"""NL merger on the synthetic fixtures: every speech of the report gets its clip of the day's recording and its page."""

from __future__ import annotations

import copy
import datetime
import json

import pytest

from optv.parliaments.NL.common import Config
from optv.parliaments.NL.merger import merge_session as M
from optv.shared.validators import validate_stage2

from .nl_fixtures import SESSION, parsed_media, parsed_proceedings

STREAM = "https://stream.example.org/2026-03-03/plenairezaal/index.m3u8"
DEBATE_PAGE = "https://debatdirect.tweedekamer.nl/2026-03-03/voorbeeld/plenaire-zaal/voorbeeldwet-12345-10-15"
REPORT_PAGE = "https://www.tweedekamer.nl/kamerstukken/plenaire_verslagen/detail/2025-2026/50"


@pytest.fixture(scope="module")
def proceedings() -> dict:
    return parsed_proceedings()


@pytest.fixture(scope="module")
def media() -> dict:
    return parsed_media()


@pytest.fixture(scope="module")
def merged(proceedings, media) -> dict:
    return M.merge_documents(copy.deepcopy(proceedings), copy.deepcopy(media), SESSION)


def test_clip_url_names_the_range_in_the_streams_clock_format():
    zone = datetime.timezone(datetime.timedelta(hours=2))
    start = datetime.datetime(2026, 9, 23, 11, 42, 8, tzinfo=zone)
    end = datetime.datetime(2026, 9, 23, 11, 44, 4, tzinfo=zone)
    assert M.clip_url(STREAM, start, end) == (
        STREAM + "?start=2026-09-23T11%3A42%3A08%2B0200&end=2026-09-23T11%3A44%3A04%2B0200")


def test_every_speech_is_merged_with_text_and_a_clip(merged, proceedings):
    speeches = merged["data"]
    assert len(speeches) == len(proceedings["data"]) == 8
    assert [s["speechIndex"] for s in speeches] == list(range(1, 9))
    for speech in speeches:
        assert speech["media"]["videoFileURI"].startswith(STREAM + "?start=")
        assert speech["textContents"][0]["textBody"]
        assert speech["debug"]["proceedingIndex"] == speech["speechIndex"]


def test_merged_document_validates(merged):
    errors = [f for f in validate_stage2(merged, schema="full", semantic=True) if f["severity"] == "error"]
    assert errors == []


def test_period_session_and_meta(merged):
    speech = merged["data"][0]
    assert speech["parliament"] == "NL"
    assert speech["electoralPeriod"] == {"number": 2025}
    assert speech["session"] == {"number": 50, "dateStart": "2026-03-03T10:15:00+01:00",
                                 "dateEnd": "2026-03-03T11:46:00+01:00"}
    assert speech["originalLanguage"] == "nl"
    meta = merged["meta"]
    assert meta["session"] == SESSION
    assert meta["sourceLabel"] == "Handelingen 2025-2026, nr. 50"
    assert meta["sourceURI"] == REPORT_PAGE
    assert meta["reportStatus"] == "Gecorrigeerd"
    assert {"parse_media", "parse_proceedings", "merge"} <= set(meta["processing"])
    assert "parliament" not in meta and "electoralPeriod" not in meta


def test_media_block_of_a_speech(merged):
    media = merged["data"][1]["media"]
    assert media["videoFileURI"] == (
        STREAM + "?start=2026-03-03T10%3A15%3A12%2B0100&end=2026-03-03T10%3A17%3A20%2B0100")
    assert media["duration"] == 128.0
    assert media["originMediaID"] == "a0000000-0000-0000-0000-000000000002"
    assert media["additionalInformation"] == {"stream": STREAM}
    assert media["aligned"] is False
    assert "audioFileURI" not in media          # the clip is the only address; align_prep reads it


def test_rights_come_from_the_manifest(merged):
    speech = merged["data"][1]
    text = speech["textContents"][0]
    assert (text["type"], text["language"], text["sourceURI"]) == ("proceedings", "nl", REPORT_PAGE)
    assert text["originTextID"] == "d0000000-0000-0000-0000-000000000030"
    assert text["creator"] and text["license"]
    assert speech["media"]["creator"] and speech["media"]["license"].startswith("https://www.tweedekamer.nl/")
    assert "originID" not in speech and "originTextID" not in speech


def test_a_speech_links_to_the_moment_it_starts_at(merged):
    pages = [s["media"]["sourcePage"] for s in merged["data"]]
    # The member's speech opens with the chair's short turn, which Debat Direct marks as a chairman moment.
    assert pages[1] == DEBATE_PAGE + "?event=chairman2026-03-03T10%3A15%3A12%2B0100"
    # A part of a cut turn starts where the speaker resumes.
    assert pages[3] == DEBATE_PAGE + "?event=speaker2026-03-03T10%3A55%3A05%2B0100"


def test_a_speech_without_a_marked_moment_links_to_its_start_time(merged):
    pages = [s["media"]["sourcePage"] for s in merged["data"]]
    # Debat Direct marks nothing at the late point of order, and only the start of the votes.
    assert pages[6].endswith("regeling-van-werkzaamheden-11-40?event=speaker2026-03-03T11%3A41%3A55%2B0100")
    assert pages[7].endswith("stemmingen-11-45?event=chairman2026-03-03T11%3A45%3A00%2B0100")


def test_a_speech_without_a_debate_is_cut_from_the_days_recording_and_links_to_the_report(merged):
    opening = merged["data"][0]
    assert opening["media"]["videoFileURI"].startswith(STREAM + "?start=2026-03-03T10%3A15%3A00")
    assert opening["media"]["sourcePage"] == REPORT_PAGE + "#d0000000-0000-0000-0000-000000000010"
    assert "originMediaID" not in opening["media"]


def test_source_pages_are_unique(merged):
    pages = [s["media"]["sourcePage"] for s in merged["data"]]
    assert len(set(pages)) == len(pages)


def test_a_moment_inside_the_speech_is_used_when_none_marks_its_start():
    debate = {"sourcePage": "https://dd/x", "events": [
        {"time": "2026-03-03T10:00:00+01:00", "type": "debate_start", "objectId": "d"},
        {"time": "2026-03-03T10:00:05+01:00", "type": "speaker", "objectId": "p"},
    ]}
    zone = datetime.timezone(datetime.timedelta(hours=1))
    start = datetime.datetime(2026, 3, 3, 10, 0, 0, tzinfo=zone)
    end = start + datetime.timedelta(seconds=60)
    speech = {"originTextID": "t", "people": [{"context": "main-speaker"}]}
    assert M.source_page(debate, speech, "https://report", start, end) == (
        "https://dd/x?event=speaker2026-03-03T10%3A00%3A05%2B0100")
    # Nothing but the start of the debate: that moment is taken.
    debate["events"] = debate["events"][:1]
    assert M.source_page(debate, speech, "https://report", start, end) == (
        "https://dd/x?event=debate_start2026-03-03T10%3A00%3A00%2B0100")


def test_speeches_without_any_recording_are_dropped(proceedings, media):
    no_streams = copy.deepcopy(media)
    for debate in no_streams["data"]:
        debate["stream"] = None
    doc = M.merge_documents(copy.deepcopy(proceedings), no_streams, SESSION)
    assert doc["data"] == []


def test_a_speech_without_text_stays_as_a_clip(proceedings, media):
    textless = copy.deepcopy(proceedings)
    textless["data"][1]["textBody"] = []
    speech = M.merge_documents(textless, copy.deepcopy(media), SESSION)["data"][1]
    assert speech["textContents"] == []
    assert "proceedingIndex" not in speech["debug"]
    assert speech["media"]["videoFileURI"]


def test_a_sitting_before_the_first_period_is_refused(proceedings, media):
    with pytest.raises(ValueError):
        M.merge_documents(copy.deepcopy(proceedings), copy.deepcopy(media), "2025-011")


def test_merge_session_writes_the_file_and_leaves_an_unchanged_one_alone(tmp_path, proceedings, media):
    config = Config(tmp_path)
    config.file(SESSION, "proceedings", create=True).write_text(json.dumps(proceedings))
    config.file(SESSION, "media", create=True).write_text(json.dumps(media))
    out = M.merge_session(SESSION, config)
    assert out == config.file(SESSION, "merged")
    first = out.read_text()
    assert len(json.loads(first)["data"]) == 8
    M.merge_session(SESSION, config)
    assert out.read_text() == first              # same speeches: the file, with its merge stamp, is untouched


def test_merge_session_needs_both_parsed_files(tmp_path):
    with pytest.raises(FileNotFoundError):
        M.merge_session(SESSION, Config(tmp_path))
