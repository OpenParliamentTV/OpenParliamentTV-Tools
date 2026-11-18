"""FI parser unit tests (speech records + broadcast media + helpers).

Fixtures under ``tests/fixtures/FI/`` are trimmed copies of real data:
``speeches-sample.json`` holds open data API speech records (an MP from 58/2026,
a minister from 56/2026, a Swedish speech from 5/2023, plus a synthetic record
without spoken text); ``event-sample.json`` models the verkkolähetys
``speakers[]`` payload for plenary 58/2026. No spaCy or network access is
needed: the parser is driven with a stub sentencizer.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import pytest

from optv.parliaments.FI.common import (
    parse_session_str, session_number_int, session_str, term_years,
)
from optv.parliaments.FI.parsers import proceedings2json as p2j
from optv.parliaments.FI.parsers.media2json import drop_replaced_picks, speaker_media_record
from optv.parliaments.FI.scraper.eduskunta_api import parse_sitting_aggregation
from optv.parliaments.FI.scraper.fetch_media import _extract_json_value
from optv.parliaments.FI.scraper.fetch_proceedings import needs_refresh
from optv.parliaments.FI.workflow import sittings_in_term
from optv.shared.agenda_types import (
    CORE_BUDGET, CORE_QA, CORE_REGULAR, CORE_VOTING, classify_fi,
)

FIX = Path(__file__).resolve().parent / "fixtures" / "FI"


class StubNLP:
    """Minimal sentencizer: split on sentence-final punctuation."""

    def __call__(self, text):
        import re

        parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", text) if p.strip()]

        class S:
            def __init__(self, t):
                self.text = t

        class Doc:
            sents = [S(p) for p in parts]

        return Doc()


# ---- session-key helpers ----

def test_session_key_round_trip():
    assert session_str(2026, 58) == "2026-058"
    assert parse_session_str("2026-058") == (2026, 58)


def test_session_number_encoding_is_term_unique():
    # Per-year numbering collides; the encoding separates years of a term.
    assert session_number_int(2023, 2023, 58) == 58
    assert session_number_int(2023, 2026, 58) == 3058
    assert session_number_int(2023, 2024, 58) != session_number_int(2023, 2025, 58)


def test_term_years_spans_five_valtiopaivat():
    assert term_years(2023) == [2023, 2024, 2025, 2026, 2027]


# ---- agenda classification ----

def test_classify_fi_core_types():
    assert classify_fi("Suullinen kyselytunti") == ("FI-suullinen_kyselytunti", CORE_QA)
    assert classify_fi("Valtion talousarvio vuodelle 2027") == ("FI-talousarvio", CORE_BUDGET)
    assert classify_fi("Hallituksen esitys eduskunnalle ...") == ("FI-esitys", CORE_REGULAR)
    assert classify_fi("Ainoan käsittelyn äänestykset")[1] == CORE_VOTING
    assert classify_fi(None) == (None, CORE_REGULAR)
    assert classify_fi("Jokin tuntematon otsikko") == (None, CORE_REGULAR)


# ---- RSC flight value extraction (the riskiest media-scraper bit) ----

def test_extract_json_value_handles_rsc_date_and_undefined():
    flight = (
        'noise {"speakers":[{"name":"A","time":1,"x":$undefined,'
        '"ts":"$D2026-05-29T10:07:28.000Z"}]} trailing'
    )
    speakers = _extract_json_value(flight, "speakers")
    assert speakers == [{"name": "A", "time": 1, "x": None,
                         "ts": "2026-05-29T10:07:28.000Z"}]
    assert _extract_json_value(flight, "missing") is None


# ---- media records ----

def test_speaker_media_record_offsets_and_fragment():
    event = json.loads((FIX / "event-sample.json").read_text())
    rec = speaker_media_record(event["speakers"][0], event,
                               "https://verkkolahetys.eduskunta.fi/x")
    assert rec["personNumber"] == 1391
    assert rec["isReply"] is False
    media = rec["media"]
    assert media["videoFileURI"].endswith("playlist.m3u8#t=455,483")
    assert "#t=" not in media["audioFileURI"]  # clean master for align_prep
    assert media["additionalInformation"]["startOffset"] == 455.0
    assert media["duration"] == 28.0
    assert media["additionalInformation"]["eventRef"] == event["eventId"]


def test_speaker_media_record_reply_flag():
    event = json.loads((FIX / "event-sample.json").read_text())
    rec = speaker_media_record(event["speakers"][1], event, "x")
    assert rec["isReply"] is True


@pytest.mark.parametrize("end_time", ["null", None, ""])
def test_speaker_media_record_without_an_end_time(end_time):
    """The broadcast page writes the string "null" for a speech it has no end for; such a speech keeps its start and gets no duration."""
    event = json.loads((FIX / "event-sample.json").read_text())
    speaker = dict(event["speakers"][0], endTime=end_time)
    media = speaker_media_record(speaker, event, "https://verkkolahetys.eduskunta.fi/x")["media"]
    assert media["duration"] == 0.0
    assert media["videoFileURI"].endswith("playlist.m3u8#t=455,455")
    assert media["additionalInformation"]["startOffset"] == 455.0


def test_speech_without_an_end_time_ends_where_the_next_speaker_starts():
    event = json.loads((FIX / "event-sample.json").read_text())
    speaker = dict(event["speakers"][0], endTime="null")
    media = speaker_media_record(speaker, event, "x", next_start=500.0)["media"]
    assert media["duration"] == 45.0
    assert media["videoFileURI"].endswith("playlist.m3u8#t=455,500")


@pytest.mark.parametrize("next_start, duration", [(455.0 + 1800, 1800.0), (455.0 + 1801, 0.0), (455.0 + 5767, 0.0)])
def test_next_speaker_start_counts_only_within_half_an_hour(next_start, duration):
    """A longer gap to the next entry is a break in the sitting, not the length of the speech."""
    event = json.loads((FIX / "event-sample.json").read_text())
    speaker = dict(event["speakers"][0], endTime="null")
    assert speaker_media_record(speaker, event, "x", next_start=next_start)["media"]["duration"] == duration


@pytest.mark.parametrize("next_start", [455.0, 400.0, None])
def test_next_speaker_start_is_no_end_unless_it_is_later(next_start):
    """An earlier or equal start is no end either."""
    event = json.loads((FIX / "event-sample.json").read_text())
    speaker = dict(event["speakers"][0], endTime="null")
    media = speaker_media_record(speaker, event, "x", next_start=next_start)["media"]
    assert media["duration"] == 0.0
    assert media["videoFileURI"].endswith("playlist.m3u8#t=455,455")


def test_a_known_end_time_is_kept_whatever_the_next_speaker_start():
    event = json.loads((FIX / "event-sample.json").read_text())
    media = speaker_media_record(event["speakers"][0], event, "x", next_start=470.0)["media"]
    assert media["duration"] == 28.0
    assert media["videoFileURI"].endswith("playlist.m3u8#t=455,483")


def _entry(person, time, end="null"):
    return {"personNumber": person, "time": time, "endTime": end, "lastName": f"P{person}"}


def _people(entries):
    return [(e["personNumber"], e["time"]) for e in entries]


def test_replaced_pick_is_dropped_and_its_successor_carries_the_speech():
    """Lyly picked, replaced at once by Valtonen who speaks for minutes."""
    speakers = [_entry(1, 0, 60), _entry(2, 100), _entry(3, 100, 447), _entry(4, 445, 700)]
    assert _people(drop_replaced_picks(speakers)) == [(1, 0), (3, 100), (4, 445)]


def test_a_chain_of_replaced_picks_is_dropped_up_to_the_entry_with_an_end():
    speakers = [_entry(1, 100), _entry(2, 100), _entry(3, 100, 198), _entry(2, 208, 332)]
    assert _people(drop_replaced_picks(speakers)) == [(3, 100), (2, 208)]


def test_a_short_wrong_pick_goes_when_the_first_person_is_entered_again():
    """Nieminen picked, switched to Niemi for 9 s, Nieminen entered again 2 s after the start."""
    speakers = [_entry(7, 100), _entry(8, 100, 109), _entry(7, 102, 192), _entry(9, 190, 250)]
    assert _people(drop_replaced_picks(speakers)) == [(7, 102), (9, 190)]


@pytest.mark.parametrize("speakers", [
    # the successor is long: it is the speech, even though the first person follows shortly
    [_entry(7, 100), _entry(8, 100, 400), _entry(7, 110, 500)],
    # the first person comes back, but much later
    [_entry(7, 100), _entry(8, 100, 109), _entry(7, 130, 200)],
    # the entry after the successor is somebody else
    [_entry(7, 100), _entry(8, 100, 109), _entry(9, 102, 200)],
])
def test_the_successor_stays_unless_it_is_a_short_pick_corrected_back(speakers):
    assert _people(drop_replaced_picks(speakers)) == _people(speakers[1:])


def test_entries_that_are_not_replaced_picks_stay():
    speakers = [
        _entry(1, 100, 150), _entry(2, 100, 160),   # same start, both with an end
        _entry(3, 200), _entry(4, 290, 400),         # no end, next entry later: a speech missing its end
        _entry(5, 500),                              # no end, last entry
    ]
    assert drop_replaced_picks(speakers) == speakers
    assert drop_replaced_picks([]) == []


def test_session_parse_drops_replaced_picks(tmp_path, caplog):
    from optv.parliaments.FI.common import Config
    from optv.parliaments.FI.parsers.media2json import parse_media_for_session

    event = json.loads((FIX / "event-sample.json").read_text())
    first, second = event["speakers"]
    replaced = dict(first, personNumber=4242, lastName="Replaced", endTime="null")
    event["speakers"] = [replaced, first, second]
    config = Config(tmp_path)
    config.dir("media", create=True)
    config.raw_event("2026-058").write_text(json.dumps(event))

    with caplog.at_level("INFO"):
        records = parse_media_for_session(config, "2026-058")["data"]
    assert [r["personNumber"] for r in records] == [first["personNumber"], second["personNumber"]]
    pages = [r["media"]["sourcePage"] for r in records]
    assert len(set(pages)) == len(pages)
    assert any("dropped 1 replaced speaker pick" in r.getMessage() for r in caplog.records)


def test_session_parse_passes_each_entry_the_next_start(tmp_path):
    from optv.parliaments.FI.common import Config
    from optv.parliaments.FI.parsers.media2json import parse_media_for_session

    event = json.loads((FIX / "event-sample.json").read_text())
    first, second = event["speakers"]
    event["speakers"] = [dict(first, endTime="null"), dict(second, endTime="null")]
    config = Config(tmp_path)
    config.dir("media", create=True)
    config.raw_event("2026-058").write_text(json.dumps(event))

    records = parse_media_for_session(config, "2026-058")["data"]
    assert [r["media"]["duration"] for r in records] == [22.0, 0.0]   # 455 → 477, then the last entry


@pytest.mark.parametrize("start_time", ["null", None])
def test_speaker_media_record_without_a_start_time_is_dropped(start_time):
    event = json.loads((FIX / "event-sample.json").read_text())
    speaker = dict(event["speakers"][0], time=start_time)
    assert speaker_media_record(speaker, event, "x") is None


# ---- speech-record parsing ----

def _records():
    return json.loads((FIX / "speeches-sample.json").read_text())


def _parse_fixture():
    return p2j.parse_speeches(_records(), "unused", 2026, 58, nlp=StubNLP())


def test_parser_extracts_speaker_and_join_keys():
    doc = _parse_fixture()
    a = doc["data"][0]
    person = a["people"][0]
    assert person["label"] == "Ari Koponen"
    assert person["faction"] == {"label": "ps"}
    assert person["originPersonID"] == "1391"
    assert "role" not in person
    assert a["debug"]["personNumber"] == "1391"
    assert a["debug"]["tila"] == "Hyväksytty"
    assert a["originID"] == "PUH 58/2026/6/1/1"
    assert a["textContents"][0]["originTextID"] == "PUH 58/2026/6/1/1"


def test_parser_converts_utc_to_helsinki_offset():
    a = _parse_fixture()["data"][0]
    # Late May → EEST (UTC+3).
    assert a["dateStart"] == "2026-05-29T13:07:28+03:00"
    assert a["dateEnd"] == "2026-05-29T13:07:50+03:00"
    assert p2j.to_helsinki("2026-01-15T10:00:00+00:00") == "2026-01-15T12:00:00+02:00"
    assert p2j.to_helsinki(None) is None


def test_parser_minister_gets_role_and_group_faction():
    minister = _parse_fixture()["data"][1]["people"][0]
    assert minister["label"] == "Sanni Grahn-Laasonen"
    assert minister["role"] == "Sosiaaliturvaministeri"
    assert minister["context"] == "main-speaker"
    # No party abbreviation for ministers; the parliamentary group gives it.
    assert minister["faction"] == {"label": "kok"}


def test_parser_strips_brackets():
    sentences = _parse_fixture()["data"][0]["textContents"][0]["textBody"][0]["sentences"]
    joined = " ".join(s["text"] for s in sentences)
    assert "Hälinää" not in joined            # editorial bracket stripped
    assert "työeläkejärjestelmää" in joined


def test_parser_skips_records_without_spoken_text():
    doc = _parse_fixture()
    ids = [s["originID"] for s in doc["data"]]
    assert "PUH 58/2026/6/1/99" not in ids    # text was only a stage direction
    assert [s["speechIndex"] for s in doc["data"]] == [1, 2, 3]


def test_parser_detects_swedish_speech():
    doc = _parse_fixture()
    langs = [(s["originalLanguage"], s["textContents"][0]["language"]) for s in doc["data"]]
    assert langs == [("fi", "fi"), ("fi", "fi"), ("sv", "sv")]


def test_parser_agenda_classification():
    agenda = _parse_fixture()["data"][0]["agendaItem"]
    assert agenda["id"] == "kohta-6"
    assert agenda["type"] == CORE_REGULAR
    assert agenda["nativeType"] == "FI-esitys"


def test_parser_meta_date_range():
    meta = _parse_fixture()["meta"]
    assert meta["session"] == "2026-058"
    assert meta["sourceLabel"] == "PTK 58/2026 vp"
    assert meta["dateStart"] == "2023-05-02T15:34:22+03:00"
    assert meta["dateEnd"] == "2026-05-29T13:07:50+03:00"


# ---- language detection ----

def test_detect_language():
    assert p2j.detect_language("Ärade talman! Det är viktigt att vi tar ansvar för detta.") == "sv"
    assert p2j.detect_language("Arvoisa puhemies! Tämä on tärkeä asia, ja se pitää hoitaa.") == "fi"
    # Finnish "en" (I don't) is not Swedish "en".
    assert p2j.detect_language("Arvoisa rouva puhemies! En pitkitä enempää, haluan vain kiittää.") == "fi"
    # A Swedish closing phrase does not turn a Finnish speech Swedish.
    assert p2j.detect_language(
        "Kunnioittakaamme siis toisiamme ja toistemme mielipiteitä. "
        "Kiitoksia näistä vuosista, tack så mycket!") == "fi"
    assert p2j.detect_language("") == "fi"


# ---- download helpers ----

def _rec(tila, start="2026-05-29T10:07:28+00:00"):
    return {"id": "x", "tila": tila, "aloitushetki": start}


def test_needs_refresh_only_for_recent_draft_text():
    day = datetime.date(2026, 6, 15)
    assert needs_refresh([_rec("Hyväksytty"), _rec("Käsitelty")], today=day)
    assert not needs_refresh([_rec("Hyväksytty"), _rec("Tarkistettu")], today=day)
    # Draft text of an old sitting is not fetched again.
    assert not needs_refresh([_rec("Käsitelty")], today=datetime.date(2026, 11, 1))
    assert not needs_refresh([_rec("Käsitelty", start=None)], today=day)


def test_sitting_discovery_filters_term_years():
    payload = {"results": [
        {"key": {"valtiopaivavuosi": "2026", "taysistuntonumero": "58"}, "docCount": 11},
        {"key": {"valtiopaivavuosi": "2019", "taysistuntonumero": "47"}, "docCount": 482},
        {"key": {"valtiopaivavuosi": "2024", "taysistuntonumero": "3"}, "docCount": 6},
        {"key": {"valtiopaivavuosi": None, "taysistuntonumero": "1"}, "docCount": 1},
    ]}
    sittings = parse_sitting_aggregation(payload)
    assert sittings[(2026, 58)] == 11
    assert len(sittings) == 3
    assert sittings_in_term(sittings, 2023) == [(2024, 3), (2026, 58)]
