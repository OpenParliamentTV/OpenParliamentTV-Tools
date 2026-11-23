"""NL parsers, session keys and scraper helpers, on the synthetic fixtures of ``tests/fixtures/NL``. No spaCy model and no network."""

from __future__ import annotations

import argparse
import datetime
import json

import pytest

from optv.parliaments.NL import align_prep
from optv.parliaments.NL.common import (
    Config, in_scope, parse_session_key, period_of, session_key, session_number, session_year_label, session_years,
)
from optv.parliaments.NL.parsers import media2json as m2j
from optv.parliaments.NL.parsers import proceedings2json as p2j
from optv.parliaments.NL.scraper import fetch
from optv.shared.agenda_types import (
    CORE_BUDGET, CORE_CLOSING, CORE_CONDOLENCE, CORE_OPENING, CORE_PROCEDURAL, CORE_QA, CORE_REGULAR,
    CORE_VOTING, classify_nl,
)

from .nl_fixtures import SESSION, parsed_media, parsed_proceedings, raw_debates, report_bytes


@pytest.fixture(scope="module")
def proceedings() -> dict:
    return parsed_proceedings()


def _speech(proceedings: dict, index: int) -> dict:
    return proceedings["data"][index - 1]


def _texts(speech: dict, block_type: str | None = None) -> list[str]:
    return [s["text"] for b in speech["textBody"] if block_type in (None, b["type"]) for s in b["sentences"]]


# ---- session keys and periods ----

def test_session_key_round_trip():
    assert session_key(2026, 5) == "2026-005"
    assert parse_session_key("2026-005") == (2026, 5)
    assert session_year_label(2026) == "2026-2027"
    with pytest.raises(ValueError):
        parse_session_key("2026-5")


def test_period_follows_the_first_sitting_of_a_term_not_the_session_year():
    # Session year 2025-2026 began under the previous House; sitting 12 was the new one's first.
    assert period_of("2025-011") is None
    assert period_of("2025-012") == 2025
    assert period_of("2026-005") == 2025
    assert period_of("not-a-key") is None


def test_session_number_keeps_the_session_years_of_a_term_apart():
    assert session_number(2025, "2025-094") == 94
    assert session_number(2025, "2026-005") == 1005
    assert session_number(2025, "2026-094") != session_number(2025, "2025-094")


def test_session_years_of_a_term_run_to_the_current_one():
    assert session_years(2025, 2026) == [2025, 2026]
    assert session_years(2025, 2025) == [2025]


def test_in_scope_applies_period_and_limit_session():
    args = argparse.Namespace(period=2025, limit_to_period=True, limit_session="")
    assert in_scope(args, "2026-005")
    assert not in_scope(args, "2025-011")          # the previous House
    assert not in_scope(args, "garbage")
    args.limit_session = r"^(2025-094|2026-005)$"
    assert in_scope(args, "2025-094")
    assert not in_scope(args, "2025-096")
    args.limit_to_period = False
    args.limit_session = ""
    assert in_scope(args, "2025-011")


# ---- agenda types ----

@pytest.mark.parametrize("kind,title,core", [
    ("Plenair debat", "Kansspelen", CORE_REGULAR),
    ("Plenair debat", "Begroting Buitenlandse Zaken 2027", CORE_BUDGET),
    ("Vragenuur", "Vragenuur", CORE_QA),
    ("Stemmingen", "Stemmingen", CORE_VOTING),
    ("Hamerstukken", "Hamerstukken", CORE_VOTING),
    ("Regeling van werkzaamheden", "Regeling van werkzaamheden", CORE_PROCEDURAL),
    ("Mededelingen", None, CORE_PROCEDURAL),
    ("Opening", None, CORE_OPENING),
    ("Sluiting", None, CORE_CLOSING),
    ("Herdenking", "Herdenking", CORE_CONDOLENCE),
    ("Een nieuw soort", "Iets", CORE_REGULAR),
])
def test_classify_nl(kind, title, core):
    assert classify_nl(kind, title) == (kind, core)


def test_classify_nl_without_a_kind():
    assert classify_nl(None) == (None, CORE_REGULAR)


# ---- speakers ----

@pytest.mark.parametrize("inverted,natural", [
    ("Dijk van", "van Dijk"),
    ("Brink van den", "van den Brink"),
    ("Abassi El", "El Abassi"),
    ("Palmen-Schlangen", "Palmen-Schlangen"),
    ("Bruins Slot", "Bruins Slot"),
    ("Van", "Van"),
    ("", ""),
])
def test_natural_surname(inverted, natural):
    assert p2j.natural_surname(inverted) == natural


def test_people_carry_name_faction_role_and_source_id(proceedings):
    member = _speech(proceedings, 2)["people"][0]
    assert member == {
        "type": "memberOfParliament", "label": "Bram de Vries", "firstname": "Bram", "lastname": "de Vries",
        "role": "lid Tweede Kamer", "faction": {"label": "DEF"},
        "originPersonID": "c0000000-0000-0000-0000-000000000002", "context": "main-speaker",
    }
    minister = _speech(proceedings, 3)["people"][0]
    assert minister["type"] == "guest"
    assert minister["role"] == "minister van Voorbeelden"
    assert "faction" not in minister


# ---- the report: speeches ----

def test_report_meta(proceedings):
    meta = proceedings["meta"]
    assert meta["session"] == SESSION
    assert meta["sourceLabel"] == "Handelingen 2025-2026, nr. 50"
    assert meta["sourceURI"] == "https://www.tweedekamer.nl/kamerstukken/plenaire_verslagen/detail/2025-2026/50"
    assert meta["report"]["kind"] == "Eindpublicatie" and meta["report"]["status"] == "Gecorrigeerd"
    # The report's clock times carry no zone; they are Dutch local time.
    assert meta["dateStart"] == "2026-03-03T10:15:00+01:00"
    assert meta["dateEnd"] == "2026-03-03T11:46:00+01:00"


def test_fourteen_turns_become_eight_speeches(proceedings):
    speeches = proceedings["data"]
    assert [s["speechIndex"] for s in speeches] == list(range(1, 9))
    assert [s["people"][0]["label"] for s in speeches] == [
        "Anna Jansen", "Bram de Vries", "Daan Smit", "Daan Smit", "Daan Smit",
        "Bram de Vries", "Carla Bakker", "Anna Jansen"]
    starts = [datetime.datetime.fromisoformat(s["dateStart"]) for s in speeches]
    ends = [datetime.datetime.fromisoformat(s["dateEnd"]) for s in speeches]
    assert all(end <= nxt for end, nxt in zip(ends, starts[1:])), "speeches overlap"


def test_a_short_chair_turn_opens_the_speech_it_introduces(proceedings):
    speech = _speech(proceedings, 2)
    # The chair's eight seconds before the member are part of the member's speech.
    assert speech["dateStart"] == "2026-03-03T10:15:12+01:00"
    assert speech["dateEnd"] == "2026-03-03T10:17:20+01:00"
    assert speech["debug"]["turns"] == 2
    assert speech["originTextID"] == "d0000000-0000-0000-0000-000000000030"
    spoken = [b for b in speech["textBody"] if b["type"] == "speech"]
    assert (spoken[0]["speaker"], spoken[0]["speakerstatus"]) == ("Anna Jansen", "president")
    assert (spoken[1]["speaker"], spoken[1]["speakerstatus"]) == ("Bram de Vries", "main-speaker")
    assert [(p["label"], p["context"]) for p in speech["people"]] == [
        ("Bram de Vries", "main-speaker"), ("Anna Jansen", "president"), ("Carla Bakker", "speaker")]


def test_speaker_lines_are_not_part_of_the_text(proceedings):
    # "Mevrouw Bakker (GHI):", "De voorzitter:", "Minister Smit:" open a contribution in the report.
    for speech in proceedings["data"]:
        for text in _texts(speech, "speech"):
            assert not text.startswith(("De voorzitter:", "Minister Smit:"))
            assert "(DEF):" not in text and "(GHI):" not in text


def test_a_motion_is_read_by_its_mover_and_its_registration_is_record_text(proceedings):
    speech = _speech(proceedings, 2)
    by_speaker = [(b["speaker"], s["text"]) for b in speech["textBody"] if b["type"] == "speech" for s in b["sentences"]]
    assert ("Bram de Vries", "verzoekt de regering iets te doen,") in by_speaker
    comments = _texts(speech, "comment")
    assert "De voorzitter: Deze motie is voorgesteld door het lid De Vries." in comments
    assert "Zij krijgt nr. 7 (12345)." in comments


def test_interruptions_keep_their_speakers(proceedings):
    speech = _speech(proceedings, 2)
    statuses = {b["speaker"]: b["speakerstatus"] for b in speech["textBody"] if b["speaker"]}
    assert statuses == {"Anna Jansen": "president", "Bram de Vries": "main-speaker", "Carla Bakker": "speaker"}


def test_record_text_is_a_comment_of_one_sentence(proceedings):
    speech = _speech(proceedings, 2)
    comments = [b for b in speech["textBody"] if b["type"] == "comment"]
    assert all(b["speaker"] is None and b["speakerstatus"] is None for b in comments)
    assert all(len(b["sentences"]) == 1 for b in comments)
    # The heading that only repeats the title is dropped, the announcement kept.
    assert _texts(speech)[0] == "Aan de orde is de behandeling van het wetsvoorstel."
    assert "Voorbeeldwet" not in _texts(speech)
    # The suspension closes the speech before it.
    assert _texts(speech)[-1] == "De vergadering wordt van 10.17 uur tot 10.30 uur geschorst."


def test_a_suspension_ends_the_joining(proceedings):
    # The chair's five seconds after the suspension belong to the minister's speech, not to the member's.
    assert _texts(_speech(proceedings, 3), "speech")[0] == "Ik heropen de vergadering."
    assert "Ik heropen de vergadering." not in _texts(_speech(proceedings, 2))


def test_a_long_turn_is_cut_where_its_speaker_resumes(proceedings):
    parts = [_speech(proceedings, i) for i in (3, 4, 5)]
    assert [p["debug"]["part"] for p in parts] == ["1/3", "2/3", "3/3"]
    assert [(p["dateStart"][11:19], p["dateEnd"][11:19]) for p in parts] == [
        ("10:30:00", "10:55:05"), ("10:55:05", "11:15:05"), ("11:15:05", "11:40:05")]
    # The first part keeps the turn's id; the others take the id of the passage they start with.
    assert [p["originTextID"][-4:] for p in parts] == ["0050", "0053", "0055"]
    assert _texts(parts[1], "speech") == ["Tweede blok.", "U vervolgt."]
    for part in parts:
        seconds = (datetime.datetime.fromisoformat(part["dateEnd"])
                   - datetime.datetime.fromisoformat(part["dateStart"])).total_seconds()
        assert seconds <= p2j.MAX_SPEECH_SECONDS


def test_short_turns_join_the_speech_before_them(proceedings):
    speech = _speech(proceedings, 6)
    assert speech["debug"]["turns"] == 4
    assert (speech["dateStart"][11:19], speech["dateEnd"][11:19]) == ("11:40:10", "11:40:55")
    assert [(b["speaker"], b["sentences"][0]["text"]) for b in speech["textBody"]] == [
        ("Bram de Vries", "Ik vraag een debat aan over voorbeelden."),
        ("Anna Jansen", "Is daar steun voor?"),
        ("Carla Bakker", "Steun."),
        ("Anna Jansen", "Er is een meerderheid."),
    ]


def test_a_pause_ends_the_joining(proceedings):
    # A minute after the last turn: a speech of its own, short as it is.
    speech = _speech(proceedings, 7)
    assert speech["people"][0]["label"] == "Carla Bakker"
    assert speech["debug"]["turns"] == 1
    assert _texts(speech) == ["Een punt van orde."]


def test_a_run_of_votes_is_one_speech_of_the_chair(proceedings):
    speech = _speech(proceedings, 8)
    assert speech["debug"]["kind"] == "voting"
    assert (speech["dateStart"][11:19], speech["dateEnd"][11:19]) == ("11:45:00", "11:45:40")
    assert [(p["label"], p["context"]) for p in speech["people"]] == [("Anna Jansen", "president")]
    assert speech["agendaItem"]["title"] == "Stemmingen moties Voorbeeldwet"
    assert speech["agendaItem"]["officialTitle"] == "Stemmingen moties Voorbeeldwet"
    assert speech["agendaItem"]["type"] == CORE_VOTING
    assert [(b["type"], b["sentences"][0]["text"]) for b in speech["textBody"]][2:] == [
        ("comment", "In stemming komt de motie-De Vries (12345, nr. 7)."),
        ("speech", "Ik constateer dat deze motie is aangenomen."),
        ("comment", "In stemming komt de motie-De Vries (12345, nr. 8)."),
        ("speech", "Ik constateer dat deze motie is verworpen."),
    ]


def test_the_opening_carries_its_record_text(proceedings):
    speech = _speech(proceedings, 1)
    assert speech["agendaItem"]["type"] == CORE_OPENING
    assert _texts(speech, "comment") == ["Voorzitter: Jansen", "Aanwezig zijn 3 leden der Kamer."]
    assert _texts(speech, "speech") == ["Ik open de vergadering."]


def test_an_agenda_item_without_a_turn_gives_no_speech(proceedings):
    # The closing holds only the line "Sluiting 11.46 uur."
    assert all(s["agendaItem"]["nativeType"] != "Sluiting" for s in proceedings["data"])


def test_agenda_item_of_a_debate(proceedings):
    assert _speech(proceedings, 2)["agendaItem"] == {
        "officialTitle": "Wijziging van de Voorbeeldwet (12345)", "title": "Voorbeeldwet",
        "id": "a0000000-0000-0000-0000-000000000002", "type": CORE_REGULAR, "nativeType": "Plenair debat",
    }
    assert _speech(proceedings, 2)["activityId"] == "a0000000-0000-0000-0000-000000000002"


# ---- grouping and cutting on their own ----

def _turn(start: int, seconds: int, *, agenda: str = "a", kind: str = "speech") -> p2j.Turn:
    t0 = datetime.datetime(2026, 3, 3, 10, 0, tzinfo=p2j.AMSTERDAM) + datetime.timedelta(seconds=start)
    return p2j.Turn(kind, f"t{start}", t0, t0 + datetime.timedelta(seconds=seconds), {"label": f"P{start}"}, False,
                    {"id": agenda}, agenda)


def test_group_turns_gives_leading_short_turns_to_the_next_speech():
    speeches = p2j.group_turns([_turn(0, 5), _turn(5, 5), _turn(10, 60)])
    assert [[t.origin_id for t in s.turns] for s in speeches] == [["t0", "t5", "t10"]]
    assert speeches[0].lead.origin_id == "t10"


def test_group_turns_keeps_short_turns_alone_when_nothing_is_near():
    speeches = p2j.group_turns([_turn(0, 5), p2j.BREAK, _turn(600, 5), _turn(605, 3)])
    assert [[t.origin_id for t in s.turns] for s in speeches] == [["t0"], ["t600", "t605"]]
    assert speeches[1].lead.origin_id == "t600"         # the longest of them gives the speaker


def test_group_turns_does_not_join_across_agenda_items():
    speeches = p2j.group_turns([_turn(0, 60, agenda="a"), _turn(60, 5, agenda="b")])
    assert [[t.origin_id for t in s.turns] for s in speeches] == [["t0"], ["t60"]]


def test_group_turns_stops_joining_when_a_speech_would_grow_too_long():
    speeches = p2j.group_turns([_turn(0, 60), _turn(60, 5), _turn(65, 5)], max_seconds=66)
    assert [[t.origin_id for t in s.turns] for s in speeches] == [["t0", "t60"], ["t65"]]


def test_split_long_turn_leaves_a_turn_without_resumption_points_whole():
    turn = _turn(0, 4000)
    turn.blocks = [p2j.Block("speech", ["a"], turn.speaker, False, turn.start, "b1")]
    assert p2j.split_long_turn(turn) == [turn]


# ---- Debat Direct records ----

def test_debate_record():
    debate = next(d for d in parsed_media()["data"] if d["name"] == "Voorbeeldwet")
    assert debate["stream"] == "https://stream.example.org/2026-03-03/plenairezaal/index.m3u8"   # no player parameters
    assert debate["sourcePage"] == "https://debatdirect.tweedekamer.nl/2026-03-03/voorbeeld/plenaire-zaal/voorbeeldwet-12345-10-15"
    assert debate["dateStart"] == "2026-03-03T10:15:10+01:00"
    assert debate["date"] == "2026-03-03"
    # The source lists moments newest first; the record has them in order.
    times = [e["time"] for e in debate["events"]]
    assert times == sorted(times)
    assert debate["events"][1] == {"time": "2026-03-03T10:15:12+01:00", "type": "chairman",
                                   "objectId": "c0000000-0000-0000-0000-000000000001"}


def test_a_debate_without_category_is_filed_under_the_default():
    order = next(d for d in parsed_media()["data"] if d["debateType"] == "Regeling van werkzaamheden")
    assert "/2026-03-03/overig/plenaire-zaal/" in order["sourcePage"]


def test_debates_are_in_time_order():
    starts = [d["dateStart"] for d in parsed_media()["data"]]
    assert starts == sorted(starts) and len(starts) == 3


def test_a_debate_without_recording_has_no_stream():
    raw = raw_debates()
    del next(iter(raw["debates"].values()))["video"]
    assert sum(1 for d in m2j.parse_debates(raw, SESSION)["data"] if d["stream"] is None) == 1


# ---- scraper helpers ----

def _version(id_, kind, changed, content_type="text/xml"):
    return {"Id": id_, "Soort": kind, "Status": "x", "ContentType": content_type, "GewijzigdOp": changed}


def test_choose_report_prefers_the_final_version():
    versions = [
        _version("a", "Tussenpublicatie", "2026-03-03T12:00:00+01:00"),
        _version("b", "Eindpublicatie", "2026-03-20T12:00:00+01:00"),
        _version("c", "Tussenpublicatie", "2026-03-21T09:00:00+01:00"),
    ]
    chosen = fetch.choose_report(versions)
    assert chosen["Id"] == "b" and fetch.is_final(chosen)


def test_choose_report_takes_the_newest_interim_version_until_then():
    versions = [
        _version("a", "Tussenpublicatie", "2026-03-03T12:00:00+01:00"),
        _version("b", "Tussenpublicatie", "2026-03-03T23:50:00+01:00"),
        _version("pdf", "Tussenpublicatie", "2026-03-04T08:00:00+01:00", content_type="application/pdf"),
    ]
    chosen = fetch.choose_report(versions)
    assert chosen["Id"] == "b" and not fetch.is_final(chosen)
    assert fetch.choose_report([]) is None and not fetch.is_final(None)


def test_sitting_key_from_a_listing_record():
    assert fetch._sitting_key({"Vergaderjaar": "2026-2027", "Nummer": 5}) == "2026-005"
    assert fetch._sitting_key({"Vergaderjaar": "2026-2027", "Nummer": None}) is None
    assert fetch._sitting_key({"Vergaderjaar": "", "Nummer": 5}) is None


def test_report_activities_lists_agenda_items_with_their_day():
    activities = fetch.report_activities(report_bytes())
    assert len(activities) == 5
    assert activities[1] == ("a0000000-0000-0000-0000-000000000002", "2026-03-03")


def test_odata_url_keeps_the_query_syntax_readable_for_the_server():
    url = fetch._odata_url("Vergadering", filter="Nummer eq 5 and Vergaderjaar eq '2026-2027'", orderby="Nummer")
    assert url == ("https://gegevensmagazijn.tweedekamer.nl/OData/v5/Vergadering"
                   "?$filter=Nummer%20eq%205%20and%20Vergaderjaar%20eq%20%272026-2027%27&$orderby=Nummer")


# ---- audio for alignment ----

MASTER = """#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Original Audio",DEFAULT=YES,URI="stream_06/prog_index.m3u8?start=a&end=b"
#EXT-X-STREAM-INF:BANDWIDTH=397989,RESOLUTION=320x180,AUDIO="audio"
stream_01/prog_index.m3u8?start=a&end=b
"""


def test_audio_playlist_is_read_from_the_master():
    master_url = "https://stream.example.org/2026-03-03/plenairezaal/index.m3u8?start=a&end=b"
    assert align_prep.audio_playlist(master_url, MASTER) == (
        "https://stream.example.org/2026-03-03/plenairezaal/stream_06/prog_index.m3u8?start=a&end=b")
    assert align_prep.audio_playlist(master_url, "#EXTM3U\n") == master_url


def test_audio_is_staged_only_for_speeches_with_text_and_alignable_length():
    def speech(duration, text=True):
        body = [{"type": "speech", "sentences": [{"text": "x"}]}] if text else []
        return {"media": {"videoFileURI": "https://s/index.m3u8?start=a&end=b", "duration": duration},
                "textContents": [{"textBody": body}] if text else []}

    spec = align_prep._extract(speech(120.0))
    assert spec.source_url.endswith("index.m3u8?start=a&end=b") and spec.session_key is None
    assert align_prep._extract(speech(120.0, text=False)) is None
    too_long = speech(align_prep.MAX_ALIGN_SECONDS + 1.0)
    assert align_prep._extract(too_long) is None
    assert too_long["debug"]["alignSkip"].startswith("clip-too-long")


# ---- a replaced report ----

class _FakeSource:
    """Answers the scraper's requests from the fixtures and counts them."""

    def __init__(self):
        self.requests = 0

    def get(self, url):
        self.requests += 1
        return report_bytes()

    def get_json(self, url):
        self.requests += 1
        raw = raw_debates()
        if url.endswith("/agenda/2026-03-03"):
            return {"debates": raw["days"]["2026-03-03"]}
        return raw["debates"][url.rsplit("/", 1)[-1]]


def _sitting(*versions):
    return {"Vergaderjaar": "2025-2026", "Nummer": 50, "Datum": "2026-03-03T00:00:00+01:00", "Verslag": list(versions)}


def test_fetch_sitting_writes_three_files_and_skips_an_unchanged_report(tmp_path):
    config = Config(tmp_path)
    interim = _version("interim", "Tussenpublicatie", "2026-03-03T23:50:00+01:00")
    odata, debatdirect = _FakeSource(), _FakeSource()

    assert fetch.fetch_sitting(config, SESSION, _sitting(interim), odata, debatdirect)
    assert config.raw_sitting(SESSION).exists() and config.raw_report(SESSION).exists()
    stored = json.loads(config.raw_debates(SESSION).read_text())
    # One agenda request and one per debate the report names and Debat Direct lists: not the opening, not the closing.
    assert (odata.requests, debatdirect.requests) == (1, 4)
    assert len(stored["debates"]) == 3 and stored["reportId"] == "interim"

    assert not fetch.fetch_sitting(config, SESSION, _sitting(interim), odata, debatdirect)
    assert (odata.requests, debatdirect.requests) == (1, 4)
    assert not (config.dir("cache") / fetch.UPGRADES_FILE).exists()


def test_a_replaced_report_is_queued_for_a_rebuild_once(tmp_path):
    config = Config(tmp_path)
    interim = _version("interim", "Tussenpublicatie", "2026-03-03T23:50:00+01:00")
    final = _version("final", "Eindpublicatie", "2026-03-20T12:00:00+01:00")
    odata, debatdirect = _FakeSource(), _FakeSource()

    fetch.fetch_sitting(config, SESSION, _sitting(interim), odata, debatdirect)
    assert fetch.fetch_sitting(config, SESSION, _sitting(interim, final), odata, debatdirect)
    queue = config.dir("cache") / fetch.UPGRADES_FILE
    assert queue.read_text().split() == [SESSION]

    fetch.queue_report_upgrade(config, SESSION)
    assert queue.read_text().split() == [SESSION]


def test_a_sitting_without_a_report_is_skipped(tmp_path):
    source = _FakeSource()
    assert not fetch.fetch_sitting(Config(tmp_path), SESSION, _sitting(), source, source)
    assert source.requests == 0
