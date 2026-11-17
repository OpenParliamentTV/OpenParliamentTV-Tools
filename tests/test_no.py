"""NO (Stortinget) adapter: Referat parser, merger, Qbrick, HTTP and freshness.

Fixtures under ``tests/fixtures/NO/`` are synthetic, modelled on the
publikasjon XML (``forhandlinger.dtd``) and the ``eksport/moter`` overview.
No spaCy or network access is needed: the parser is driven with a stub
sentencizer and ``urlopen`` / ``fetch_bytes`` are monkeypatched.
"""

from __future__ import annotations

import datetime
import email.message
import io
import json
import os
import re
import shutil
from pathlib import Path
from urllib.error import HTTPError

import pytest

from optv.parliaments.NO.common import Config, period_to_sesjonider
from optv.parliaments.NO.merger.merge_session import merge_session
from optv.parliaments.NO.parsers import proceedings2json as p2j
from optv.parliaments.NO.scraper import fetch_proceedings as fp
from optv.parliaments.NO.scraper import http_get
from optv.parliaments.NO.scraper import fetch_meetings as fm
from optv.parliaments.NO.scraper.fetch_meetings import sesjon_ended
from optv.parliaments.NO.scraper.qbrick import account_id_from_html, chapters_from_payload, tc_date_shift

FIX = Path(__file__).resolve().parent / "fixtures" / "NO"
REFERAT = "refs-202526-10-14"


class StubNLP:
    """Minimal sentencizer: split on sentence-final punctuation."""

    def __call__(self, text):
        parts = [p.strip() for p in re.split(r"(?<=[.!?»])\s+", text) if p.strip()]

        class S:
            def __init__(self, t):
                self.text = t

        class Doc:
            sents = [S(p) for p in parts]

        return Doc()


def _data_dir(tmp_path: Path, moteids=(11518,)) -> Config:
    meetings = tmp_path / "original" / "meetings"
    proceedings = tmp_path / "original" / "proceedings"
    meetings.mkdir(parents=True)
    proceedings.mkdir(parents=True)
    shutil.copy(FIX / "meetings-2025-2026.json", meetings / "2025-2026.json")
    for moteid in moteids:
        shutil.copy(FIX / "referat-sample.xml", proceedings / f"{moteid}.xml")
    return Config(tmp_path)


def _parse(tmp_path: Path, moteid: int = 11518) -> dict:
    config = _data_dir(tmp_path, (moteid,))
    return p2j.parse_proceedings_for_meeting(config, 22, moteid, nlp=StubNLP())


def _texts(block: dict) -> list[str]:
    return [s["text"] for s in block["sentences"]]


# ---- parser ----

def test_parser_emits_every_agenda_container(tmp_path):
    data = _parse(tmp_path)["data"]
    assert [s["speechIndex"] for s in data] == list(range(1, 10))
    assert [s["debug"]["proceedingIndex"] for s in data] == list(range(1, 10))
    agenda = [(s["agendaItem"].get("id"), s["agendaItem"].get("nativeType"),
               s["agendaItem"]["type"]) for s in data]
    assert agenda == [
        (None, None, "procedural"),
        (None, None, "procedural"),
        ("sak-1", "NO-trontaledebatt", "regular"),
        ("sak-1", "NO-trontaledebatt", "regular"),
        ("sak-2", "NO-lovforslag", "regular"),          # number from Sakshode/Saknr
        ("sak-3", "NO-muntlig_sporretime", "qa"),
        ("sak-3", "NO-muntlig_sporretime", "qa"),
        ("sak-4", "NO-interpellasjon", "questioning_of_the_government"),
        ("votering-1", "NO-votering", "voting"),
    ]
    assert data[4]["agendaItem"]["officialTitle"] == "sak nr. 2"
    assert data[8]["agendaItem"]["title"] == "Votering"


def test_parser_folds_untimed_elements_into_preceding_record(tmp_path):
    data = _parse(tmp_path)["data"]
    # Leading untimed remark → prepended to the first timed record.
    first = data[0]["textContents"][0]["textBody"]
    assert _texts(first[0]) == ["Representanten Ola Nordmann har igjen tatt sete."]
    assert _texts(first[1]) == ["Møtet er satt."]
    assert data[0]["debug"]["foldedElements"] == 1
    # President's untimed "Takk." → appended to Kari Nordmann's speech.
    body = data[2]["textContents"][0]["textBody"]
    assert [b["speakerstatus"] for b in body] == ["main-speaker", "president"]
    assert _texts(body[1]) == ["Takk."]
    # Voting block: untimed remark, untimed vote explanation and the
    # [00:00:00] placeholder of the following Sakreferat all join the timed
    # opening remark of the vote.
    vote = data[8]
    blocks = vote["textContents"][0]["textBody"]
    assert [(b["speaker"], b["speakerstatus"]) for b in blocks] == [
        ("Presidenten", "president"),
        ("Presidenten", "president"),
        ("Per Hansen", "speaker"),
        ("Presidenten", "president"),
    ]
    assert vote["debug"]["foldedElements"] == 3
    assert _texts(blocks[3]) == ["Ingen merknader."]
    # The member speaking inside the president's clip is listed as speaker.
    assert vote["people"][1] == {"label": "Per Hansen", "context": "speaker",
                                 "faction": {"label": "FrP"}, "originPersonID": "PHAN"}


def test_parser_text_includes_quotes_and_lists_but_not_tables(tmp_path):
    data = _parse(tmp_path)["data"]
    texts = _texts(data[2]["textContents"][0]["textBody"][0])
    assert texts == ["Første avsnitt.", "Andre avsnitt.", "«Sitert tekst.»", "Punkt en."]
    vote_texts = _texts(data[8]["textContents"][0]["textBody"][1])
    assert "«Stortinget ber regjeringen.»" in vote_texts


def test_parser_speaker_fields_and_reply_flag(tmp_path):
    data = _parse(tmp_path)["data"]
    pres = data[0]
    assert pres["people"] == [{"label": "Presidenten", "context": "president", "role": "president"}]
    assert pres["textContents"][0]["textBody"][-1]["type"] == "speech"
    minister = data[4]["people"][0]
    assert minister["role"] == "statsminister"
    assert minister["originPersonID"] == "KNOR"
    assert data[3]["isReply"] is True and data[2]["isReply"] is False


def test_parser_datestart_has_oslo_offset_across_dst(tmp_path):
    october = _parse(tmp_path / "oct", 11518)
    november = _parse(tmp_path / "nov", 11525)
    assert october["data"][0]["dateStart"] == "2025-10-14T10:00:05+02:00"
    assert november["data"][0]["dateStart"] == "2025-11-05T10:00:05+01:00"
    assert october["meta"]["dateStart"] == "2025-10-14T10:00:05+02:00"


def test_parser_text_ids_use_referat_and_clock_time(tmp_path):
    data = _parse(tmp_path)["data"]
    ids = [s["textContents"][0]["originTextID"] for s in data]
    # Keyed by clock time (the Qbrick reference_id form), not by the element
    # Id, which changes with every Referat version.
    assert ids[0] == f"{REFERAT}-10:00:05"
    assert ids[1] == f"{REFERAT}-10:00:20"
    assert ids[8] == f"{REFERAT}-10:40:00"
    assert len(set(ids)) == len(ids)


def test_text_id_collision_and_untimed_fallback():
    used: set[str] = set()
    el = p2j.ET.fromstring('<Hovedinnlegg Id="i9"/>')
    assert p2j._text_id(el, REFERAT, "10:00:00", "x", used) == f"{REFERAT}-10:00:00"
    assert p2j._text_id(el, REFERAT, "10:00:00", "x", used) == f"{REFERAT}-10:00:00-2"
    assert p2j._text_id(el, REFERAT, None, "x", used) == f"{REFERAT}-i9"


def test_parse_navn_treats_midnight_placeholder_as_untimed():
    el = p2j.ET.fromstring("<Navn>Presidenten [00:00:00]:</Navn>")
    assert p2j.parse_navn(el)["time"] is None


def test_clock_rolls_over_past_midnight():
    clock = p2j._Clock(datetime.date(2025, 12, 1))
    assert clock.at("23:58:00").date() == datetime.date(2025, 12, 1)
    assert clock.at("00:02:00").date() == datetime.date(2025, 12, 2)


def test_period_mapping_covers_older_terms():
    assert period_to_sesjonider(20)[0] == "2017-2018"
    assert period_to_sesjonider(21)[-1] == "2024-2025"
    assert period_to_sesjonider(22)[0] == "2025-2026"


# ---- merger ----

TC_IN = "2025-10-14T07:58:00+00:00"   # 09:58:00 Oslo


def _write_media(config: Config, chapters: list[dict]) -> None:
    doc = {
        "meta": {"session": "22_11518", "processing": {"parse_media": "2025-10-15T00:00:00"}},
        "moteid": 11518,
        "parts": [{
            "moteid": 11518, "delnr": 1, "qbvid": "qb1",
            "mp4_url": "https://example.org/v_1080p.mp4",
            "audio_mp4_url": "https://example.org/v_240p.mp4",
            "hls_url": None, "tc_in_utc": TC_IN,
            "tc_out_utc": "2025-10-14T08:58:00+00:00", "duration_seconds": 3600.0,
            "thumbnail_url": None, "chapters": chapters,
        }],
    }
    out = config.file("22_11518", "media", create=True)
    out.write_text(json.dumps(doc))


def _merge(tmp_path: Path, chapters: list[dict]) -> dict:
    doc = _parse(tmp_path)
    config = Config(tmp_path)
    config.file("22_11518", "proceedings", create=True).write_text(json.dumps(doc))
    _write_media(config, chapters)
    return merge_session(config, "22_11518")


def test_merge_source_page_uses_msid_and_offsets(tmp_path):
    merged = _merge(tmp_path, [])
    first = merged["data"][0]
    assert first["media"]["sourcePage"].endswith("?meid=11518&del=1&msid=125")
    assert first["media"]["additionalInformation"]["startOffset"] == 125.0
    assert first["media"]["videoFileURI"] == "https://example.org/v_1080p.mp4#t=125.000,140.000"
    assert first["dateEnd"] == "2025-10-14T10:00:20+02:00"
    assert all(s["media"]["videoFileURI"] for s in merged["data"])


def test_merge_last_speech_of_part_uses_chapter_end(tmp_path):
    chapters = [
        {"start": 0.0, "end": 125.0, "name": "Presidenten"},
        {"start": 125.0, "end": 140.0, "name": "Presidenten"},
        {"start": 2520.0, "end": 2700.0, "name": "Presidenten"},
    ]
    data = _merge(tmp_path, chapters)["data"]
    assert data[0]["debug"]["merge"]["durationSource"] == "chapter"
    assert data[1]["debug"]["merge"]["durationSource"] == "next-speech"
    last = data[-1]
    assert last["debug"]["merge"]["durationSource"] == "chapter"
    assert last["media"]["duration"] == 180.0
    assert last["dateEnd"] == "2025-10-14T10:43:00+02:00"


def test_merge_last_speech_without_chapter_runs_to_part_end(tmp_path):
    last = _merge(tmp_path, [])["data"][-1]
    assert last["debug"]["merge"]["durationSource"] == "part-end"
    assert last["media"]["duration"] == 1080.0


def test_merge_meta_range_is_first_start_to_last_end(tmp_path):
    meta = _merge(tmp_path, [])["meta"]
    assert meta["dateStart"] == "2025-10-14T10:00:05+02:00"
    assert meta["dateEnd"] == "2025-10-14T10:58:00+02:00"


# ---- Qbrick ----

def test_account_id_from_archive_html():
    html = '{"qbrickAccountId":"AccX","qbrickVideoId":"vid1"}'
    assert account_id_from_html(html) == "AccX"
    assert account_id_from_html("<html></html>") is None


def test_chapters_are_cleaned_and_sorted():
    payload = {"tracks": [
        {"id": "evnt", "sequences": [{"id": "0"}]},
        {"id": "cptr", "sequences": [
            {"type": "chapter", "id": "2", "start": 200.0, "end": 300.0, "name": "B"},
            {"type": "chapter", "id": "1"},                       # malformed
            {"type": "chapter", "id": "0", "start": 0.0, "end": 200.0, "name": "A"},
            {"type": "chapter", "id": "3", "start": 300.0, "end": 300.0, "name": "C"},
        ]},
    ]}
    assert chapters_from_payload(payload) == [
        {"start": 0.0, "end": 200.0, "name": "A"},
        {"start": 200.0, "end": 300.0, "name": "B"},
    ]
    assert chapters_from_payload({"tracks": [{"id": "cptr", "name": "Chapters"}]}) == []
    assert chapters_from_payload({}) == []


def test_tc_in_before_part_date_is_moved_to_that_date():
    assert tc_date_shift("2025-11-03T08:55:00+00:00", "2025-11-04") == datetime.timedelta(days=1)
    assert tc_date_shift("2025-11-04T08:55:00+00:00", "2025-11-04") is None
    # A part running past midnight starts on the next day: no correction.
    assert tc_date_shift("2025-11-04T23:10:00+00:00", "2025-11-04") is None
    assert tc_date_shift("2025-11-04T08:55:00+00:00", None) is None


# ---- HTTP helper ----

class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _http_error(code: int, retry_after: str | None = None) -> HTTPError:
    headers = email.message.Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return HTTPError("https://data.stortinget.no/x", code, "err", headers, None)


@pytest.mark.parametrize("retry_after,expected_wait", [("3", 3.0), (None, http_get.RATE_LIMIT_WAIT)])
def test_fetch_bytes_retries_429(monkeypatch, retry_after, expected_wait):
    calls = []

    def fake_urlopen(req, timeout):
        calls.append(req.full_url)
        if len(calls) == 1:
            raise _http_error(429, retry_after)
        return _Resp(b"ok")

    waits = []
    monkeypatch.setattr(http_get, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_get.time, "sleep", waits.append)
    monkeypatch.setattr(http_get, "_last_request", {})
    assert http_get.fetch_bytes("https://data.stortinget.no/x", retry_count=3) == b"ok"
    assert len(calls) == 2
    assert expected_wait in waits


def test_fetch_bytes_does_not_retry_404(monkeypatch):
    def fake_urlopen(req, timeout):
        raise _http_error(404)

    monkeypatch.setattr(http_get, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_get.time, "sleep", lambda s: None)
    with pytest.raises(HTTPError):
        http_get.fetch_bytes("https://data.stortinget.no/x", retry_count=3)


# ---- download freshness ----

def test_sesjon_ended_after_september_of_end_year():
    assert not sesjon_ended("2025-2026", datetime.date(2026, 9, 30))
    assert sesjon_ended("2025-2026", datetime.date(2026, 10, 1))
    assert not sesjon_ended("2026-2027", datetime.date(2026, 11, 17))


def _overview(epoch_ms: int, ids: list[int]) -> dict:
    return {"respons_dato_tid": f"/Date({epoch_ms}+0200)/", "sesjon_id": "2025-2026",
            "moter_liste": [{"id": i, "referat_id": f"refs-{i}"} for i in ids]}


MAY_2026 = 1779914709282       # 2026-05-27, before 2025-2026 ended
NOV_2026 = 1794900000000       # 2026-11-17, after it ended


def test_overview_is_final_only_when_fetched_after_sesjon_end():
    assert not fm.overview_is_final(_overview(MAY_2026, [1]), "2025-2026")
    assert fm.overview_is_final(_overview(NOV_2026, [1]), "2025-2026")
    assert not fm.overview_is_final(_overview(NOV_2026, [1]), "2026-2027")


def test_overview_fetched_before_sesjon_end_is_refreshed(tmp_path, monkeypatch):
    config = Config(tmp_path)
    target = config.dir("meetings", create=True) / "2025-2026.json"
    target.write_text(json.dumps(_overview(MAY_2026, [1])))
    fetched = []

    def fake_fetch(url, **kw):
        fetched.append(url)
        return json.dumps(_overview(NOV_2026, [1, 2])).encode()

    monkeypatch.setattr(fm, "fetch_bytes", fake_fetch)
    fm.fetch_meetings(config, "2025-2026")
    assert len(fetched) == 1
    assert [m["id"] for m in json.loads(target.read_text())["moter_liste"]] == [1, 2]
    # Now final: served from the cache.
    fm.fetch_meetings(config, "2025-2026")
    assert len(fetched) == 1


def test_open_overview_unchanged_is_not_rewritten(tmp_path, monkeypatch):
    config = Config(tmp_path)
    target = config.dir("meetings", create=True) / "2026-2027.json"
    cached = _overview(NOV_2026, [5])
    cached["sesjon_id"] = "2026-2027"
    target.write_text(json.dumps(cached))
    os.utime(target, (1_000_000, 1_000_000))
    monkeypatch.setattr(fm, "fetch_bytes",
                        lambda url, **kw: json.dumps({**cached, "respons_dato_tid": "/Date(1794990000000+0100)/"}).encode())
    fm.fetch_meetings(config, "2026-2027")
    assert target.stat().st_mtime == 1_000_000


def test_referat_status_parsed_from_head():
    assert fp.referat_status(b'<Forhandlinger Id="refs-x" Status="Kvalitetssikret">') == "Kvalitetssikret"
    assert fp.referat_status(b"<Forhandlinger>") is None


def test_final_referat_is_served_from_cache(tmp_path, monkeypatch):
    config = _data_dir(tmp_path)
    monkeypatch.setattr(fp, "fetch_bytes", lambda *a, **k: pytest.fail("fetched a final Referat"))
    assert fp.fetch_proceedings_for_meeting(config, 11518) == tmp_path / "original" / "proceedings" / "11518.xml"


def test_non_final_referat_is_refetched_and_rewritten_only_on_change(tmp_path, monkeypatch):
    config = _data_dir(tmp_path)
    target = tmp_path / "original" / "proceedings" / "11518.xml"
    draft = b'<Forhandlinger Id="refs-x" Status="Kvalitetssikret"><Mote/></Forhandlinger>'
    target.write_bytes(draft)
    os.utime(target, (1_000_000, 1_000_000))

    monkeypatch.setattr(fp, "fetch_bytes", lambda *a, **k: draft)
    fp.fetch_proceedings_for_meeting(config, 11518)
    assert target.stat().st_mtime == 1_000_000          # unchanged bytes → untouched

    final = b'<Forhandlinger Id="refs-x" Status="Komplett"><Mote/></Forhandlinger>'
    monkeypatch.setattr(fp, "fetch_bytes", lambda *a, **k: final)
    fp.fetch_proceedings_for_meeting(config, 11518)
    assert target.read_bytes() == final
