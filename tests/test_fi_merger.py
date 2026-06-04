"""FI merger unit tests — the personNumber + start-time media-spine join.

Exercises the join helpers directly with in-memory proceedings / media dicts
shaped like the parser outputs, so no disk or network is touched.
"""

from __future__ import annotations

from optv.parliaments.FI.merger import merge_session as M


def _proc(person_number, start_local, speech_index, text="Hei."):
    return {
        "parliament": "FI",
        "agendaItem": {"officialTitle": "T", "title": "T", "type": "regular"},
        "speechIndex": speech_index,
        "originID": f"o{speech_index}",
        "originalLanguage": "fi",
        "dateStart": start_local,
        "people": [{"label": f"P{person_number}", "context": "main-speaker",
                    "faction": {"label": "ps"}}],
        "textContents": [{"type": "proceedings", "language": "FI",
                          "textBody": [{"type": "speech", "sentences": [{"text": text}]}]}],
        "debug": {"personNumber": str(person_number)},
    }


def _media(person_number, start_offset, ts_utc, is_reply=False, first="A", last="B"):
    return {
        "personNumber": person_number,
        "timeStamp": ts_utc,
        "topicId": "6",
        "isReply": is_reply,
        "party": {"fi": "ps"},
        "firstName": first,
        "lastName": last,
        "media": {"videoFileURI": "u#t=1,2", "audioFileURI": "u",
                  "duration": 28.0,
                  "additionalInformation": {"startOffset": start_offset}},
    }


def test_match_by_person_and_nearest_time():
    # Two speeches by the same member; each video clip must bind the nearest
    # PTK speech in time, not just the first.
    proc = [
        _proc(1391, "2026-05-29T13:07:28", 1, text="Eka."),
        _proc(1391, "2026-05-29T13:20:00", 2, text="Toka."),
    ]
    index = M._index_proceedings(proc)
    early = M._match_proceeding(_media(1391, 455, "2026-05-29T10:07:28.300Z"), index)
    late = M._match_proceeding(_media(1391, 1200, "2026-05-29T10:20:00.100Z"), index)
    assert early["originID"] == "o1"
    assert late["originID"] == "o2"
    # Both consumed; a third clip for the same person finds nothing.
    assert M._match_proceeding(_media(1391, 1500, "2026-05-29T10:25:00Z"), index) is None


def test_match_respects_tolerance():
    proc = [_proc(1391, "2026-05-29T13:07:28", 1)]
    index = M._index_proceedings(proc)
    # Video timestamp 30 min off the PTK time → beyond JOIN_TOLERANCE_S.
    assert M._match_proceeding(_media(1391, 455, "2026-05-29T10:40:00Z"), index) is None


def test_merge_one_grafts_text_and_keeps_media():
    proc = _proc(1391, "2026-05-29T13:07:28", 1, text="Sisältö.")
    proc["_utc"] = 0.0
    proc["_used"] = False
    speech = M.merge_one(_media(1391, 455, "2026-05-29T10:07:28Z", is_reply=True),
                         proc, "FI", 2023, 3058, 1)
    assert speech["electoralPeriod"] == {"number": 2023}
    assert speech["session"] == {"number": 3058}
    assert speech["isReply"] is True                       # reply flag from video
    assert speech["media"]["videoFileURI"] == "u#t=1,2"
    assert speech["textContents"][0]["textBody"][0]["sentences"][0]["text"] == "Sisältö."
    assert speech["originID"] == "o1"


def test_merge_one_media_only_when_no_text():
    speech = M.merge_one(_media(999, 100, "2026-05-29T10:00:00Z", first="Outi", last="Outo"),
                         None, "FI", 2023, 3058, 4)
    assert speech["textContents"] == []
    assert speech["debug"]["merge"]["text-missing"] is True
    assert speech["people"][0]["label"] == "Outi Outo"
    assert speech["people"][0]["faction"] == {"label": "ps"}
    assert speech["agendaItem"]["id"] == "kohta-6"


def test_helsinki_to_utc_dst_offset():
    # Late May → EEST (UTC+3): 13:07:28 local == 10:07:28 UTC.
    local = M._helsinki_to_utc_epoch("2026-05-29T13:07:28")
    utc = M._utc_epoch("2026-05-29T10:07:28Z")
    assert abs(local - utc) < 1.0
