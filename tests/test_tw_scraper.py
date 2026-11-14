"""TW scraper tests: retry/backoff, incremental downloads, discovery by date,
dataset-221 speech kinds and the official-text re-check policy.

No network: ``urlopen`` and the API client are replaced by fakes.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from optv.parliaments.TW.common import Config
from optv.parliaments.TW.scraper import ly_api
from optv.parliaments.TW.scraper.fetch_media import (
    discover_plenary_meetings,
    fetch_meeting_index,
)
from optv.parliaments.TW.scraper.fetch_proceedings import fetch_proceedings_for_session
from optv.parliaments.TW.scraper.fetch_records import (
    download_records,
    fetch_records_for_session,
)
from optv.parliaments.TW.scraper.fetch_speech_kinds import (
    fetch_speech_kinds_for_session,
    iso_date,
    roc_date,
)
from optv.parliaments.TW.scraper.ly_api import LYApiClient

SESSION = "1106005"
CODE = "院會-11-6-5"


def _clip(ivod_id, day, name="張智倫", start="10:00:00"):
    return {
        "IVOD_ID": ivod_id, "影片種類": "Clip", "日期": day, "委員名稱": name,
        "開始時間": f"{day}T{start}+08:00", "支援功能": ["ai-transcript"],
        "會議資料": {"會議代碼": CODE, "標題": "第11屆第6會期第5次會議"},
    }


def _write_ivods(config, clips, session=SESSION):
    config.file(session, "ivods", create=True).write_text(json.dumps(
        {"meta": {"meetingCode": CODE, "session": session}, "ivods": clips},
        ensure_ascii=False))


# ---- HTTP retry ----

class _Resp:
    def __init__(self, body):
        self.body = body

    def read(self):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _client(monkeypatch, responses, sleeps):
    calls = iter(responses)

    def fake_urlopen(req, timeout=None, context=None):
        r = next(calls)
        if isinstance(r, Exception):
            raise r
        return _Resp(r)

    monkeypatch.setattr(ly_api, "urlopen", fake_urlopen)
    monkeypatch.setattr(ly_api.time, "sleep", lambda s: sleeps.append(s))
    return LYApiClient(min_interval=0, retry_count=4, retry_delay_max=10)


def _http_error(code, headers=None):
    return HTTPError("https://ly.govapi.tw/v2/ivods/1", code, "x", headers or {}, None)


def test_retries_429_with_backoff(monkeypatch):
    sleeps = []
    client = _client(monkeypatch, [_http_error(429), _http_error(429), b'{"ok": 1}'], sleeps)
    assert client.get_json("ivods/1", use_cache=False) == {"ok": 1}
    assert sleeps == [1.0, 2.0]


def test_honours_numeric_retry_after(monkeypatch):
    sleeps = []
    client = _client(monkeypatch, [_http_error(429, {"Retry-After": "3"}), b"{}"], sleeps)
    client.get_json("ivods/1", use_cache=False)
    assert sleeps == [3.0]


def test_does_not_retry_404(monkeypatch):
    sleeps = []
    client = _client(monkeypatch, [_http_error(404)], sleeps)
    with pytest.raises(HTTPError):
        client.get_json("ivods/1", use_cache=False)
    assert sleeps == []


def test_detail_without_whisperx_is_not_cached(monkeypatch, tmp_path):
    no_wx = json.dumps({"data": {"IVOD_ID": 1, "transcript": {}}}).encode()
    with_wx = json.dumps({"data": {"IVOD_ID": 1, "transcript": {
        "whisperx": [{"start": 0, "end": 1, "text": "好"}]}}}).encode()
    client = _client(monkeypatch, [no_wx, with_wx], [])
    client.cache_dir = tmp_path
    assert not client.get_ivod(1)["transcript"]
    assert client.get_ivod(1)["transcript"]["whisperx"]  # fetched again
    assert len(list(tmp_path.iterdir())) == 1  # now cached


# ---- incremental details ----

class _FakeApi:
    def __init__(self, fail=()):
        self.fail = set(fail)
        self.calls = []

    def get_ivod(self, ivod_id):
        self.calls.append(ivod_id)
        if ivod_id in self.fail:
            raise HTTPError("u", 429, "Too Many Requests", {}, None)
        return {"IVOD_ID": ivod_id, "transcript": {"whisperx": [{"text": "x"}]}}


def test_details_fetch_only_missing_clips_and_retry_failures(tmp_path):
    config = Config(tmp_path)
    _write_ivods(config, [_clip(1, "2026-10-30"), _clip(2, "2026-10-30"), _clip(3, "2026-11-03")])

    api = _FakeApi(fail={3})
    out = fetch_proceedings_for_session(SESSION, config, api)
    assert [d["IVOD_ID"] for d in json.loads(out.read_text())["ivods"]] == [1, 2]

    api = _FakeApi()
    fetch_proceedings_for_session(SESSION, config, api)
    assert api.calls == [3]  # only the clip that failed last time
    assert [d["IVOD_ID"] for d in json.loads(out.read_text())["ivods"]] == [1, 2, 3]

    api = _FakeApi()
    mtime = out.stat().st_mtime_ns
    fetch_proceedings_for_session(SESSION, config, api)
    assert api.calls == [] and out.stat().st_mtime_ns == mtime


# ---- multi-day meetings and discovery ----

class _ListApi:
    def __init__(self, ivods=(), by_date=None):
        self.ivods = list(ivods)
        self.by_date = by_date or {}
        self.list_calls = 0

    def list_ivods_for_meeting(self, code):
        self.list_calls += 1
        return self.ivods

    def list_plenary_codes_for_date(self, day):
        return self.by_date.get(day, {})


def test_meeting_index_refetched_when_a_sitting_day_is_missing(tmp_path):
    config = Config(tmp_path)
    _write_ivods(config, [_clip(1, "2026-10-30")])

    api = _ListApi([_clip(1, "2026-10-30")])
    fetch_meeting_index(CODE, config, api, expected_dates={"2026-10-30"})
    assert api.list_calls == 0  # cached index already has that day

    api = _ListApi([_clip(1, "2026-10-30"), _clip(2, "2026-11-03")])
    out = fetch_meeting_index(CODE, config, api, expected_dates={"2026-10-30", "2026-11-03"})
    assert api.list_calls == 1
    assert len(json.loads(out.read_text())["ivods"]) == 2


def test_discover_plenary_meetings_by_date():
    api = _ListApi(by_date={
        "2026-10-30": {CODE: {"2026-10-30"}},
        "2026-11-03": {CODE: {"2026-11-03"}, "院會-10-8-1": {"2026-11-03"},
                       "院會-11-6-1-1": {"2026-11-03"}},
    })
    found = discover_plenary_meetings(api, date(2026, 10, 30), date(2026, 11, 3), term=11)
    # Other terms and non-standard codes are skipped; dates are merged per code.
    assert found == {CODE: {"2026-10-30", "2026-11-03"}}


# ---- dataset-221 speech kinds ----

def test_roc_dates_round_trip():
    assert roc_date("2026-10-27") == "115/10/27"
    assert iso_date("115/10/27") == "2026-10-27"


class _KindsApi:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get_json(self, url, params, use_cache=True):
        self.calls.append(params)
        return {"dataList": self.rows}


def test_speech_kinds_keep_spoken_rows_of_the_meeting(tmp_path):
    config = Config(tmp_path)
    _write_ivods(config, [_clip(1, "2026-10-30", "洪孟楷"), _clip(2, "2026-11-03", "張智倫")])
    rows = [
        {"smeetingDate": "115/10/30", "meetingName": "第11屆第6會期第5次會議 ",
         "speechKindName": "國是論壇", "speechType": "口頭質詢", "legislatorNameList": "洪孟楷"},
        {"smeetingDate": "115/11/03", "meetingName": "第11屆第6會期第5次會議",
         "speechKindName": "質詢", "speechType": "口頭質詢", "legislatorNameList": "張智倫"},
        {"smeetingDate": "115/11/03", "meetingName": "第11屆第6會期第5次會議",
         "speechKindName": "質詢", "speechType": "書面質詢", "legislatorNameList": "陳冠廷"},
        {"smeetingDate": "115/10/31", "meetingName": "第11屆第6會期第5次會議",
         "speechKindName": "其他", "speechType": "口頭質詢", "legislatorNameList": "某某"},
        {"smeetingDate": "115/11/03", "meetingName": "第11屆第6會期第6次會議",
         "speechKindName": "其他", "speechType": "口頭質詢", "legislatorNameList": "某某"},
    ]
    api = _KindsApi(rows)
    out = fetch_speech_kinds_for_session(SESSION, config, api, today=date(2026, 11, 4))
    doc = json.loads(out.read_text())
    assert api.calls == [{"meetingDateS": "115/10/30", "meetingDateE": "115/11/03", "fileType": "json"}]
    assert [r["legislatorNameList"] for r in doc["rows"]] == ["洪孟楷", "張智倫"]

    # Complete for the current sitting days: no further request.
    fetch_speech_kinds_for_session(SESSION, config, api, today=date(2026, 11, 4))
    assert len(api.calls) == 1


def test_speech_kinds_refetched_while_incomplete(tmp_path):
    config = Config(tmp_path)
    _write_ivods(config, [_clip(1, "2026-11-03"), _clip(2, "2026-11-03", "李坤城")])
    api = _KindsApi([])  # dataset 221 not updated yet
    fetch_speech_kinds_for_session(SESSION, config, api, today=date(2026, 11, 3))
    fetch_speech_kinds_for_session(SESSION, config, api, today=date(2026, 11, 4))
    assert len(api.calls) == 2
    # Given up after RECHECK_DAYS.
    fetch_speech_kinds_for_session(SESSION, config, api, today=date(2026, 12, 31))
    assert len(api.calls) == 2


# ---- official per-clip text ----

class _TextApi:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get_text(self, url):
        ivod_id = url.rsplit("/", 1)[-1]
        self.calls.append(ivod_id)
        return self.pages.get(ivod_id, "")


def test_records_recheck_policy(tmp_path):
    config = Config(tmp_path)
    _write_ivods(config, [_clip(1, "2026-11-03"), _clip(2, "2026-11-03")])
    now = datetime(2026, 11, 4, 12, tzinfo=timezone.utc)
    checks: dict[str, str] = {}

    api = _TextApi({"1": "陳委員昭姿：謝謝主席。<br />"})
    assert fetch_records_for_session(SESSION, config, api, checks, now=now)
    records = json.loads(config.file(SESSION, "records").read_text())["records"]
    assert list(records) == ["1"]  # empty pages are not stored

    # Within the re-check interval: nothing is fetched.
    api = _TextApi({})
    assert not fetch_records_for_session(SESSION, config, api, checks, now=now + timedelta(hours=2))
    assert api.calls == []

    # A day later only the clip still without text is checked.
    api = _TextApi({"2": "主席：好。<br />"})
    assert fetch_records_for_session(SESSION, config, api, checks, now=now + timedelta(days=1))
    assert api.calls == ["2"]

    # Sittings older than MAX_AGE_DAYS are not re-checked once checked.
    _write_ivods(config, [_clip(3, "2026-01-05")], session="1105001")
    old_checks = {"3": (now - timedelta(days=2)).isoformat()}
    api = _TextApi({})
    fetch_records_for_session("1105001", config, api, old_checks, now=now)
    assert api.calls == []


def test_records_queue_published_sessions_for_rebuild(tmp_path, monkeypatch):
    from optv.parliaments.TW.scraper import fetch_records

    config = Config(tmp_path)
    _write_ivods(config, [_clip(1, "2026-11-03")])
    config.file(SESSION, "processed", create=True).write_text('{"meta": {}, "data": []}')
    monkeypatch.setattr(fetch_records, "LYApiClient",
                        lambda **kw: _TextApi({"1": "主席：好。<br />"}))
    args = SimpleNamespace(period=11, limit_to_period=True, limit_session=None,
                           force=False, retry_count=1, retry_delay_max=0)
    download_records(config, args)
    upgrades = config.dir("cache") / "tw-official-text-upgrades.txt"
    assert upgrades.read_text().split() == [SESSION]
