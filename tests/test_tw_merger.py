"""End-to-end through TW parsers + merger.

Mirrors ``tests/test_merger_contract.py`` for DE: feeds the fixtures
through media2json + proceedings2json + merge_session, then verifies the
merged Stage 2 doc carries the keys the downstream stages (align, NEL,
NER) will read.
"""

from __future__ import annotations

import json
from pathlib import Path

from optv.parliaments.TW.common import Config
from optv.parliaments.TW.merger.merge_session import merge_session
from optv.parliaments.TW.parsers.media2json import parse_ivod_list
from optv.parliaments.TW.parsers.proceedings2json import parse_details

FIX = Path(__file__).resolve().parent / "fixtures" / "TW"
SESSION = "1105011"

DOWNSTREAM_REQUIRED = {
    "speechIndex", "session", "electoralPeriod",
    "people", "textContents", "media", "agendaItem",
}


def _set_up_data_dir(tmp_path: Path) -> Config:
    """Stage the fixtures into a TW data layout and parse them in place."""
    config = Config(tmp_path)
    # Copy raw fixtures into original/{media,proceedings}/.
    raw_ivods = json.loads((FIX / "sample-ivods.json").read_text())
    raw_details = json.loads((FIX / "sample-details.json").read_text())
    config.file(SESSION, "ivods", create=True).write_text(
        json.dumps(raw_ivods, ensure_ascii=False))
    config.file(SESSION, "details", create=True).write_text(
        json.dumps(raw_details, ensure_ascii=False))
    # Parse them.
    media_doc = parse_ivod_list(raw_ivods)
    proc_doc = parse_details(raw_details, term=11, session_period=5, meeting_number=11)
    config.file(SESSION, "media", create=True).write_text(
        json.dumps(media_doc, ensure_ascii=False))
    config.file(SESSION, "proceedings", create=True).write_text(
        json.dumps(proc_doc, ensure_ascii=False))
    return config


def test_merge_produces_downstream_ready_speeches(tmp_path):
    config = _set_up_data_dir(tmp_path)
    merged_file = merge_session(SESSION, config)
    merged = json.loads(merged_file.read_text())
    assert merged["meta"]["session"] == SESSION
    assert "merge" in merged["meta"]["processing"]
    assert merged["data"], "merger must produce at least one speech"
    for sp in merged["data"]:
        missing = DOWNSTREAM_REQUIRED - set(sp)
        assert not missing, f"merged speech missing keys: {missing}"
        # TW has no joint speech id (the IVOD_ID is the media+text id), so the
        # redundant speech-level originID is dropped by the normalizer; the
        # IVOD_ID lives in media.originMediaID.
        assert "originID" not in sp
        assert sp["media"]["originMediaID"].isdigit()


def test_merge_join_by_ivod_id_carries_text(tmp_path):
    """The fixture has 2 media records with matching proceedings; both should
    end up with non-empty textContents (text-missing=0)."""
    config = _set_up_data_dir(tmp_path)
    merged_file = merge_session(SESSION, config)
    merged = json.loads(merged_file.read_text())
    text_missing = sum(
        1 for s in merged["data"]
        if s.get("debug", {}).get("merge", {}).get("text-missing")
    )
    assert text_missing == 0
    # First-speech ordering: media records are sorted by 開始時間.
    # The text body must carry whisperx-derived sentences.
    sp = merged["data"][0]
    sentences = sp["textContents"][0]["textBody"][0]["sentences"]
    assert sentences
    assert all("timeStart" in s and "timeEnd" in s for s in sentences)


def test_merge_handles_media_only_speech(tmp_path):
    """Strip the proceedings fixture down to one record so the second media
    record has no text → debug.merge.text-missing=True."""
    config = _set_up_data_dir(tmp_path)
    proc_doc = json.loads(config.file(SESSION, "proceedings").read_text())
    proc_doc["data"] = proc_doc["data"][:1]
    config.file(SESSION, "proceedings").write_text(
        json.dumps(proc_doc, ensure_ascii=False))

    merged_file = merge_session(SESSION, config)
    merged = json.loads(merged_file.read_text())
    flagged = [
        s for s in merged["data"]
        if s.get("debug", {}).get("merge", {}).get("text-missing")
    ]
    assert len(flagged) == 1
    assert flagged[0]["textContents"] == []
    # The media block (videoFileURI etc.) must still be intact.
    assert flagged[0]["media"]["videoFileURI"]
