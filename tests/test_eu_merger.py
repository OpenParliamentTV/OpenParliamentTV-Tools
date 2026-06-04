"""EU merger: the CRE proceedings parser can emit the same speech twice
(identical speechId, only the index differs). The merger must dedupe by
speechId so each speech maps to one Stage 2 record / one sourcePage."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from optv.parliaments.EU.common import Config
from optv.parliaments.EU.merger.merge_session import merge_session


def _epoch(iso):
    return int(datetime.fromisoformat(iso).timestamp())


def _speech(speech_id, idx, start="2025-10-08T08:31:03+00:00",
            end="2025-10-08T08:32:39+00:00"):
    return {
        "speechId": speech_id,
        "speechIndex": idx,
        "dateStart": start,
        "dateEnd": end,
        "speaker": {"name": "Sven Mikser", "factionAbbr": "S&D",
                    "factionLabel": "S&D Group"},
        "agendaItem": {"officialTitle": "A debate", "number": 3},
        "textParagraphs": ["Hello.", "World."],
        "debug": {"vodURL": f"https://europarl/vod?s={speech_id}"},
    }


def _write(cfg, session, proc_data, media_data):
    p = cfg.file(session, "proceedings", create=True)
    p.write_text(json.dumps({"meta": {}, "data": proc_data}))
    m = cfg.file(session, "media", create=True)
    m.write_text(json.dumps({"meta": {}, "data": media_data}))


def test_merge_dedupes_duplicate_speech_ids(tmp_path: Path):
    cfg = Config(tmp_path)
    session = "20251008"
    sitting = {
        "sittingStart": _epoch("2025-10-08T06:00:00+00:00"),
        "sittingEnd": _epoch("2025-10-08T20:00:00+00:00"),
        "hlsMasterUrl": "https://cdn/master.m3u8",
        "hlsAudioUrls": {"or": "https://cdn/audio.m3u8"},
        "eventRef": "evt1",
        "title": "Sitting",
    }
    # Same speechId twice (the parser bug) + one distinct speech.
    proc = [_speech("AAA", 11), _speech("AAA", 100), _speech("BBB", 12)]
    _write(cfg, session, proc, [sitting])

    out = merge_session(session, cfg)
    data = json.loads(Path(out).read_text())["data"]

    # The speechId is the text id (EU has no joint id), so it lives in
    # textContents[].originTextID; the merger dedupes by it internally and the
    # redundant speech-level originID is dropped by the speech-id normalizer.
    ids = [s["textContents"][0]["originTextID"] for s in data]
    assert ids.count("AAA") == 1            # duplicate dropped
    assert sorted(set(ids)) == ["AAA", "BBB"]
    assert all("originID" not in s for s in data)   # no redundant speech-level id

    pages = [s["media"]["sourcePage"] for s in data]
    assert len(pages) == len(set(pages))    # sourcePage now unique per speech

    # speechIndex re-sequenced contiguously after dedup (no gap from the drop).
    assert [s["speechIndex"] for s in data] == [1, 2]
