"""Pin the top-level shape parsers emit, so renames in the parsers cannot
silently break the merger (the originID/originTextID failure mode)."""

import json
from pathlib import Path

from optv.parliaments.DE.parsers.media2json import parse_media_data
from optv.parliaments.DE.parsers.proceedings2json import parse_transcript

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "DE"

# Keys merge_item.merge_item dereferences with [...] on a proceeding speech.
# Missing any of these would crash the merger at runtime.
PROCEEDING_REQUIRED_TOP_LEVEL = {
    "parliament", "electoralPeriod", "session", "speechIndex",
    "originID", "agendaItem", "people", "textContents", "documents", "debug",
}
PROCEEDING_TEXTCONTENT_REQUIRED = {
    "type", "sourceURI", "originTextID", "textBody",
}

# Keys merge_item dereferences on a media item.
MEDIA_REQUIRED_TOP_LEVEL = {
    "parliament", "electoralPeriod", "session", "speechIndex",
    "agendaItem", "dateStart", "dateEnd", "media", "debug",
}


def test_proceedings_parser_emits_required_top_level_keys():
    speeches = list(parse_transcript(str(FIXTURES / "tiny-proceedings.xml")))
    assert speeches, "fixture must yield at least one speech"
    for speech in speeches:
        missing = PROCEEDING_REQUIRED_TOP_LEVEL - set(speech)
        assert not missing, (
            f"proceedings speech missing top-level keys {missing}; "
            f"merge_item will KeyError on these"
        )


def test_proceedings_text_contents_have_origin_text_id():
    speeches = list(parse_transcript(str(FIXTURES / "tiny-proceedings.xml")))
    for speech in speeches:
        for tc in speech["textContents"]:
            missing = PROCEEDING_TEXTCONTENT_REQUIRED - set(tc)
            assert not missing, f"textContents item missing keys: {missing}"


def test_media_parser_emits_required_top_level_keys():
    raw = json.loads((FIXTURES / "tiny-media-input.json").read_text())
    parsed = parse_media_data(raw)
    assert parsed["data"], "media fixture must produce at least one item"
    for item in parsed["data"]:
        missing = MEDIA_REQUIRED_TOP_LEVEL - set(item)
        assert not missing, (
            f"media item missing top-level keys {missing}; "
            f"merge_item will KeyError on these"
        )


def test_proceedings_speech_index_starts_at_1001():
    """Convention: proceedings.speechIndex starts at 1001 so it never
    collides with media.speechIndex (which starts at 1)."""
    speeches = list(parse_transcript(str(FIXTURES / "tiny-proceedings.xml")))
    indexes = [s["speechIndex"] for s in speeches]
    assert all(i >= 1001 for i in indexes)


def test_media_speech_index_starts_at_1():
    raw = json.loads((FIXTURES / "tiny-media-input.json").read_text())
    parsed = parse_media_data(raw)
    indexes = [item["speechIndex"] for item in parsed["data"]]
    assert indexes[0] == 1
