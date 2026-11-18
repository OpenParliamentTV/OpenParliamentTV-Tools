"""End-to-end through the merger: parse fixtures, run merge_data, then verify
the merged speech has the keys the align/NER/NEL stages will read.

Catches contract drift between the parsers and merger that the unit tests
on synthetic dicts (test_merge_item.py) might miss because they fabricate
inputs by hand."""

from tests.de_fixtures import merge_tiny_fixtures

# Keys consumed downstream:
#   align.py reads speech.textContents[].textBody[].sentences[].text
#   ner.py   reads same
#   nel.py   reads speech.people[].label, .faction.label
# All three also read speech.speechIndex, .session.number, .electoralPeriod.number.
DOWNSTREAM_REQUIRED = {
    "speechIndex", "session", "electoralPeriod", "people", "textContents",
    "media", "agendaItem",
}


def test_merge_data_produces_speeches_with_downstream_keys():
    merged = merge_tiny_fixtures()
    assert merged["data"], "merge_data must produce at least one merged speech"
    for speech in merged["data"]:
        missing = DOWNSTREAM_REQUIRED - set(speech)
        assert not missing, f"merged speech missing keys: {missing}"
        # Speech-id model: top-level `originID` is set only when the source has a
        # *joint* id (DE has none → absent). The speech must still be identifiable
        # via the text id (textContents[].originTextID) or speechIndex.
        text_id = any(
            tc.get("originTextID") for tc in (speech.get("textContents") or [])
        )
        assert speech.get("originID") or text_id or speech.get("speechIndex"), (
            "merged speech must carry a stable identity "
            "(originID, textContents[].originTextID, or speechIndex)"
        )


def test_merge_data_meta_has_processing_timestamps():
    merged = merge_tiny_fixtures()
    assert "merge" in merged["meta"]["processing"]
    assert "parse_media" in merged["meta"]["processing"]
