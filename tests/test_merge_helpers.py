"""Pure helpers from optv/parliaments/DE/merger/merge_session.py."""

from optv.parliaments.DE.merger.merge_session import (
    is_utc_offset,
    remove_accents,
    speaker_cleanup,
)


def test_is_utc_offset_accepts_canonical():
    assert is_utc_offset("+02:00")
    assert is_utc_offset("-05:30")
    assert is_utc_offset("+00:00")


def test_is_utc_offset_rejects_garbage():
    assert not is_utc_offset("CET")
    assert not is_utc_offset("+0200")
    assert not is_utc_offset("")
    assert not is_utc_offset("Z")


def test_remove_accents_strips_combining_marks():
    assert remove_accents("Müller") == "Muller"
    assert remove_accents("Görke") == "Gorke"
    assert remove_accents("Schäuble") == "Schauble"
    assert remove_accents("plain ascii") == "plain ascii"


def test_speaker_cleanup_normalises_label():
    item = {"people": [{"label": "Steffi von der Müller"}]}
    assert speaker_cleanup(item, "fallback") == "steffi muller"


def test_speaker_cleanup_strips_alterspresident_prefix():
    item = {"people": [{"label": "Alterspräsident Gregor Gysi"}]}
    assert "alterspr" not in speaker_cleanup(item, "fallback")


def test_speaker_cleanup_returns_default_when_no_people():
    assert speaker_cleanup({"people": []}, "fallback") == "fallback"
    assert speaker_cleanup({}, "fallback") == "fallback"
