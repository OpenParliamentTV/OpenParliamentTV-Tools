"""AT merger: trailing-handoff trim, confidence/linkedMediaIndexes gate, speaker vocabulary."""

from __future__ import annotations

import importlib

import pytest

from optv.parliaments.AT.parsers.proceedings2json import at_speaker_status

_merger = importlib.import_module("optv.parliaments.AT.merger.merge_session")


def _block(pad, text):
    return {"speakerID": pad, "speaker": f"P{pad}", "speakerstatus": "role",
            "sentences": [{"text": text}], "text": text}


def _speech_with_blocks(pad_blocks, people_pads):
    """pad_blocks: list of (pad, text); people_pads: ordered PADs for people[]."""
    return {
        "people": [{"originPersonID": p, "label": f"P{p}"} for p in people_pads],
        "textContents": [{"textBody": [_block(p, t) for p, t in pad_blocks]}],
    }


# --------------------------------------------------------------------------- #
# Trailing-handoff trim
# --------------------------------------------------------------------------- #

def test_trim_drops_trailing_chair_handoff():
    # speaker "1" ... chair "9" handoff at the end → drop the chair tail only.
    sp = _speech_with_blocks(
        [("1", "Meine Damen und Herren"), ("1", "Zur Sache"),
         ("9", "Zu Wort gelangt der nächste Redner.")],
        people_pads=["1", "9"])
    removed = _merger._trim_trailing_handoff(sp, "1")
    assert removed == 1
    bodies = [b["speakerID"] for b in sp["textContents"][0]["textBody"]]
    assert bodies == ["1", "1"]
    # chair, now unreferenced, drops out of people[]
    assert [p["originPersonID"] for p in sp["people"]] == ["1"]


def test_trim_keeps_interior_interruptions():
    # chair interrupts mid-speech and the speaker continues → keep everything.
    sp = _speech_with_blocks(
        [("1", "Beginn"), ("9", "Bitte, Herr Abgeordneter."), ("1", "Fortsetzung"),
         ("9", "Zu Wort gelangt der nächste Redner.")],
        people_pads=["1", "9"])
    removed = _merger._trim_trailing_handoff(sp, "1")
    assert removed == 1  # only the final chair handoff
    bodies = [b["speakerID"] for b in sp["textContents"][0]["textBody"]]
    assert bodies == ["1", "9", "1"]  # interior chair interruption preserved


def test_trim_noop_when_speaker_is_last():
    sp = _speech_with_blocks([("9", "Intro"), ("1", "Rede")], people_pads=["1", "9"])
    assert _merger._trim_trailing_handoff(sp, "1") == 0
    assert len(sp["textContents"][0]["textBody"]) == 2  # leading chair kept


def test_trim_noop_on_pad_mismatch():
    # On-camera speaker never authors a block → keep text verbatim, no guessing.
    sp = _speech_with_blocks([("7", "A"), ("8", "B")], people_pads=["7", "8"])
    assert _merger._trim_trailing_handoff(sp, "1") == 0
    assert len(sp["textContents"][0]["textBody"]) == 2


# --------------------------------------------------------------------------- #
# apply_gate: confidence + linkedMediaIndexes
# --------------------------------------------------------------------------- #

def _text_speech(speech_index, std_id, agenda_type, chars, duration):
    n = max(chars // 5, 1)
    body = [{"sentences": [{"text": "x" * 5} for _ in range(n)]}]
    return {
        "speechIndex": speech_index,
        "agendaItem": {"type": agenda_type},
        "media": {"duration": duration},
        "textContents": [{"textBody": body}],
        "debug": {"stdId": std_id},
    }


def test_apply_gate_clean_speech():
    s = _text_speech(1, std_id=100, agenda_type="regular", chars=3000, duration=180)
    _merger.apply_gate([s])
    assert s["debug"]["confidence"] == 1.0
    assert s["debug"]["linkedMediaIndexes"] == [1]
    assert "confidenceReason" not in s["debug"]


def test_apply_gate_cps_cap():
    s = _text_speech(70, std_id=319278, agenda_type="regular", chars=17900, duration=124)
    _merger.apply_gate([s])
    assert s["debug"]["confidence"] == 0.5
    assert s["debug"]["confidenceReason"] == "cps-cap"
    assert s["debug"]["linkedMediaIndexes"] == [70]


def test_apply_gate_qa_not_blanket_by_default():
    # AT default: Q&A keeps confidence 1 unless cps-cap fires.
    s = _text_speech(5, std_id=200, agenda_type="qa", chars=5000, duration=600)
    _merger.apply_gate([s])
    assert s["debug"]["confidence"] == 1.0


def test_apply_gate_shared_stdid_links_multiple():
    a = _text_speech(3, std_id=500, agenda_type="regular", chars=1000, duration=120)
    b = _text_speech(4, std_id=500, agenda_type="regular", chars=1000, duration=120)
    _merger.apply_gate([a, b])
    assert a["debug"]["linkedMediaIndexes"] == [3, 4]
    assert b["debug"]["linkedMediaIndexes"] == [3, 4]  # count>1 → text gated downstream


def test_apply_gate_skips_media_only():
    s = {"speechIndex": 9, "media": {"duration": 60}, "textContents": [],
         "debug": {"stdId": 1}}
    _merger.apply_gate([s])
    assert "confidence" not in s["debug"]
    assert "linkedMediaIndexes" not in s["debug"]


# --------------------------------------------------------------------------- #
# Speaker vocabulary: people[].context + textBody[].speakerstatus
# --------------------------------------------------------------------------- #

_GUEST_PRESIDENT_ROLES = [
    "Präsidentin des Rechnungshofes",
    "Präsident der Parlamentarischen Versammlung des Europarates",
    "Präsidentin des Europäischen Parlaments",
]


@pytest.mark.parametrize("role,expected", [
    ("Präsident", "president"),
    ("Präsidentin", "president"),
    ("Zweite Präsidentin", "vice-president"),
    ("Dritter Präsident", "vice-president"),
    ("Vizepräsident", "vice-president"),
])
@pytest.mark.parametrize("on_camera", [True, False])
def test_speaker_status_chair(role, expected, on_camera):
    assert at_speaker_status(role, on_camera) == expected


@pytest.mark.parametrize("role", _GUEST_PRESIDENT_ROLES + [
    "Abgeordneter", "Bundesminister für Finanzen", "Schriftführerin", None, ""])
def test_speaker_status_non_chair(role):
    assert at_speaker_status(role, True) == "main-speaker"
    assert at_speaker_status(role, False) == "speaker"


def _person(pad, role):
    return {"originPersonID": pad, "label": f"P{pad}", "role": role, "context": role}


def _role_block(pad, text, with_id=True):
    b = {"speaker": f"P{pad}" if pad else None, "speakerstatus": "Abgeordneter",
         "sentences": [{"text": text}], "text": text}
    if with_id:
        b["speakerID"] = pad
    return b


def _statuses(speech):
    return [b["speakerstatus"] for b in speech["textContents"][0]["textBody"]]


def test_assign_speaker_status_mixed_speech():
    # Q&A: chair intro, on-camera MP asks, minister answers, MP follows up,
    # plus a heading paragraph before any speaker.
    sp = {
        "people": [_person("1", "Abgeordneter"), _person("9", "Präsident"),
                   _person("5", "Bundesminister für Inneres")],
        "textContents": [{"textBody": [
            {"speaker": None, "speakerID": None, "speakerstatus": None,
             "sentences": [{"text": "Fragestunde"}]},
            _role_block("9", "Wir kommen zur Fragestunde."),
            _role_block("1", "Herr Minister!"),
            _role_block("5", "Die Antwort lautet."),
            _role_block("1", "Danke."),
        ]}],
    }
    _merger.assign_speaker_status(sp, "1")
    assert _statuses(sp) == [None, "president", "main-speaker", "speaker", "main-speaker"]
    assert [p["context"] for p in sp["people"]] == ["main-speaker", "president", "speaker"]
    assert [p["role"] for p in sp["people"]] == ["Abgeordneter", "Präsident", "Bundesminister für Inneres"]


def test_assign_speaker_status_chair_on_camera():
    sp = {"people": [_person("9", "Präsidentin")],
          "textContents": [{"textBody": [_role_block("9", "Die Sitzung ist eröffnet.")]}]}
    _merger.assign_speaker_status(sp, "9")
    assert _statuses(sp) == ["president"]
    assert sp["people"][0]["context"] == "president"


def test_assign_speaker_status_guest_president_is_main_speaker():
    sp = {"people": [_person("7", "Präsidentin des Rechnungshofes"), _person("9", "Präsident")],
          "textContents": [{"textBody": [_role_block("9", "Bitte."), _role_block("7", "Danke.")]}]}
    _merger.assign_speaker_status(sp, "7")
    assert _statuses(sp) == ["president", "main-speaker"]


def test_assign_speaker_status_label_fallback_and_idempotent():
    # Blocks without speakerID resolve by their unique speaker label.
    sp = {"people": [_person("1", "Abgeordnete"), _person("9", "Präsident")],
          "textContents": [{"textBody": [_role_block("9", "Bitte.", with_id=False),
                                         _role_block("1", "Rede.", with_id=False)]}]}
    _merger.assign_speaker_status(sp, "1")
    assert _statuses(sp) == ["president", "main-speaker"]
    _merger.assign_speaker_status(sp, "1")
    assert _statuses(sp) == ["president", "main-speaker"]


def test_merge_one_emits_vocabulary_only():
    media_rec = {"speechIndex": 3, "stdId": 100, "padIntern": "1",
                 "speakerName": "P1 (ÖVP)", "agendaTitle": "TOP 1 Erste Lesung: Test",
                 "media": {"duration": 120}}
    proc = {"stdId": 100, "people": [_person("9", "Präsident"), _person("1", "Abgeordneter")],
            "textContents": [{"textBody": [_role_block("9", "Zu Wort gelangt P1."),
                                           _role_block("1", "Rede.")]}]}
    speech = _merger.merge_one(media_rec, proc, "AT", 27, 50, (None, None))
    assert [p["originPersonID"] for p in speech["people"]] == ["1", "9"]
    assert [p["context"] for p in speech["people"]] == ["main-speaker", "president"]
    assert _statuses(speech) == ["president", "main-speaker"]
