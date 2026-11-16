"""Session text probe per parliament: which speeches count as carrying merged
text (``TEXT_SIGNAL``) and whether one textless speech makes the whole session
``no_text`` (``TEXT_PROBE``), which gates the align and NER stages.

Media-spine parliaments keep clips without a text match as media-only records,
so they probe "any"; proceedings-spine parliaments build every speech from the
text, so a textless speech means an incomplete merge and they probe "every".
"""

from __future__ import annotations

import importlib
import json

import pytest

from optv.shared.config import BaseConfig
from optv.shared.session_status import SessionStatus

MEDIA_SPINE = ["AT", "DE-BW", "DE-BY", "DE-HH", "DE-NI", "DE-NW", "DE-SH", "DE-SN",
               "ES", "FI", "PT", "SE", "TW"]
PROCEEDINGS_SPINE = ["DE", "DE-RP", "DE-ST", "EU", "FR", "NO"]


def _config(parliament):
    return importlib.import_module(f"optv.parliaments.{parliament}.common").Config


@pytest.mark.parametrize("parliament", MEDIA_SPINE)
def test_media_spine_parliaments_probe_any(parliament):
    assert _config(parliament).TEXT_PROBE == "any"


@pytest.mark.parametrize("parliament", PROCEEDINGS_SPINE)
def test_proceedings_spine_parliaments_probe_every(parliament):
    assert getattr(_config(parliament), "TEXT_PROBE", "every") == "every"


def _text(aligned=False):
    speech = {"textContents": [{"textBody": [{"type": "speech", "sentences": [{"text": "Hallo."}]}]}],
              "debug": {}}
    if aligned:
        speech["debug"]["alignDuration"] = 2.0
    return speech


_TEXTLESS = {"textContents": [], "debug": {}}


class _TextContentsConfig(BaseConfig):
    TEXT_SIGNAL = "textContents"
    TEXT_PROBE = "any"


def _write(cfg, speeches, session="1001"):
    cfg.file(session, "processed", create=True).write_text(
        json.dumps({"meta": {}, "data": speeches}), encoding="utf-8")
    return cfg.status(session)


def test_text_contents_signal_with_any_probe(tmp_path):
    cfg = _TextContentsConfig(tmp_path)
    status = _write(cfg, [_TEXTLESS, _text(aligned=True), _text()])
    assert SessionStatus.no_text not in status and SessionStatus.aligned in status
    assert SessionStatus.no_text in _write(cfg, [_TEXTLESS, {"textContents": [{"textBody": []}]}])
    assert SessionStatus.empty in _write(cfg, [])


@pytest.mark.parametrize("parliament", ["FI", "PT", "SE", "TW"])
def test_text_contents_parliaments_use_the_shared_probe(parliament, tmp_path):
    # These read text from textContents (their mergers stamp no proceedingIndex)
    # and no longer override status().
    Config = _config(parliament)
    assert Config.TEXT_SIGNAL == "textContents"
    assert Config.status is BaseConfig.status
    cfg = Config(tmp_path)
    assert SessionStatus.no_text not in _write(cfg, [_TEXTLESS, _text()])
