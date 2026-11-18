"""The shared factories for the scope and merge hooks."""

import importlib
from types import SimpleNamespace

import pytest

from optv.shared import workflow as wf


def _args(period, *, limit_to_period=True, limit_session=""):
    return SimpleNamespace(period=period, limit_to_period=limit_to_period, limit_session=limit_session)


def test_zero_padded_prefix_matches_single_digit_periods():
    scope = wf.period_prefix_scope(width=2)
    assert scope(_args(8), "08025")
    assert not scope(_args(8), "18025")
    assert not scope(_args(8), "80250")
    assert scope(_args(18), "18025")
    # The default misses the zero-padded key.
    assert not wf._default_session_in_scope(_args(8), "08025")


def test_separator_keeps_longer_numbers_out():
    scope = wf.period_prefix_scope(sep="_")
    assert scope(_args(22), "22_11518")
    assert not scope(_args(22), "225_1")
    assert wf.period_prefix_scope(sep="-")(_args(2025), "2025-091")
    assert not wf.period_prefix_scope(sep="-")(_args(2025), "20251-091")


def test_limit_flags_apply_like_the_default():
    scope = wf.period_prefix_scope(width=2)
    assert scope(_args(8, limit_to_period=False), "09001")
    assert scope(_args(9, limit_session="0900[1-3]"), "09002")
    assert not scope(_args(9, limit_session="0900[1-3]"), "09004")
    assert not scope(_args(8, limit_session="0900[1-3]"), "09002")


@pytest.mark.parametrize("parliament, period, inside, outside", [
    ("DE-BW", 17, "17118", "18001"),
    ("DE-BY", 19, "19054", "18054"),
    ("DE-HH", 23, "23018", "22101"),
    ("DE-NI", 19, "19080", "18009"),
    ("DE-NW", 18, "18117", "17117"),
    ("DE-SH", 20, "20119", "19119"),
    ("DE-SN", 8, "08025", "07025"),
    ("DE-ST", 9, "09002", "08105"),
    ("SE", 2025, "2025-091", "20251-091"),
    ("NO", 22, "22_11518", "225_1"),
])
def test_parliament_scope_hooks(parliament, period, inside, outside):
    hooks = importlib.import_module(f"optv.parliaments.{parliament}.workflow").HOOKS
    assert hooks.session_in_scope(_args(period), inside)
    assert not hooks.session_in_scope(_args(period), outside)


def test_adapt_merge_reorders_the_arguments():
    calls = []

    def merge_session(session, config, args):
        calls.append((session, config, args))
        return "merged-path"

    hook = wf.adapt_merge(merge_session)
    assert hook("config", "21001", "args") == "merged-path"
    assert calls == [("21001", "config", "args")]


@pytest.mark.parametrize("parliament", ["AT", "DE-BW", "DE-BY", "DE-NW", "DE-SH", "DE-SN",
                                        "EU", "FI", "FR", "NO", "PT", "SE"])
def test_sliced_audio_parliaments_use_the_shared_align_hook(parliament):
    """One align hook body for every parliament that slices per-speech audio: it reads the richest available document, so a re-align keeps published entity links and NER."""
    hooks = importlib.import_module(f"optv.parliaments.{parliament}.workflow").HOOKS
    assert hooks.align_session_to_file.__qualname__.startswith("make_align_hook.")
