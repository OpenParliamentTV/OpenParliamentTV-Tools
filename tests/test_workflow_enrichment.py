"""Source selection + gating for the in-place enrichment stages (NEL / NER).

These lock in the fix for the stale-cache class of bug: a media-only `aligned`
stub (left over from a run where proceedings were briefly unavailable) used to
shadow a fully transcribed published session, so NER ran over text-less input
and the publish guard silently dropped the result on every run. The general
rule is now: source from `processed/` (the git-tracked high-water mark) when it
exists, and gate on the file's own `meta.processing.<stage>` rather than on
local cache mtimes.
"""

import json

from optv.shared.session_status import SessionStatus
from optv.shared.workflow import (
    _align_is_current,
    _enrichment_is_current,
    _enrichment_source,
    _nel_is_current,
)


class _FakeConfig:
    """Minimal stand-in exposing the one method the helpers call."""

    def __init__(self, root):
        self.root = root

    def file(self, session, stage="processed", create=False):
        return self.root / f"{session}-{stage}.json"


def _touch(path, processing=None):
    path.write_text(
        json.dumps({"meta": {"processing": processing or {}}, "data": []}),
        encoding="utf-8",
    )


def test_enrichment_source_prefers_processed_over_cache(tmp_path):
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21020", "aligned"))      # stale media-only stub on disk
    _touch(cfg.file("21020", "processed"))    # the published truth
    assert _enrichment_source(cfg, "21020") == cfg.file("21020", "processed")


def test_enrichment_source_falls_back_to_freshest_cache_before_first_publish(tmp_path):
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21020", "merged"))
    _touch(cfg.file("21020", "aligned"))
    # No processed yet -> richest existing cache (aligned beats merged).
    assert _enrichment_source(cfg, "21020") == cfg.file("21020", "aligned")


def test_enrichment_source_returns_merged_path_when_nothing_exists(tmp_path):
    cfg = _FakeConfig(tmp_path)
    assert _enrichment_source(cfg, "21020") == cfg.file("21020", "merged")


def test_ner_runs_when_published_file_has_no_ner_pass(tmp_path):
    """The reported case: processed/ freshly merged+aligned but never NER'd."""
    src = tmp_path / "21074-session.json"
    _touch(src, {"merge": "2026-06-18T17:15:49", "align": "2026-06-18T17:42:49"})
    assert _enrichment_is_current(src, "ner") is False


def test_ner_skipped_when_pass_is_newer_than_upstream(tmp_path):
    src = tmp_path / "21079-session.json"
    _touch(src, {
        "merge": "2026-05-21T11:15:28",
        "align": "2026-05-21T11:15:54",
        "ner": "2026-06-19T05:04:53",
    })
    assert _enrichment_is_current(src, "ner") is True


def test_ner_reruns_when_remerge_advances_past_pass(tmp_path):
    src = tmp_path / "21074-session.json"
    _touch(src, {
        "merge": "2026-06-18T17:15:49",
        "ner": "2026-05-15T16:54:00",  # pre-remerge NER
    })
    assert _enrichment_is_current(src, "ner") is False


def test_nel_not_rerun_just_because_align_is_newer(tmp_path):
    """The reported empty-every-cron case: nel always stamps before align in a
    run, so align is permanently newer. NEL only depends on merge, so this must
    still count as current and skip."""
    src = tmp_path / "21045-session.json"
    _touch(src, {
        "merge": "2025-12-01T09:15:27",
        "nel": "2025-12-01T09:15:30",
        "align": "2025-12-01T09:15:34",  # newer than nel, but irrelevant to NEL
    })
    assert _nel_is_current(src) is True


def test_nel_reruns_when_remerge_advances_past_pass(tmp_path):
    src = tmp_path / "21074-session.json"
    _touch(src, {
        "merge": "2026-06-18T17:15:49",  # re-merge after last nel
        "nel": "2025-12-01T09:15:30",
        "align": "2025-12-01T09:15:34",
    })
    assert _nel_is_current(src) is False


# ---- align gate (_align_is_current) ----------------------------------------
#
# These lock in the fix for the "re-aligns every cron" bug: the old
# is_newer(merged, aligned) cache-mtime gate re-ran the (expensive) alignment
# whenever the aligned cache was absent (it lives under gitignored cache/, so a
# checkout that pulled only processed/ never has it) or whenever a no-op
# re-merge bumped the merged cache's mtime. The gate now compares in-file
# meta.processing timestamps instead.


def test_align_skipped_when_processed_align_newer_than_merge(tmp_path):
    """Steady state: published file is aligned past its merge -> no work."""
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21045", "processed"),
           {"merge": "2026-06-19T09:25:23", "align": "2026-06-19T09:40:00"})
    assert _align_is_current(cfg, "21045", {SessionStatus.aligned}) is True


def test_align_skipped_when_aligned_cache_absent_but_processed_fresh(tmp_path):
    """The gitignored-cache case: only processed/ was pulled. The old mtime
    gate saw no aligned cache and re-aligned every run; now it's a no-op."""
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21045", "merged"), {"merge": "2026-06-19T09:25:23"})
    _touch(cfg.file("21045", "processed"),
           {"merge": "2026-06-19T09:25:23", "align": "2026-06-19T09:40:00"})
    # no aligned cache on disk
    assert _align_is_current(cfg, "21045", {SessionStatus.aligned}) is True


def test_align_reruns_when_merged_cache_advances_past_alignment(tmp_path):
    """A genuine re-merge (new merged cache) the demotion guard kept out of
    processed/ must still re-align."""
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21045", "merged"), {"merge": "2026-06-19T10:00:00"})
    _touch(cfg.file("21045", "processed"),
           {"merge": "2026-06-19T09:25:23", "align": "2026-06-19T09:40:00"})
    assert _align_is_current(cfg, "21045", {SessionStatus.aligned}) is False


def test_align_reruns_when_never_aligned(tmp_path):
    """Merged but no alignment anywhere -> align once (then it settles)."""
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21045", "merged"), {"merge": "2026-06-19T09:25:23"})
    _touch(cfg.file("21045", "processed"), {"merge": "2026-06-19T09:25:23"})
    assert _align_is_current(cfg, "21045", set()) is False


def test_align_uses_aligned_cache_stamp_when_processed_has_none(tmp_path):
    """Aligned locally but not yet published: the aligned cache carries the
    align stamp."""
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21045", "merged"), {"merge": "2026-06-19T09:25:23"})
    _touch(cfg.file("21045", "aligned"),
           {"merge": "2026-06-19T09:25:23", "align": "2026-06-19T09:40:00"})
    assert _align_is_current(cfg, "21045", {SessionStatus.aligned}) is True


def test_align_legacy_alignment_without_stamp_not_redone(tmp_path):
    """Legacy/debug-only alignment (aligned flag set, no meta.processing.align
    anywhere) must not be re-aligned from scratch."""
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21045", "merged"), {"merge": "2026-06-19T09:25:23"})
    _touch(cfg.file("21045", "processed"), {"merge": "2026-06-19T09:25:23"})
    assert _align_is_current(cfg, "21045", {SessionStatus.aligned}) is True
