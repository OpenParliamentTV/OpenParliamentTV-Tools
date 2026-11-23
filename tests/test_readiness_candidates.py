"""Choosing the parliament to add next from the Research repository's overview: the most feasible one without an implementation."""

import json
import os

import pytest

from optv.scripts.readiness import candidates


def _entry(code, total, status="Not started", name=None, kind="National", verified="2026-10-27"):
    return {"shortcode": code, "parliament": name or f"Parliament of {code}", "type": kind,
            "optv_status": status, "verified": verified, "profile_path": f"parliaments/{code[:2]}/{code}.md",
            "feasibility": {"total": total, "max": 23,
                            "tiers": {"data": {"score": 11, "max": 12},
                                      "engineering": {"score": total - 14, "max": 8},
                                      "operations": {"score": 3, "max": 3}}}}


# In the order the Research repository writes it: score descending, then code.
OVERVIEW = [_entry("DE", 22, "Operational"), _entry("AT", 21, "In development"), _entry("IT", 21),
            _entry("DK", 20, verified="2026-11-05"), _entry("FI", 20, "In development"), _entry("IS", 20),
            _entry("DE-BB", 15, "Research", kind="Regional"), _entry("XX", 9)]


@pytest.fixture
def roots(tmp_path):
    """A Tools tree that implements DE, AT and FI, and a Research repository beside it."""
    tools, data_root = tmp_path / "tools", tmp_path / "repos"
    for code in ("DE", "AT", "FI"):
        directory = tools / "optv" / "parliaments" / code
        directory.mkdir(parents=True)
        (directory / "manifest.yaml").write_text("name: x\n")
    (tools / "optv" / "parliaments" / "__pycache__").mkdir()
    research = data_root / "OpenParliamentTV-Research"
    for entry in OVERVIEW:
        profile = research / entry["profile_path"]
        profile.parent.mkdir(parents=True, exist_ok=True)
        profile.write_text(f"# {entry['shortcode']}\n")
    (research / "parliaments" / "_template.md").write_text("# template\n")
    overview = research / "overview.json"
    overview.write_text(json.dumps({"generated": "2026-11-17T14:00:08Z", "parliaments": OVERVIEW}))
    # The overview is the newest file, as after a regeneration.
    stamp = overview.stat().st_mtime + 10
    os.utime(overview, (stamp, stamp))
    return tools, data_root


def test_the_next_parliament_is_the_most_feasible_one_without_an_implementation(roots):
    result = candidates.candidates(*roots)
    assert [(c["code"], c["rank"], c["score"]) for c in result["candidates"]] == [
        ("IT", 3, 21), ("DK", 4, 20), ("IS", 6, 20), ("XX", 8, 9)]
    first = result["candidates"][0]
    assert first["name"] == "Parliament of IT" and first["type"] == "national"
    assert first["tiers"] == {"data": "11/12", "engineering": "7/8", "operations": "3/3"}
    assert first["profile"].endswith("OpenParliamentTV-Research/parliaments/IT/IT.md")
    assert result["newer_profiles"] == 0


def test_a_parliament_that_was_set_aside_is_left_out_unless_asked_for(roots):
    assert candidates.candidates(*roots)["deferred"] == ["DE-BB"]
    included = candidates.candidates(*roots, include_deferred=True)
    assert included["deferred"] == []
    assert [c["code"] for c in included["candidates"]] == ["IT", "DK", "IS", "DE-BB", "XX"]


def test_implementing_the_first_candidate_moves_on_to_the_second(roots):
    tools, data_root = roots
    directory = tools / "optv" / "parliaments" / "IT"
    directory.mkdir()
    (directory / "manifest.yaml").write_text("name: x\n")
    assert candidates.candidates(tools, data_root)["candidates"][0]["code"] == "DK"


def test_profiles_edited_after_the_overview_are_counted(roots):
    tools, data_root = roots
    research = data_root / "OpenParliamentTV-Research"
    later = (research / "overview.json").stat().st_mtime + 100
    os.utime(research / "parliaments" / "DK" / "DK.md", (later, later))
    os.utime(research / "parliaments" / "_template.md", (later, later))
    result = candidates.candidates(tools, data_root)
    assert result["newer_profiles"] == 1
    assert "1 profile(s) changed since" in candidates.render(result)


def test_the_listing_names_the_next_and_those_after_it(roots):
    text = candidates.render(candidates.candidates(*roots), limit=2)
    assert text.splitlines() == [
        "Next:  IT     21/23 (data 11/12, engineering 7/8, operations 3/3)  Parliament of IT; national; "
        "profile verified 2026-10-27",
        "then:  DK     20/23 (data 11/12, engineering 6/8, operations 3/3)  Parliament of DK; national; "
        "profile verified 2026-11-05",
        "       and 2 more",
        "Left out, looked into and set aside: DE-BB"]


def test_without_the_research_repository_there_is_no_choice(roots, tmp_path):
    tools, _ = roots
    with pytest.raises(candidates.CandidatesError, match="no Research overview"):
        candidates.candidates(tools, tmp_path / "nowhere")


def test_when_everything_is_implemented_it_says_so(roots):
    tools, data_root = roots
    for code in ("IT", "DK", "IS", "XX", "DE-BB"):
        directory = tools / "optv" / "parliaments" / code
        directory.mkdir()
        (directory / "manifest.yaml").write_text("name: x\n")
    result = candidates.candidates(tools, data_root)
    assert result["candidates"] == [] and result["deferred"] == []
    assert candidates.render(result) == "Every surveyed parliament has an implementation."
