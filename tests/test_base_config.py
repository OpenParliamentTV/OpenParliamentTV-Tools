"""BaseConfig knobs a parliament's Config sets instead of overriding methods."""

import importlib

import pytest

from optv.shared.config import BaseConfig


def test_extra_dirs_resolve_against_the_data_and_cache_directories(tmp_path):
    class Config(BaseConfig):
        EXTRA_DIRS = {
            "meetings": ("data", "original/meetings"),
            "scratch": ("cache", "scratch"),
            "ivods": ("data", "original/media"),
        }

    config = Config(tmp_path / "data", cache_dir=tmp_path / "elsewhere")
    assert config.dir("meetings") == tmp_path / "data" / "original" / "meetings"
    assert config.dir("scratch") == tmp_path / "elsewhere" / "scratch"
    # An alias of a shared directory gets its own file name there.
    assert config.file("1105011", "ivods") == tmp_path / "data" / "original" / "media" / "1105011-ivods.json"
    assert config.dir("meetings", create=True).is_dir()


def test_extra_dirs_do_not_leak_into_the_base_class(tmp_path):
    assert BaseConfig.EXTRA_DIRS == {}
    with pytest.raises(KeyError):
        BaseConfig(tmp_path).dir("meetings")


@pytest.mark.parametrize("parliament, raw_name, session", [
    ("DE", "raw-21001-media.json", "21001"),
    ("AT", "27144-mediathek.json", "27144"),
    ("FI", "2026-058-event.json", "2026-058"),
    ("PT", "17-1-059-av.json", "17-1-059"),
    ("TW", "1105011-ivods.json", "1105011"),
    ("NO", "22_11518-media.json", "22_11518"),
])
def test_sessions_are_discovered_from_media_files(parliament, raw_name, session, tmp_path):
    config = importlib.import_module(f"optv.parliaments.{parliament}.common").Config(tmp_path)
    (config.dir("media", create=True) / raw_name).write_text("{}")
    (config.dir("media") / "unrelated.txt").write_text("")
    assert config.sessions() == [session]
    assert config.sessions(prefix=session[:2]) == [session]
    assert config.sessions(prefix="zz") == []


@pytest.mark.parametrize("parliament, raw_name, session", [
    ("SE", "2025-091-anforanden.json", "2025-091"),
    ("EU", "raw-20251008-cre.json", "20251008"),
    ("FR", "2026O1N232-cr.xml", "2026O1N232"),
])
def test_sessions_are_discovered_from_proceedings_files(parliament, raw_name, session, tmp_path):
    config = importlib.import_module(f"optv.parliaments.{parliament}.common").Config(tmp_path)
    (config.dir("proceedings", create=True) / raw_name).write_text("{}")
    (config.dir("media", create=True) / f"{session}-media.json").write_text("{}")
    (config.dir("media") / "other-media.json").write_text("{}")
    assert config.sessions() == [session]


@pytest.mark.parametrize("parliament, name, expected", [
    ("NO", "meetings", "original/meetings"),
    ("DE-HH", "subtitles", "original/subtitles"),
    ("PT", "opendata", "original/opendata"),
    ("PT", "opendata_cache", "cache/opendata"),
    ("TW", "details", "original/proceedings"),
    ("TW", "speechkinds", "original/media"),
])
def test_parliament_specific_directories(parliament, name, expected, tmp_path):
    config = importlib.import_module(f"optv.parliaments.{parliament}.common").Config(tmp_path)
    assert config.dir(name) == tmp_path / expected
