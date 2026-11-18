"""Session start/end must be real sitting times, never placeholders.

The platform shows these as the session's start and end. Proceedings carry the
official times as naive local timestamps; when they carry none (ParlaMint-DE,
the WP16/17 source) or a malformed one (upstream Bundestag XML), the media's
first speech start / last speech end is the session bound instead.
"""

import pytest

from optv.parliaments.DE.merger.merge_session import session_bounds

MEDIA_META = {"dateStart": "2011-05-26T08:30:03+02:00",
              "dateEnd": "2011-05-26T22:41:26+02:00"}


@pytest.mark.parametrize("proceedings,expected", [
    # Official sitting times get the media's UTC offset.
    ({"dateStart": "2011-05-26T09:00:00", "dateEnd": "2011-05-26T22:42:00"},
     ("2011-05-26T09:00:00+02:00", "2011-05-26T22:42:00+02:00")),
    # ParlaMint-DE whole-day placeholder (17111).
    ({"dateStart": "2011-05-26T00:00:00", "dateEnd": "2011-05-26T23:59:59"},
     (MEDIA_META["dateStart"], MEDIA_META["dateEnd"])),
    # Malformed sitzung-datum="02014" (18013-18018).
    ({"dateStart": "02014T13:00:00", "dateEnd": "02014T17:04:00"},
     (MEDIA_META["dateStart"], MEDIA_META["dateEnd"])),
    # Empty sitzung-ende-uhrzeit (18023): only the end falls back.
    ({"dateStart": "2011-05-26T09:00:00", "dateEnd": "2011-05-26TERROR:"},
     ("2011-05-26T09:00:00+02:00", MEDIA_META["dateEnd"])),
    # No proceedings times at all.
    ({}, (MEDIA_META["dateStart"], MEDIA_META["dateEnd"])),
    # Protocol of another sitting (20015 holds sitting 28), or a mistyped
    # sitzung-datum (20036: 08.05. for the 18.05. sitting): the day does not
    # match the speeches, so neither bound is trusted.
    ({"dateStart": "2011-04-07T09:00:00", "dateEnd": "2011-04-07T23:29:00"},
     (MEDIA_META["dateStart"], MEDIA_META["dateEnd"])),
])
def test_session_bounds(proceedings, expected):
    assert session_bounds(proceedings, MEDIA_META) == expected
