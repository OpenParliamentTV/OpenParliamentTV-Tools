"""The label backfill must repair the broken people and leave everything else alone.

The dangerous failure mode is not "fails to fix" -- it is "rewrites a speaker that
was already correct". Re-deriving a healthy media speaker from its raw title would
replace the canonical proceedings-sourced faction label ("BÜNDNIS 90/DIE GRÜNEN")
with the feed's spelling ("B90/GRÜNE"), which the platform then displays.
"""

import copy

from optv.scripts.repair_speaker_labels import repair_data

# nel.get_nel_data() shape: {cleaned label: entity}
PERSONS = {
    'hans michelbach': {'id': 'Q1581364'},
    'alexander s neu': {'id': 'Q15434455'},
    'markus kurth': {'id': 'Q1901650'},
}
FACTIONS = {
    'cdu csu': {'id': 'Q1023134'},
    'die linke': {'id': 'Q1826856'},
    'bundnis 90 die grunen': {'id': 'Q1007353'},
    'b90 grune': {'id': 'Q1007353'},
}

MICHELBACH_TITLE = ('Redebeitrag von Dr. h. c. (Univ Kyiv) Hans Michelbach (CDU/CSU) '
                    'am 27.06.2019 um 09:56 Uhr (107. Sitzung, TOP 4)')
KURTH_TITLE = ('Redebeitrag von Markus Kurth (B90/GRÜNE) am 12.05.2011 '
               'um 10:00 Uhr (107. Sitzung, TOP 4)')


def _session():
    return [
        {   # media speaker shredded by the old title regex: the name ended up in
            # `label`, its remains in `role`, and half the faction in `faction`.
            'speechIndex': 1,
            'debug': {'originalTitle': MICHELBACH_TITLE},
            'textContents': [{'textBody': [{'sentences': [{'text': 'Guten Tag.'}]}]}],
            'people': [
                {'label': 'h. c.', 'context': 'main-speaker',
                 'role': 'Univ Kyiv) Hans Michelbach (CDU',
                 'faction': {'label': 'CSU', 'wid': '', 'wtype': 'ORG'}},
            ],
        },
        {   # proceedings Fragestunde speaker: closing paren eaten.
            'speechIndex': 2,
            'debug': {},
            'people': [
                {'label': 'Alexander S. Neu (DIE LINKE', 'context': 'speaker'},
            ],
        },
        {   # already correct -- must come out untouched.
            'speechIndex': 3,
            'debug': {'originalTitle': KURTH_TITLE},
            'people': [
                {'label': 'Markus Kurth', 'context': 'main-speaker', 'wid': 'Q1901650',
                 'wtype': 'PERSON',
                 'faction': {'label': 'BÜNDNIS 90/DIE GRÜNEN', 'wid': 'Q1007353',
                             'wtype': 'ORG'}},
            ],
        },
    ]


def test_media_speaker_is_rebuilt_from_the_raw_title():
    data = _session()
    repair_data(data, PERSONS, FACTIONS)
    person = data[0]['people'][0]
    assert person['label'] == 'Hans Michelbach'
    assert person['wid'] == 'Q1581364'
    assert person['faction']['label'] == 'CDU/CSU'
    assert person['faction']['wid'] == 'Q1023134'
    # the wreckage in `role` must be gone, not merely overwritten alongside it
    assert 'role' not in person


def test_proceedings_speaker_loses_the_faction_parenthetical():
    data = _session()
    repair_data(data, PERSONS, FACTIONS)
    person = data[1]['people'][0]
    assert person['label'] == 'Alexander S. Neu'
    assert person['wid'] == 'Q15434455'


def test_a_correct_speaker_is_never_rewritten():
    data = _session()
    before = copy.deepcopy(data[2])
    repair_data(data, PERSONS, FACTIONS)
    # Re-deriving from KURTH_TITLE would downgrade the faction label to 'B90/GRÜNE'.
    assert data[2] == before


def test_transcript_is_left_alone():
    data = _session()
    before = copy.deepcopy(data[0]['textContents'])
    repair_data(data, PERSONS, FACTIONS)
    assert data[0]['textContents'] == before
