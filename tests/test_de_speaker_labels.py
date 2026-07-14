"""Speaker labels must survive parsing well enough for NEL to link them.

Every case here is a label that reached production unlinked, and the platform
reported it as "Speaker/Faction not in database". The Wikidata IDs existed all
along -- the labels were mangled before NEL ever saw them.
"""

import pytest
from lxml import etree

from optv.parliaments.DE.parsers.common import (fix_fullname, parse_fullname,
                                                split_role_faction)
from optv.parliaments.DE.parsers.media2json import extract_title_data
from optv.parliaments.DE.parsers.proceedings2json import (faction_from_redner,
                                                          speaker_from_redner)


@pytest.mark.parametrize("raw,expected", [
    # Fragestunde speakers are bare <name> text, faction in parens + trailing colon.
    # Stripping the trailing punctuation used to eat the closing paren too, leaving
    # "Alexander S. Neu (DIE LINKE" -- an unlinkable label.
    ('Dr. Alexander S. Neu (DIE LINKE) : ', 'Alexander S. Neu'),
    ('Karl-Heinz Brunner (SPD) : ', 'Karl-Heinz Brunner'),
    # A constituency suffix disambiguates the name in the source, but the entity
    # registry keys people by the bare name.
    ('Norbert Müller (Potsdam) (DIE LINKE) : ', 'Norbert Müller'),
    ('Christian Kühn (Tübingen) (BÜNDNIS 90/DIE GRÜNEN):', 'Christian Kühn'),
    # Stray space inside the opening paren.
    ('Konstantin von Notz ( BÜNDNIS 90/DIE GRÜNEN) :', 'Konstantin von Notz'),
    # Truncated parenthetical (no closer) in the source.
    ('Bodo Ramelow, Ministerpräsident (Thüringen', 'Bodo Ramelow'),
])
def test_parse_fullname_drops_the_faction_parenthetical(raw, expected):
    assert parse_fullname(raw) == (expected, None)


@pytest.mark.parametrize("raw,name,status", [
    ('Vizepräsident Peter Hintze: ', 'Peter Hintze', 'vice-president'),
    ('Präsidentin Bärbel Bas:', 'Bärbel Bas', 'president'),
])
def test_parse_fullname_still_reads_the_chair_status(raw, name, status):
    assert parse_fullname(raw) == (name, status)


@pytest.mark.parametrize("raw,expected", [
    # An honorary doctorate can carry a parenthetical qualifier, which used to be
    # left stranded at the front of the label.
    ('Dr. h. c. (Univ Kyiv) Hans Michelbach', 'Hans Michelbach'),
    # ... and is spelled both with and without the inner space.
    ('Dr. h.c. Edelgard Bulmahn', 'Edelgard Bulmahn'),
    ('Dr. h. c. Gernot Erler', 'Gernot Erler'),
    ('Prof. Dr. Lothar Zimmermann', 'Lothar Zimmermann'),
    ('Angela Merkel', 'Angela Merkel'),
])
def test_fix_fullname_strips_academic_titles(raw, expected):
    assert fix_fullname(raw) == expected


@pytest.mark.parametrize("value,role,faction", [
    ('CDU/CSU', None, 'CDU/CSU'),
    ('Bundestagspräsident/CDU/CSU', 'Bundestagspräsident', 'CDU/CSU'),
    ('Bundestagsvizepräsidentin/B90/Grüne', 'Bundestagsvizepräsidentin', 'B90/Grüne'),
    ('Bundestagsvizepräsidentin/Gruppe Die Linke', 'Bundestagsvizepräsidentin', 'Gruppe Die Linke'),
    # The role itself contains a slash -- cutting at the first "/" lost the faction.
    ('Vorsitzender der CDU/CSU-Bundestagsfraktion/CDU/CSU',
     'Vorsitzender der CDU/CSU-Bundestagsfraktion', 'CDU/CSU'),
    # Ministers hold no seat: role, dangling separator, no faction.
    ('Bundesministerin/', 'Bundesministerin', ''),
    ('', None, ''),
])
def test_split_role_faction(value, role, faction):
    assert split_role_faction(value) == (role, faction)


@pytest.mark.parametrize("title,label,faction", [
    # The lazy name group used to stop at the first "(", handing the academic
    # title's qualifier to the faction: label "h. c.", faction "CSU".
    ('Redebeitrag von Dr. h. c. (Univ Kyiv) Hans Michelbach (CDU/CSU) am 27.06.2019 '
     'um 09:00 Uhr (107. Sitzung, TOP 5)', 'Hans Michelbach', 'CDU/CSU'),
    ('Redebeitrag von Angela Merkel (CDU/CSU), am 12.06.2019 um 15:30 Uhr '
     '(100. Sitzung, TOP 1)', 'Angela Merkel', 'CDU/CSU'),
    ('Redebeitrag von Petra Pau (Bundestagsvizepräsidentin/Gruppe Die Linke) am '
     '01.02.2024 um 09:00 Uhr (150. Sitzung, TOP 2)', 'Petra Pau', 'Gruppe Die Linke'),
    # Ceremonial readings nest a second paren inside the role slot.
    ('Redebeitrag von Tankred Suckau (liest Walter Kempowski (1929-2007)/) am '
     '08.05.2025 um 12:44 Uhr (900. Sitzung, TOP 1)', 'Tankred Suckau', ''),
])
def test_media_title_keeps_the_name_whole(title, label, faction):
    metadata = extract_title_data(title)
    assert metadata is not None
    assert fix_fullname(metadata['fullname']) == label
    assert split_role_faction(metadata['faction'])[1] == faction


# --- the <redner> display text (cause D: particles the structured fields drop) ---


def _redner(xml: str):
    """Parse a <p klasse="redner"> and hand back its <redner> child.

    The speaker's real name is the *tail* of that child -- the text the Bundestag
    prints after </redner> -- not the structured fields inside it.
    """
    return etree.fromstring(xml).find('.//redner')


@pytest.mark.parametrize("xml,expected", [
    # The structured fields drop the name particle; the display text keeps it.
    ('<p klasse="redner"><redner id="11004435"><name><vorname>Kees</vorname>'
     '<nachname>Vries</nachname><fraktion>CDU/CSU</fraktion></name></redner>'
     'Kees de Vries (CDU/CSU):</p>', 'Kees de Vries'),
    ('<p klasse="redner"><redner id="11004105"><name><titel>Dr.</titel>'
     '<vorname>Thomas</vorname><nachname>Maizière</nachname>'
     '<fraktion>CDU/CSU</fraktion></name></redner>'
     'Dr. Thomas de Maizière (CDU/CSU):</p>', 'Thomas de Maizière'),
    ('<p klasse="redner"><redner id="11004107"><name><vorname>Hans-Georg</vorname>'
     '<nachname>Marwitz</nachname><fraktion>CDU/CSU</fraktion></name></redner>'
     'Hans-Georg von der Marwitz (CDU/CSU):</p>', 'Hans-Georg von der Marwitz'),
    # The source XML duplicates every structured field; the display text does not.
    ('<p klasse="redner"><redner id="11005217 999990074"><name>'
     '<vorname>SvenjaSvenja</vorname><nachname>SchulzeSchulze</nachname>'
     '<fraktion>SPDSPD</fraktion></name></redner>Svenja Schulze (SPD):</p>',
     'Svenja Schulze'),
    # A personal statement (§31 GO) runs its whole text into the same node, so the
    # display text is cut at the first ":".
    ('<p klasse="redner"><redner id="1"><name><vorname>Heike</vorname>'
     '<nachname>Baehrens</nachname><fraktion>SPD</fraktion></name></redner>'
     'Heike Baehrens (SPD): Angesichts der großen Zahl der Flüchtlinge</p>',
     'Heike Baehrens'),
])
def test_speaker_name_comes_from_the_display_text(xml, expected):
    assert speaker_from_redner(_redner(xml))[0] == expected


def test_speaker_falls_back_to_the_structured_fields():
    redner = _redner('<p klasse="redner"><redner id="1"><name><vorname>Angela</vorname>'
                     '<nachname>Merkel</nachname></name></redner></p>')
    assert speaker_from_redner(redner)[0] == 'Angela Merkel'


def test_chair_status_is_not_promoted_from_the_display_text():
    # The display text reveals the office, but changing people[].context on
    # already-published speeches is a separate decision from repairing a name.
    redner = _redner('<p klasse="redner"><redner id="1"><name><vorname>Wolfgang</vorname>'
                     '<nachname>Schäuble</nachname></name></redner>'
                     'Präsident Dr. Wolfgang Schäuble:</p>')
    name, status = speaker_from_redner(redner)
    assert (name, status) == ('Wolfgang Schäuble', None)


@pytest.mark.parametrize("fraktion,tail,expected", [
    # <fraktion> is corrupt -> take the faction from the display text.
    ('SPDSPD', 'Svenja Schulze (SPD):', 'SPD'),
    ('SPDCDU/CSU', 'Alexander Föhr (CDU/CSU):', 'CDU/CSU'),
    # <fraktion> is fine -> keep it. The trailing parenthetical is often NOT a
    # faction (a constituency, a Land, an interpreting note), so trusting it
    # blindly would corrupt good data.
    ('SPD', 'Michael Thews (SPD) (Gebärdensprachdolmetschung):', 'SPD'),
    ('BÜNDNIS 90/DIE GRÜNEN', 'Volker Beck (Köln):', 'BÜNDNIS 90/DIE GRÜNEN'),
])
def test_faction_falls_back_to_the_display_text_only_when_corrupt(fraktion, tail, expected):
    redner = _redner(f'<p klasse="redner"><redner id="1"><name><vorname>X</vorname>'
                     f'<nachname>Y</nachname><fraktion>{fraktion}</fraktion>'
                     f'</name></redner>{tail}</p>')
    assert faction_from_redner(redner) == expected
