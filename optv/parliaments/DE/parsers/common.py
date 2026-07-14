# Methods common to parsing modules
import logging
logger = logging.getLogger(__name__)

import re

STATUS_TRANSLATION = {
    'Präsident': 'president',
    'Präsidentin': 'president',
    'Vizepräsident': 'vice-president',
    'Vizepräsidentin': 'vice-president',
    'Alterspräsident': 'interim-president',
    'Alterspräsidentin': 'interim-president',
}

# Academic titles, in the spellings the sources actually use: "Dr.", "Prof.",
# "Dr.-Ing.", "HonD" (no dot), "Dr. h. c." and "Dr. h.c." (no inner space), plus
# the parenthetical qualifier an honorary doctorate can carry ("Dr. h. c. (Univ
# Kyiv) Hans Michelbach").
#
# "Bundespräsident [a. D.]" is an office rather than a title, but it prefixes the
# name in exactly the same way and is no more part of it, so it comes off here.
# It deliberately does NOT go in STATUS_TRANSLATION: that maps to the *chair* of
# the sitting, and the head of state is not chairing it.
TITLE_PREFIX_RE = re.compile(
    r'^(?:'
    r'(?:Dr|Prof)\.(?:\s*-\s*Ing\.)?\s*'
    r'|Ing\.\s*'
    r'|HonD\s+'
    r'|h\.\s?c\.\s*'
    r'|E\.\s?h\.\s*'
    r'|habil\.\s*'
    r'|Bundespräsident(?:in)?\s+(?:a\.\s?D\.\s*)?'
    r'|\([^()]*\)\s*'
    r')+'
)

# Trailing parentheses on a speaker name carry the faction and/or the
# constituency -- "Alexander S. Neu (DIE LINKE)", "Norbert Müller (Potsdam)",
# "Christian Kühn (Tübingen) (BÜNDNIS 90/DIE GRÜNEN)". The entity registry keys
# people by the bare name, so these never help linking and must come off.
TRAILING_PARENS_RE = re.compile(r'(?:\s*\([^()]*\)\s*)+$')

# Faction labels as the media feed spells them inside the "(role/faction)" slot
# of a title. The faction itself may contain "/" (CDU/CSU, B90/Grüne), so the
# split has to anchor on the faction as a *suffix* rather than cut at the first
# "/" -- otherwise "Vorsitzender der CDU/CSU-Bundestagsfraktion/CDU/CSU" loses
# its faction. Verified against every "Redebeitrag von ..." title in the corpus.
MEDIA_FACTIONS = (
    'BÜNDNIS 90/DIE GRÜNEN',
    'Gruppe Die Linke',
    'Gruppe BSW',
    'CDU/CSU',
    'B90/Grüne',
    'B90/GRÜNE',
    'DIE LINKE.',
    'Die Linke',
    'fraktionslos',
    'AfD',
    'FDP',
    'SPD',
    'BSW',
)
_MEDIA_FACTIONS_BY_LENGTH = sorted(MEDIA_FACTIONS, key=len, reverse=True)

# Every spelling of a Bundestag faction across both sources -- the media feed's
# and the proceedings' <fraktion>. Used to *recognise* a faction, never to
# rewrite one: the entity dump stays the authority on what a faction links to.
KNOWN_FACTIONS = MEDIA_FACTIONS + ('DIE LINKE', 'BÜNDNIS 90/DIE GRÜNEN')


def _normalize_faction(label: str) -> str:
    label = re.sub(r'[^\w]+', ' ', (label or '')).strip().lower()
    return re.sub(r'\s+', ' ', label)


_KNOWN_FACTIONS_NORM = {_normalize_faction(f) for f in KNOWN_FACTIONS}


def is_known_faction(label: str) -> bool:
    """True if `label` is recognisably one of the Bundestag factions.

    Lets a caller tell a real faction from the corrupt values the source XML
    sometimes carries ("SPDSPD", "SPDCDU/CSU", "CDU/CSU: Ich glaube, ...").
    """
    norm = _normalize_faction(label)
    return bool(norm) and norm in _KNOWN_FACTIONS_NORM


def split_role_faction(value: str) -> tuple:
    """Split the media feed's "(role/faction)" title slot into (role, faction).

    The slot holds a faction ("CDU/CSU"), a role and a faction
    ("Bundestagsvizepräsidentin/B90/Grüne"), or a role alone with a dangling
    separator ("Bundesministerin/" -- ministers who hold no seat). Either part
    may be absent; both are returned as None/"" then.
    """
    value = (value or '').strip()
    if not value:
        return None, ''
    for faction in _MEDIA_FACTIONS_BY_LENGTH:
        if value == faction:
            return None, faction
        if value.endswith(f'/{faction}'):
            return value[:-(len(faction) + 1)].rstrip('/') or None, faction
    if value.endswith('/'):
        # Role without a faction.
        return value.rstrip('/') or None, ''
    return None, value

def parse_fullname(label: str) -> tuple:
    """Return a tuple (name, status)

    status will most often be None, except if the label starts with Prasident (or variants)
    """
    if label is None:
        return None

    # Strip leading/trailing non-alphabetic chars. A closing parenthesis is kept:
    # a Fragestunde speaker is a bare "<name>Dr. Alexander S. Neu (DIE LINKE) : </name>",
    # and stripping the ") : " wholesale used to eat the parenthesis that closes
    # the faction, stranding the opener and leaving the unlinkable
    # "Alexander S. Neu (DIE LINKE". fix_fullname drops the whole group instead.
    label = re.sub(r'^[^\w]+', '', label)
    label = re.sub(r'[^\w)]+$', '', label)
    # Replace non-breaking whitespaces
    label = re.sub(r'\xc2\xa0', ' ', label)
    # Replace multiple whitespaces
    label = re.sub(r'\s+', ' ', label)
    # Fix strange notation, like in 19040, 19170, 19176...
    label = label.replace('räsident in', 'räsidentin')

    # Split at the first whitespace to get possible status information
    info = re.split(r'\s+', label, maxsplit=1)
    if len(info) == 2 and info[0] in STATUS_TRANSLATION:
        return (fix_fullname(info[1]), STATUS_TRANSLATION.get(info[0]))

    # No matching key. Assume that there is no status at the beginning.
    return (fix_fullname(label), None)

def fix_fullname(label: str) -> str:
    if label is None:
        return label
    # Replace non-breaking whitespaces
    label = re.sub(r'\xc2\xa0', ' ', label)
    # Replace multiple whitespaces
    label = re.sub(r'\s+', ' ', label).strip()
    label = TITLE_PREFIX_RE.sub('', label)
    label = label.replace('Graf Graf ', 'Graf ')
    # There are 3 cases:
    # 19060: Carsten Sieling, Bürgermeister
    # 19099: Bodo Ramelow, Ministerpräsident (Thüringen
    # 19104: Dietmar Woidke, Ministerpräsident (Brandenburg
    # where a fullname has a comma with a role following. Strip it.
    if ',' in label:
        label, _ = label.split(',', 1)
    label = TRAILING_PARENS_RE.sub('', label)
    # An opener with no closer is one of the above, truncated in the source.
    if label.count('(') > label.count(')'):
        label = label[:label.index('(')]
    return label.strip()

def fix_faction(label: str) -> str:
    if label is None:
        return label
    # Replace non-breaking whitespaces (\xa0) and multiple whitespaces
    label = re.sub(r'\s+', ' ', label)
    return label.replace('B90/Grüne', 'BÜNDNIS 90/DIE GRÜNEN')

def fix_role(role: str) -> str:
    """Return a standardized role if defined.

    Else return the unchanged role.
    """
    return STATUS_TRANSLATION.get(role, role)

def fixup_execute(fix: dict, entry: dict) -> dict:
    """Execute a fixup action on entry.

    The action may transform things in place.
    Return the transformed entry.
    """
    if fix['action'] == 'replace':
        value = entry.get(fix['field'])
        if value is None:
            logger.debug(f"No value for field {fix['field']} in fixup action")
            return entry
        new_value = re.sub(fix['from'], fix['to'], value)
        if new_value != value:
            entry[f"{fix['field']}-original"] = value
            entry[fix['field']] = new_value
    else:
        logger.error(f"Unknown action {fix['action']}")
    return entry
