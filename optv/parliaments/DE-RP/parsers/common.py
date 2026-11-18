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
    'Landtagspräsident': 'president',
    'Landtagspräsidentin': 'president',
    'Landtagsvizepräsident': 'vice-president',
    'Landtagsvizepräsidentin': 'vice-president',
}

# OPAL puts either the faction or an office in the parentheses after a
# speaker's name: chair titles, ministry codes (`MdI`, `FM`, `MKUEM`),
# `Ministerpräsident`, `Chef der Staatskanzlei`, ... Anything that does not
# look like an office is treated as a faction, so a new faction name is not
# mistaken for an office.
_OFFICE_CODE_RE = re.compile(r'^(?:M[A-Za-z]{1,7}|[A-Z]{2})$')
_OFFICE_WORDS = ('präsident', 'minister', 'staatskanzlei', 'staatssekretär',
                 'bevollmächtigt', 'beauftragt')


def is_office(value: str) -> bool:
    """True if OPAL's parenthesised value is an office, not a faction."""
    value = (value or '').strip()
    if not value:
        return False
    if value in STATUS_TRANSLATION or _OFFICE_CODE_RE.match(value):
        return True
    lower = value.lower()
    return any(word in lower for word in _OFFICE_WORDS)

def fix_fullname(label: str) -> str:
    if label is None:
        return label
    # Replace non-breaking whitespaces
    label = re.sub(r'\xc2\xa0', ' ', label)
    # Replace multiple whitespaces
    label = re.sub(r'\s+', ' ', label)
    label = label.replace('Dr. ', '').replace('h. c. ', '').replace('Prof. ', '').replace('Graf Graf ', 'Graf ')
    # There are 3 cases:
    # 19060: Carsten Sieling, Bürgermeister
    # 19099: Bodo Ramelow, Ministerpräsident (Thüringen
    # 19104: Dietmar Woidke, Ministerpräsident (Brandenburg
    # where a fullname has a comma with a role following. Strip it.
    if ',' in label:
        label, _ = label.split(',', 1)
    return label

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
