"""Names as the Chamber writes them in its data: surname in capitals first, given names after (``DELLA VEDOVA Benedetto``)."""

from __future__ import annotations


def _is_surname_part(token: str) -> bool:
    return any(c.isalpha() for c in token) and token == token.upper()


def split_name(surname_first: str) -> tuple[str, str]:
    """``"DELLA VEDOVA Benedetto"`` → ``("Benedetto", "Della Vedova")``. A name without a part in capitals is returned as given name only."""
    tokens = (surname_first or "").split()
    cut = 0
    while cut < len(tokens) and _is_surname_part(tokens[cut]):
        cut += 1
    if cut == len(tokens):
        # Capitals throughout: nothing tells the given name from the surname.
        return "", " ".join(t.title() for t in tokens)
    return " ".join(tokens[cut:]), " ".join(t.title() for t in tokens[:cut])


def person_key(surname_first: str) -> str:
    """The form two spellings of one name are compared in."""
    return " ".join((surname_first or "").upper().split())


def person(surname_first: str, *, person_id: str | None = None, role: str | None = None,
           group: str | None = None) -> dict:
    """A Stage 2 person from a name in the Chamber's form."""
    first, last = split_name(surname_first)
    record: dict = {"label": f"{first} {last}".strip()}
    if first:
        record["firstname"] = first
    if last:
        record["lastname"] = last
    if role:
        record["role"] = role
    if group:
        record["faction"] = {"label": group}
    if person_id:
        record["originPersonID"] = person_id
    return record
