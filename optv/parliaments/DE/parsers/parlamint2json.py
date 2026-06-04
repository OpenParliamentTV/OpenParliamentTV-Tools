#! /usr/bin/env python3

# Parse ParlaMint-DE_beta TEI proceedings into the same intermediate JSON
# shape that proceedings2json.py produces from Bundestag-native TEI.
#
# Used for periods 16-17, where Bundestag does not publish proceedings in
# its native machine-readable format. ParlaMint provides Wikidata Q-IDs for
# every speaker via a central listPerson registry, so people[].wid is
# populated directly by the parser (NEL is a no-op for these speeches).

from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
from lxml import etree
from pathlib import Path
import re
import sys

from spacy.lang.de import German

# Allow relative imports (.common) and absolute imports (optv.shared.*) when
# invoked as a script.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))               # for .common
    sys.path.insert(0, str(module_dir.parents[3]))           # for optv.shared.*
    __package__ = module_dir.name

from .common import fix_fullname
from optv.shared.agenda_types import classify_parlamint_de, is_de_closing_chair_text

# Match proceedings2json: rule-based sentencizer, loaded once per process.
_nlp = German()
_nlp.add_pipe("sentencizer")


def _split_sentences(text: str) -> list:
    return [{"text": str(s).strip()} for s in _nlp(text).sents if str(s).strip()]

# ParlaMint-DE (periods 16-17) rights come from the manifest proceedings
# `overrides` entry; kept as module constants so the references below are
# unchanged (and byte-identical to the previous hardcoded values).
from optv.parliaments import get_rights as _get_rights
from optv.parliaments import get_language as _get_language

_PARLAMINT_RIGHTS = _get_rights("DE", 17, "proceedings")
PROCEEDINGS_LICENSE = _PARLAMINT_RIGHTS["license"]
PROCEEDINGS_LANGUAGE = _get_language("DE")
PROCEEDINGS_SOURCE = _PARLAMINT_RIGHTS["source"]
PROCEEDINGS_CREATOR = _PARLAMINT_RIGHTS["creator"]

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
NSMAP = {"tei": TEI_NS}


# Map ParlaMint debateSection @ana tokens to a human-readable agenda title.
# Used as a fallback when <head> is absent.
# Normalize ParlaMint parliamentaryGroup abbreviations to the labels used
# by Bundestag-native data and the RSS feed (so merger speaker-dedup works).
FACTION_LABEL_MAP = {
    "CDU_CSU": "CDU/CSU",
    "DIE_LINKE": "DIE LINKE",
    "GRUENE": "BÜNDNIS 90/DIE GRÜNEN",
    "DP_FVP": "DP/FVP",
    "DRP_NR": "DRP/NR",
    "GB_BHE": "GB/BHE",
}


SECTION_LABELS = {
    "DE-debate": "Debatte",
    "DE-question_time": "Fragestunde",
    "DE-current_affairs": "Aktuelle Stunde",
    "DE-government_declaration": "Regierungserklärung",
    "DE-opening_speech": "Eröffnungsrede",
    "DE-election": "Wahl",
    "DE-assumption": "Amtsübernahme",
    "DE-rules_of_procedure": "Geschäftsordnung",
    "DE-oath": "Vereidigung",
    "DE-condolence": "Würdigung",
    "DE-sworn_in": "Vereidigung",
    "DE-swearing_in": "Vereidigung",
}


def _xml_id(elem) -> str | None:
    return elem.get(f"{{{XML_NS}}}id")


def _date_in_range(date: str, frm: str | None, to: str | None) -> bool:
    if frm and date < frm:
        return False
    if to and date > to:
        return False
    return True


def _wid_from_idno(elem) -> str | None:
    """Extract a wikidata Q-ID from an <idno subtype="wikimedia"> child."""
    for idno in elem.findall("tei:idno[@subtype='wikimedia']", NSMAP):
        if idno.text and "wikidata.org/entity/" in idno.text:
            m = re.search(r"/entity/(Q\d+)", idno.text)
            if m:
                return m.group(1)
    return None


_REGISTRY_CACHE: dict = {}


def _load_registries(proceedings_dir: Path) -> tuple[dict, dict]:
    """Load listPerson + listOrg into in-memory dicts.

    Returns (persons, orgs).
    persons[Qid] = {label, firstname, lastname, affiliations: [(ref, frm, to, role)]}
    orgs[org_id] = {label, abbrev, wid}
    """
    person_file = proceedings_dir / "ParlaMint-DE-listPerson.xml"
    org_file = proceedings_dir / "ParlaMint-DE-listOrg.xml"
    if not person_file.exists() or not org_file.exists():
        raise FileNotFoundError(
            f"Missing ParlaMint registries in {proceedings_dir} "
            f"(expected ParlaMint-DE-listPerson.xml and -listOrg.xml)"
        )
    cache_key = (str(person_file), person_file.stat().st_mtime,
                 str(org_file), org_file.stat().st_mtime)
    if _REGISTRY_CACHE.get("key") == cache_key:
        return _REGISTRY_CACHE["persons"], _REGISTRY_CACHE["orgs"]

    orgs: dict = {}
    org_root = etree.parse(str(org_file)).getroot()
    for org in org_root.findall(".//tei:org", NSMAP):
        oid = _xml_id(org)
        if not oid:
            continue
        full = org.find("tei:orgName[@full='yes']", NSMAP)
        abbrev = org.find("tei:orgName[@full='abb']", NSMAP)
        orgs[oid] = {
            "label": (full.text if full is not None else (abbrev.text if abbrev is not None else oid)),
            "abbrev": abbrev.text if abbrev is not None else None,
            "wid": _wid_from_idno(org),
        }

    persons: dict = {}
    person_root = etree.parse(str(person_file)).getroot()
    for person in person_root.findall(".//tei:person", NSMAP):
        qid = _xml_id(person)
        if not qid:
            continue
        # Collect all <persName> variants with their date ranges.
        # An MP may have multiple entries (e.g. before/after a marriage-
        # related surname change); pick the one active on session_date
        # at use-site via _resolve_persname().
        persname_entries: list = []
        for pname in person.findall("tei:persName", NSMAP):
            firstname, lastname = "", ""
            for child in pname:
                tag = etree.QName(child.tag).localname
                if tag == "forename" and child.text:
                    firstname = (firstname + " " + child.text).strip()
                elif tag == "surname" and child.text:
                    lastname = (lastname + " " + child.text).strip()
            persname_entries.append({
                "from": pname.get("from"),
                "to": pname.get("to"),
                "firstname": firstname,
                "lastname": lastname,
            })
        affiliations = []
        for aff in person.findall("tei:affiliation", NSMAP):
            affiliations.append((
                aff.get("ref", ""),
                aff.get("from"),
                aff.get("to"),
                aff.get("role", ""),
            ))
        persons[qid] = {
            "persname_entries": persname_entries,
            "affiliations": affiliations,
        }

    _REGISTRY_CACHE["key"] = cache_key
    _REGISTRY_CACHE["persons"] = persons
    _REGISTRY_CACHE["orgs"] = orgs
    return persons, orgs


def _resolve_persname(person: dict, session_date: str, qid: str) -> tuple[str, str, str]:
    """Return (label, firstname, lastname) for the persName active on session_date.

    For marriage/lastname-change cases (different lastnames across entries),
    picks the entry whose [from, to] covers session_date. For middle-name
    expansion cases (same lastname, firstnames form a prefix-with-space
    pattern like "Michael" vs "Michael Georg"), prefers the short base form
    regardless of date — media/RSS consistently use the common short name.
    """
    entries = person.get("persname_entries") or []
    if not entries:
        return qid, "", ""

    if len({e["lastname"] for e in entries}) == 1 and len(entries) >= 2:
        firstnames = sorted({e["firstname"] for e in entries}, key=len)
        base = firstnames[0]
        if all(f == base or f.startswith(base + " ") for f in firstnames):
            picked = next(e for e in entries if e["firstname"] == base)
            firstname, lastname = picked["firstname"], picked["lastname"]
            label = fix_fullname(f"{firstname} {lastname}".strip()) or qid
            return label, firstname, lastname

    picked = None
    for entry in entries:
        if _date_in_range(session_date, entry.get("from"), entry.get("to")):
            picked = entry
            break
    if picked is None:
        picked = entries[0]
    firstname = picked["firstname"]
    lastname = picked["lastname"]
    label = fix_fullname(f"{firstname} {lastname}".strip()) or qid
    return label, firstname, lastname


def _resolve_faction(person: dict, orgs: dict, session_date: str) -> dict | None:
    """Pick the parliamentaryGroup affiliation active on session_date."""
    candidate = None
    for ref, frm, to, role in person["affiliations"]:
        if not ref.startswith("#parliamentaryGroup."):
            continue
        if not _date_in_range(session_date, frm, to):
            continue
        candidate = ref
        break
    if candidate is None:
        return None
    org = orgs.get(candidate.lstrip("#"))
    if not org:
        return None
    abbrev = org.get("abbrev") or org["label"]
    label = FACTION_LABEL_MAP.get(abbrev, abbrev)
    item: dict = {"label": label}
    if org.get("wid"):
        item["wid"] = org["wid"]
        item["wtype"] = "ORG"
    return item


def _person_type(person: dict, session_date: str) -> str:
    """memberOfGovernment if a government affiliation is active on session_date,
    else memberOfParliament."""
    for ref, frm, to, role in person["affiliations"]:
        if ref == "#government.DE" and role in ("head", "minister"):
            if _date_in_range(session_date, frm, to):
                return "memberOfGovernment"
    return "memberOfParliament"


def _is_chair_u(u) -> bool:
    return "#chair" in (u.get("ana", "") or "")


def _context_for_u(u, role_status: str | None, is_first_in_section: bool, main_speaker_wid: str | None, who_qid: str) -> tuple[str, str | None]:
    """Return (context, new_main_speaker_wid).

    context is one of: main-speaker, speaker, president, vice-president,
    interim-president.
    """
    ana = u.get("ana", "") or ""
    if "#chair" in ana:
        if role_status:
            return role_status, main_speaker_wid
        return "speaker", main_speaker_wid
    # #regular
    if main_speaker_wid is None or who_qid == main_speaker_wid:
        return "main-speaker", who_qid
    return "speaker", main_speaker_wid


CHAIR_ROLE_RE = re.compile(
    r"^\s*(Alterspr[äa]sident(?:in)?|Vizepr[äa]sident(?:in)?|Pr[äa]sident(?:in)?)\b",
    re.IGNORECASE,
)
ROLE_TO_CONTEXT = {
    "alterspräsident": "interim-president",
    "alterspräsidentin": "interim-president",
    "vizepräsident": "vice-president",
    "vizepräsidentin": "vice-president",
    "präsident": "president",
    "präsidentin": "president",
}


def _chair_role_from_note(note_text: str | None) -> str | None:
    """Extract speaker context from a <note type='speaker'> preceding a chair turn."""
    if not note_text:
        return None
    m = CHAIR_ROLE_RE.match(note_text)
    if not m:
        return None
    return ROLE_TO_CONTEXT.get(m.group(1).lower())


def _extract_seg_text(seg) -> str:
    """Plain text of a <seg>, joining text + tail of children."""
    parts = []
    if seg.text:
        parts.append(seg.text)
    for child in seg:
        if child.text:
            parts.append(child.text)
        if child.tail:
            parts.append(child.tail)
    text = " ".join(p.strip() for p in parts if p and p.strip())
    return re.sub(r"\s+", " ", text).strip()


def _kinesic_text(elem) -> str:
    """Return descriptive text for kinesic/vocal/incident, e.g. '(Beifall ...)'."""
    desc = elem.find("tei:desc", NSMAP)
    if desc is not None and desc.text:
        return f"({desc.text.strip()})"
    if elem.text and elem.text.strip():
        return f"({elem.text.strip()})"
    typ = elem.get("type") or etree.QName(elem.tag).localname
    return f"({typ})"


def _chair_u_text(u) -> str:
    """Concatenate all <seg> text inside a chair <u> (for TOP extraction)."""
    return " ".join((seg.text or "").strip() for seg in u.findall("tei:seg", NSMAP) if seg.text and seg.text.strip())


# Match the chair's "Ich rufe [die|den|das] (Tagesordnungspunkt(e)|Zusatzpunkt(e)|Einzelplan) N[ a]" announcement.
# These short labels are what media2json's fix_title produces on the media side.
# The leading alternation also accepts the verb-first inversion the chair
# uses just as often ("Jetzt/Nun rufe ich Tagesordnungspunkt N auf") — without
# it ~76 DE-17 chair-transition turns were mis-typed `regular` and shipped as
# gate-passing cps mis-merges (e.g. 17108 #76). The inversion alternative is
# non-capturing, so groups (1)=keyword / (2)=number are unchanged.
_TOP_ANNOUNCE_RE = re.compile(
    r"(?:Ich\s+rufe|(?:Jetzt|Nun|Dann|Sodann|Damit)\s+rufe\s+ich)\s+"
    r"(?:nun\s+|jetzt\s+)?"
    r"(?:die\s+|den\s+|das\s+)?"
    r"(Tagesordnungspunkte?|Zusatzpunkte?|Einzelplan)"
    r"\s+(\d+)",
    re.IGNORECASE,
)

# Match a chair announcement that the following speeches are written submissions
# ("zu Protokoll genommen") rather than delivered orally. Used to filter out
# Protokoll-rede MP <u>s that have no corresponding audio.
_PROTOKOLL_ANNOUNCE_RE = re.compile(
    r"\bRede(?:n)?\b.*?\bzu\s+Protokoll\b"
    r"|\bzu\s+Protokoll\s+(?:gegeben|genommen|gehen|nehmen)\b",
    re.IGNORECASE | re.DOTALL,
)


def _chair_announces_protokoll(u) -> bool:
    """True if a chair <u>'s last <seg> announces that subsequent MP speeches
    are recorded in writing only (zu Protokoll). When True, the following
    non-chair <u>s in document order are Protokoll-rede submissions and have
    no corresponding audio — the parser excludes them from the output.

    Detection is on the LAST <seg> only because chair handovers often pack
    multiple actions ("Überweisung…, Reden zu Protokoll, ich rufe TOP X auf")
    into one <u>, and the Protokoll instruction is the trailing one when it
    applies to the next batch of <u>s.
    """
    segs = u.findall("tei:seg", NSMAP)
    if not segs:
        return False
    last_text = (segs[-1].text or "")
    return bool(_PROTOKOLL_ANNOUNCE_RE.search(last_text))


def _extract_top_title(text: str) -> str | None:
    """Return a media-style short title ('Tagesordnungspunkt N' / 'Zusatzpunkt N' / 'Einzelplan N')
    from a chair's 'Ich rufe ... auf' announcement, or None if no match."""
    if not text:
        return None
    m = _TOP_ANNOUNCE_RE.search(text)
    if not m:
        return None
    keyword = m.group(1).lower()
    number = m.group(2)
    if keyword.startswith("tagesordnungspunkt"):
        return f"Tagesordnungspunkt {number}"
    if keyword.startswith("zusatzpunkt"):
        return f"Zusatzpunkt {number}"
    return f"Einzelplan {number}"


def _find_top_announce_split(text_body: list) -> int | None:
    """Return the index of the first 'speech'-type textBody item whose text
    contains an 'Ich rufe TOP N auf' announcement.

    Used to split a chair-transition <u> (which packs both the close of the
    previous TOP and the announcement of the next TOP) into two emitted
    speeches so that Needleman-Wunsch alignment in the merger has one
    proceedings entry per chair media clip.
    """
    for i, b in enumerate(text_body):
        if b.get("type") != "speech":
            continue
        if _TOP_ANNOUNCE_RE.search(b.get("text") or ""):
            return i
    return None


def _compute_section_titles(sections) -> list:
    """Pre-pass: for each debateSection, derive a short 'Tagesordnungspunkt N' style title
    from the chair's spoken announcement. Returns a list (same length as sections) with either
    the short title or None (caller falls back to <head> text).

    ParlaMint packs each abstimmung + next-TOP-announcement into one chair <u>. So section K's TOP is
    typically announced at the end of section K-1. Section 0 announces its own TOPs in its opening chair.
    """
    titles: list = [None] * len(sections)
    for i, section in enumerate(sections):
        us = section.findall("tei:u", NSMAP)
        chair_us = [u for u in us if "#chair" in (u.get("ana") or "")]
        if i == 0:
            # Section 0: look at its own first chair <u>
            source_u = chair_us[0] if chair_us else None
        else:
            # Section K (K>0): look at previous section's last chair <u>
            prev_chair_us = [u for u in sections[i - 1].findall("tei:u", NSMAP)
                             if "#chair" in (u.get("ana") or "")]
            source_u = prev_chair_us[-1] if prev_chair_us else None
        if source_u is not None:
            titles[i] = _extract_top_title(_chair_u_text(source_u))
    return titles


def _get_session_metadata(root) -> dict:
    """Pull period, session number, date from TEI header."""
    nsmap = NSMAP
    period = None
    session = None
    for meeting in root.findall(".//tei:meeting", nsmap):
        ana = meeting.get("ana", "") or ""
        if "#parla.term" in ana:
            n = meeting.get("n")
            if n and n.isdigit():
                period = int(n)
        elif "#parla.sitting" in ana:
            n = meeting.get("n")
            if n and n.isdigit():
                session = int(n)
    date_elem = root.find(".//tei:setting/tei:date[@ana='#parla.sitting']", nsmap)
    if date_elem is None:
        date_elem = root.find(".//tei:profileDesc//tei:date", nsmap)
    date = (date_elem.get("when") if date_elem is not None else None) or "1900-01-01"
    if period is None or session is None:
        # Try parsing from xml:id like ParlaMint-DE_2005-10-18-16-001
        xid = _xml_id(root) or ""
        m = re.search(r"-(\d{2})-(\d{3})$", xid)
        if m:
            period = period or int(m.group(1))
            session = session or int(m.group(2))
    return {"period": period, "session": session, "date": date}


def parse_transcript(filename: str, sourceUri: str | None = None, args=None):
    """Generator yielding speech dicts in the same shape as proceedings2json.parse_transcript."""
    filename = str(filename)
    tree = etree.parse(filename)
    root = tree.getroot()

    # Source URL: PI we injected at download time, else the bibl idno
    source_urls = [n.get("url") for n in root.xpath("preceding-sibling::node()")
                   if getattr(n, "target", None) == "source"]
    if source_urls:
        sourceUri = source_urls[0]
    elif sourceUri is None:
        bibl_idno = root.find(".//tei:sourceDesc/tei:bibl/tei:idno", NSMAP)
        sourceUri = bibl_idno.text if bibl_idno is not None and bibl_idno.text else filename

    meta = _get_session_metadata(root)
    period = meta["period"]
    session_no = meta["session"]
    session_date = meta["date"]
    if period is None or session_no is None:
        logger.error(f"Could not determine period/session for {filename}")
        return
    session_id = f"{period}{str(session_no).zfill(3)}"

    # Local naive timestamps; merger overrides from RSS timing.
    dateStart = f"{session_date}T00:00:00"
    dateEnd = f"{session_date}T23:59:59"

    proceedings_dir = Path(filename).parent
    persons, orgs = _load_registries(proceedings_dir)

    session_metadata = {
        "parliament": "DE",
        "electoralPeriod": {"number": period},
        "session": {
            "number": session_no,
            "dateStart": dateStart,
            "dateEnd": dateEnd,
        },
    }

    speech_index = 1001

    sections = root.findall(".//tei:div[@type='debateSection']", NSMAP)
    # Pre-pass: extract media-style short titles ('Tagesordnungspunkt N', ...) from chair announcements.
    short_titles = _compute_section_titles(sections)

    # Pre-pass: identify Protokoll-rede MP <u> elements across the whole
    # document. A chair <u> whose last <seg> says "Reden zu Protokoll" marks
    # subsequent non-chair <u>s as written-only submissions until the next
    # chair <u> without that marker. These submissions have no audio and are
    # excluded from the parser's output (OPTV is a video platform).
    # Keyed by xml:id (stable across lxml lookups, unlike Python id()).
    protokoll_u_ids: set = set()
    in_protokoll = False
    for section in sections:
        for u in section.findall("tei:u", NSMAP):
            if _is_chair_u(u):
                in_protokoll = _chair_announces_protokoll(u)
            elif in_protokoll:
                uid = _xml_id(u)
                if uid:
                    protokoll_u_ids.add(uid)

    for section_idx, section in enumerate(sections):
        head = section.find("tei:head", NSMAP)
        head_title = (head.text or "").strip() if head is not None else ""
        if not head_title:
            ana_token = (section.get("ana") or "").lstrip("#").split()[0] if section.get("ana") else ""
            head_title = SECTION_LABELS.get(ana_token, ana_token or "Tagesordnungspunkt")

        # Prefer the short chair-announced title (matches media2json fix_title output);
        # fall back to <head> text if the chair did not explicitly announce a TOP.
        section_title = short_titles[section_idx] or head_title

        section_id = _xml_id(section) or f"section{section.get('n', '')}"
        section_native_type, section_core_type = classify_parlamint_de(section.get("ana"))

        # Group <u> elements into rede-equivalent groups before yielding.
        # In Bundestag video, one clip typically covers the chair introduction
        # plus the speaker's turn (and any interjections). ParlaMint has each
        # of these as a separate <u>. Grouping them reduces the proceedings-
        # to-media ratio from ~3:1 to ~1.3:1, dramatically improving NW
        # alignment.
        children = list(section)

        # Classify <u> positions
        u_positions = []
        for ci, child in enumerate(children):
            if etree.QName(child.tag).localname == "u":
                u_positions.append((ci, _is_chair_u(child)))

        # A new rede starts at a chair <u> followed by a non-chair <u>
        rede_starts = []
        for ui, (ci, is_chair) in enumerate(u_positions):
            if is_chair and ui + 1 < len(u_positions) and not u_positions[ui + 1][1]:
                rede_starts.append(ui)

        # mini_debate_mode: section has non-chair speakers followed by a single closing chair,
        # with no chair->non-chair transition. Media has ONE chair announcement clip for these;
        # collapsing all <u>s into one proc with chair as main-speaker aligns the cardinalities.
        mini_debate_mode = False

        if not rede_starts:
            chair_indices = [ui for ui, (_, is_chair) in enumerate(u_positions) if is_chair]
            has_non_chair = any(not is_chair for _, is_chair in u_positions)
            # Chair-at-end pattern: at least one non-chair before the chair, last <u> is chair.
            if (has_non_chair and chair_indices
                    and chair_indices[-1] == len(u_positions) - 1):
                rede_groups = [(0, len(u_positions))]
                mini_debate_mode = True
            else:
                rede_groups = [(ui, ui + 1) for ui in range(len(u_positions))]
        else:
            rede_groups = []
            for ui in range(rede_starts[0]):
                rede_groups.append((ui, ui + 1))
            for gi, start in enumerate(rede_starts):
                end = rede_starts[gi + 1] if gi + 1 < len(rede_starts) else len(u_positions)
                # Session-opening split: in the first section, if the first rede_group would bundle
                # the opening chair <u> ("Die Sitzung ist eröffnet...") with the first speaker,
                # emit the chair <u> as its own proc so media's opening chair clips have something to
                # match against on the speaker axis.
                if (section_idx == 0 and gi == 0 and start == 0
                        and u_positions[start][1] and end > start + 1):
                    rede_groups.append((start, start + 1))
                    rede_groups.append((start + 1, end))
                else:
                    rede_groups.append((start, end))

            # Split a trailing chair-only run off the last rede. The
            # rede-start rule only fires on chair -> non-chair transitions, so
            # a chair <u> at the end of a section (e.g. "Die Sitzung ist
            # geschlossen") otherwise gets absorbed into the preceding rede
            # alongside its non-chair speakers. Emitting it as its own rede
            # gives the session-closing media clip a matching proc.
            if rede_groups:
                last_start, last_end = rede_groups[-1]
                tail = last_end
                while tail > last_start and u_positions[tail - 1][1]:
                    tail -= 1
                has_non_chair_before_tail = any(
                    not u_positions[ui][1] for ui in range(last_start, tail)
                )
                if tail < last_end and has_non_chair_before_tail:
                    rede_groups[-1] = (last_start, tail)
                    rede_groups.append((tail, last_end))

        # Collapse consecutive rede_groups when the original speaker of the
        # preceding group returns in the next group. Two patterns are merged:
        # (1) mid-speech chair-only interjection (clock warning, handover
        # etiquette) — the chair talks, then the same speaker continues;
        # (2) Zwischenfrage — chair announces, a different MP asks a brief
        # question, original speaker answers and continues. Both are single
        # continuous speeches in the media (one clip per speaker-turn, not
        # per rede-boundary), so the parser must match that shape. The merge
        # condition is: the LAST non-chair of the next group equals the FIRST
        # non-chair of the current group. This subsumes Fix E's same-first-
        # non-chair rule. Chair-only groups are never merged. Skipped in
        # mini_debate_mode (already collapsed).
        if not mini_debate_mode and len(rede_groups) > 1:
            def _first_non_chair_wid(start_ui, end_ui):
                for ui in range(start_ui, end_ui):
                    ci_u, is_chair = u_positions[ui]
                    if not is_chair:
                        return (children[ci_u].get("who") or "").lstrip("#")
                return None
            def _last_non_chair_wid(start_ui, end_ui):
                for ui in range(end_ui - 1, start_ui - 1, -1):
                    ci_u, is_chair = u_positions[ui]
                    if not is_chair:
                        return (children[ci_u].get("who") or "").lstrip("#")
                return None
            merged = [rede_groups[0]]
            for nxt in rede_groups[1:]:
                prev = merged[-1]
                prev_wid = _first_non_chair_wid(*prev)
                nxt_last_wid = _last_non_chair_wid(*nxt)
                if prev_wid and prev_wid == nxt_last_wid:
                    merged[-1] = (prev[0], nxt[1])
                else:
                    merged.append(nxt)
            rede_groups = merged

        for start_ui, end_ui in rede_groups:
            first_ci = u_positions[start_ui][0]
            next_u_ci = u_positions[end_ui][0] if end_ui < len(u_positions) else len(children)

            main_speaker_wid: str | None = None
            last_speaker_note: str | None = None
            all_people: dict = {}
            all_text_body: list = []
            first_speech_id: str | None = None

            for ci in range(first_ci, next_u_ci):
                child = children[ci]
                tag = etree.QName(child.tag).localname

                if tag == "head":
                    continue
                if tag == "note" and child.get("type") == "speaker":
                    last_speaker_note = (child.text or "").strip()
                    continue
                if tag == "u":
                    # Skip Protokoll-rede MP <u>s identified in the pre-pass:
                    # written-only submissions with no audio (OPTV is a video
                    # platform).
                    if _xml_id(child) in protokoll_u_ids:
                        continue
                    who = (child.get("who") or "").lstrip("#")
                    if not who:
                        logger.warning(f"<u> without @who in {filename}")
                        continue
                    person_info = persons.get(who)
                    role_status = _chair_role_from_note(last_speaker_note)
                    context, main_speaker_wid = _context_for_u(
                        child, role_status, first_speech_id is None,
                        main_speaker_wid, who
                    )

                    if person_info:
                        label, firstname, lastname = _resolve_persname(
                            person_info, session_date, who
                        )
                        person_type = _person_type(person_info, session_date)
                        faction = _resolve_faction(person_info, orgs, session_date)
                    else:
                        logger.warning(f"Unknown speaker {who} in {filename}")
                        label = who
                        firstname = ""
                        lastname = ""
                        person_type = "memberOfParliament"
                        faction = None

                    person_item: dict = {
                        "type": person_type,
                        "label": label,
                        "context": context,
                        "wid": who,
                        "wtype": "PERSON",
                    }
                    if firstname:
                        person_item["firstname"] = firstname
                    if lastname:
                        person_item["lastname"] = lastname
                    if faction:
                        person_item["faction"] = faction

                    speech_id = _xml_id(child) or f"{session_id}.{section_id}.r{speech_index}.u{len(all_people)}"
                    if first_speech_id is None:
                        first_speech_id = speech_id
                    if label not in all_people:
                        all_people[label] = person_item

                    for seg_or_other in child:
                        sub_tag = etree.QName(seg_or_other.tag).localname
                        if sub_tag == "seg":
                            text = _extract_seg_text(seg_or_other)
                            if not text:
                                continue
                            all_text_body.append({
                                "speech_id": speech_id,
                                "type": "speech",
                                "speaker": label,
                                "speakerstatus": context,
                                "text": text,
                                "sentences": _split_sentences(text),
                            })
                        elif sub_tag in ("kinesic", "vocal", "incident"):
                            text = _kinesic_text(seg_or_other)
                            all_text_body.append({
                                "speech_id": speech_id,
                                "type": "comment",
                                "speaker": None,
                                "speakerstatus": None,
                                "text": text,
                                "sentences": [{"text": text}],
                            })

                    last_speaker_note = None
                    continue

                if tag in ("kinesic", "vocal", "incident", "note"):
                    if tag == "note" and child.get("type") == "speaker":
                        last_speaker_note = (child.text or "").strip()
                        continue
                    text = _kinesic_text(child) if tag != "note" else (child.text or "").strip()
                    if not text:
                        continue
                    if tag == "note" and child.get("type") != "speaker":
                        text = f"({text})"
                    sid = first_speech_id or f"{session_id}.{section_id}.r{speech_index}"
                    all_text_body.append({
                        "speech_id": sid,
                        "type": "comment",
                        "speaker": None,
                        "speakerstatus": None,
                        "text": text,
                        "sentences": [{"text": text}],
                    })

            if not first_speech_id:
                continue

            has_main_speaker = any(p["context"] == "main-speaker" for p in all_people.values())

            # Mini-debate collapse OR chair-only rede_group (Fix 1 session-opening split):
            # promote the chair to main-speaker so NW can align speaker-to-speaker against
            # the media's chair-announcement clip. In mini-debate mode this overrides an
            # existing main-speaker; otherwise it only fires when no main-speaker was picked
            # (all-chair rede_group).
            if mini_debate_mode or not has_main_speaker:
                chair_who = None
                for ci in range(first_ci, next_u_ci):
                    child = children[ci]
                    if etree.QName(child.tag).localname == "u" and _is_chair_u(child):
                        chair_who = (child.get("who") or "").lstrip("#")
                chair_person = next((p for p in all_people.values() if p.get("wid") == chair_who), None)
                if chair_person is not None:
                    for person in all_people.values():
                        if person["context"] == "main-speaker":
                            person["context"] = "speaker"
                    chair_person["context"] = "main-speaker"

            people_list = list(all_people.values())
            people_list.sort(key=lambda p: 0 if p["context"] == "main-speaker" else 1)

            agenda_item = {"officialTitle": section_title, "type": section_core_type}
            if section_native_type:
                agenda_item["nativeType"] = section_native_type

            # Closing detection: ParlaMint has no `ana` token for closing.
            # A chair-only rede_group whose text says "Die Sitzung ist
            # geschlossen" / "schließe die Sitzung" / etc. is the session
            # close — override the inherited section type for this speech.
            # TOP-transition detection: a chair-only rede that contains
            # "Ich rufe TOP N auf" is the chair announcing the next agenda
            # item (DE-17-F02 in the audit). The proceedings text is framing,
            # not substantive speech matching one media clip — mark as
            # `procedural` so the merger gate-fails it.
            chair_intro_split_idx: int | None = None
            if not has_main_speaker:
                chair_text = " ".join(b.get("text") or "" for b in all_text_body)
                if is_de_closing_chair_text(chair_text):
                    agenda_item["type"] = "closing"
                    agenda_item["nativeType"] = "DE-closing"
                elif _TOP_ANNOUNCE_RE.search(chair_text):
                    agenda_item["type"] = "procedural"
                    agenda_item["nativeType"] = "DE-chair_transition"
                    # Bundestag publishes a separate short chair clip for each
                    # new TOP ("Ich rufe TOP N auf …") that ParlaMint folds into
                    # the previous TOP's closing chair <u>. Without splitting,
                    # NW alignment in the merger has 3 media (close-prev,
                    # open-new, first-MP) competing for 2 proceedings entries
                    # and ends up sharing the first-MP utterance across two
                    # media slots → first MP of every TOP gate-fails with
                    # linkedMediaIndexes=[N, N+1]. Splitting at the announce
                    # boundary restores 3-to-3 alignment. Both halves stay
                    # procedural-typed so the chair-transition rule gate-fails
                    # them at the merger; only the recovered first-MP speech
                    # newly passes the gate.
                    idx = _find_top_announce_split(all_text_body)
                    if idx is not None and 0 < idx < len(all_text_body):
                        chair_intro_split_idx = idx

            if chair_intro_split_idx is not None:
                close_body = all_text_body[:chair_intro_split_idx]
                open_body = all_text_body[chair_intro_split_idx:]
                new_top_title = _extract_top_title(open_body[0].get("text") or "")

                yield {
                    **session_metadata,
                    "speechIndex": speech_index,
                    "originID": first_speech_id,
                    "agendaItem": agenda_item,
                    "debug": {"proceedings-source": PROCEEDINGS_SOURCE},
                    "people": people_list,
                    "textContents": [{
                        "type": "proceedings",
                        "sourceURI": sourceUri,
                        "creator": PROCEEDINGS_CREATOR,
                        "license": PROCEEDINGS_LICENSE,
                        "language": PROCEEDINGS_LANGUAGE,
                        "originTextID": first_speech_id,
                        "textBody": close_body,
                    }],
                    "documents": [],
                }
                speech_index += 1

                open_origin_id = f"{first_speech_id}+open"
                open_agenda = {
                    "officialTitle": new_top_title or section_title,
                    "type": "procedural",
                    "nativeType": "DE-chair_transition",
                }
                yield {
                    **session_metadata,
                    "speechIndex": speech_index,
                    "originID": open_origin_id,
                    "agendaItem": open_agenda,
                    "debug": {
                        "proceedings-source": PROCEEDINGS_SOURCE,
                        "chair_intro_split": True,
                    },
                    "people": people_list,
                    "textContents": [{
                        "type": "proceedings",
                        "sourceURI": sourceUri,
                        "creator": PROCEEDINGS_CREATOR,
                        "license": PROCEEDINGS_LICENSE,
                        "language": PROCEEDINGS_LANGUAGE,
                        "originTextID": open_origin_id,
                        "textBody": open_body,
                    }],
                    "documents": [],
                }
                speech_index += 1
                continue

            yield {
                **session_metadata,
                "speechIndex": speech_index,
                "originID": first_speech_id,
                "agendaItem": agenda_item,
                "debug": {"proceedings-source": PROCEEDINGS_SOURCE},
                "people": people_list,
                "textContents": [{
                    "type": "proceedings",
                    "sourceURI": sourceUri,
                    "creator": PROCEEDINGS_CREATOR,
                    "license": PROCEEDINGS_LICENSE,
                    "language": PROCEEDINGS_LANGUAGE,
                    "originTextID": first_speech_id,
                    "textBody": all_text_body,
                }],
                "documents": [],
            }
            speech_index += 1


def get_parsed_proceedings_filename(source: Path, output_dir: Path) -> Path:
    """`<sessionid>-data.xml` → `<sessionid>-proceedings.json` so the merger picks it up."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = source.stem
    if stem.endswith("-data"):
        stem = stem[: -len("-data")]
    return output_dir / f"{stem}-proceedings.json"


def parse_proceedings(source, output, uri, args) -> dict:
    """Parse one ParlaMint XML file and write `<sessionid>-proceedings.json`."""
    speeches = list(parse_transcript(source, uri, args))
    if not speeches:
        logger.warning(f"No speeches parsed from {source}")
        return {}
    first = speeches[0]
    period = first["electoralPeriod"]["number"]
    meeting = first["session"]["number"]
    session_id = f"{period}{str(meeting).zfill(3)}"

    data = {
        "meta": {
            "session": session_id,
            "processing": {
                "parse_proceedings": datetime.now().isoformat("T", "seconds"),
            },
            "dateStart": first["session"]["dateStart"],
            "dateEnd": first["session"]["dateEnd"],
        },
        "data": speeches,
    }
    if output == "-":
        json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
    elif output:
        output_file = get_parsed_proceedings_filename(Path(source), Path(output))
        logger.debug(f"Saving to {output_file}")
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    return data


def parse_parlamint_directory(directory: Path, args) -> None:
    """Update parsed JSON for all `<sessionid>-data.xml` files in directory.

    Skips XML files that do not look like ParlaMint sessions
    (so Bundestag-native `<sessionid>-proceedings.xml` are ignored).
    """
    directory = Path(directory)
    for source in sorted(directory.glob("*-data.xml")):
        output_file = get_parsed_proceedings_filename(source, directory)
        # mtime-driven, matching parse_proceedings_directory: re-parse only
        # when the source XML is genuinely newer than the parsed output. The
        # global --force flag is intentionally not consulted here — parsing is
        # an implicit always-run step, not a command-selected stage.
        if output_file.exists() and output_file.stat().st_mtime >= source.stat().st_mtime:
            continue
        # Cheap sanity check: is this actually a ParlaMint session file?
        try:
            with open(source, "rb") as f:
                head = f.read(2048)
        except OSError:
            continue
        if b"parla.sitting" not in head:
            logger.debug(f"Skipping non-ParlaMint XML {source.name}")
            continue
        logger.info(f"Parsing ParlaMint {source.name}")
        parse_proceedings(source, directory, str(source), args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse ParlaMint-DE_beta TEI XML files.")
    parser.add_argument("source", type=str, nargs="?", help="Source XML file or directory")
    parser.add_argument("--uri", type=str, help="Origin URI")
    parser.add_argument("--output", type=str, default="-",
                        help="Output directory (or - for stdout)")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    if args.source is None:
        parser.print_help()
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    source = Path(args.source)
    if source.is_dir():
        parse_parlamint_directory(source, args)
    else:
        parse_proceedings(args.source, args.output, args.uri, args)
