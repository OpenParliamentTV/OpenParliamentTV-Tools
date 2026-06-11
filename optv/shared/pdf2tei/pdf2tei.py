"""Turn extracted blocks into ParlaMint-style TEI + listPerson/listOrg.

Reads a block list (see :mod:`optv.shared.pdf2tei.common`) and a
:class:`~optv.shared.pdf2tei.config.ParliamentConfig`, and emits the TEI dialect
the ParlaMint reader (:mod:`optv.shared.pdf2tei.tei2json`) consumes::

    <meeting ana="#parla.term|#parla.sitting">,
    <div type="debateSection"> with <head>,
    <note type="speaker"> before each <u who ana="#chair|#regular">,
    <seg> paragraphs, <incident><desc> interjections.

The builder is parliament-agnostic: all variance is in ``cfg`` (regexes/layout)
and the German-language constants imported from :mod:`optv.shared.lang.de`.
"""
from __future__ import annotations

import re

from lxml import etree

from .common import read_blocks
from ..lang.de import (
    MONTHS, DATE_RE, INCIDENT_KW, TITLE_STOP, HEADING_NOISE,
    pdf_chair_context, faction_slug,
)

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"

_TITLE_PREFIX = re.compile(r"^(Dr\.|Prof\.|Dr\.\s*h\.\s*c\.|h\.\s*c\.)\s+", re.IGNORECASE)
# XML 1.0-compatible text (allow TAB/LF/CR + printable Unicode).
_XML_CTRL = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _sub(parent, tag, text=None, **attrs):
    el = etree.SubElement(parent, f"{{{TEI_NS}}}{tag}")
    for k, v in attrs.items():
        if k == "xmlid":
            el.set(f"{{{XML_NS}}}id", v)
        elif v is not None:
            el.set(k, v)
    if text is not None:
        el.text = _XML_CTRL.sub(" ", text)
    return el


def strip_title(name: str) -> str:
    n = name.strip()
    while True:
        m = _TITLE_PREFIX.match(n)
        if not m:
            break
        n = n[m.end():].strip()
    return n


def person_id(name: str) -> str:
    slug = re.sub(r"[^0-9A-Za-zÄÖÜäöüß]+", "_", strip_title(name)).strip("_")
    return "p_" + (slug or "unknown")


def split_name(name: str) -> tuple[str, str]:
    parts = strip_title(name).split()
    if not parts:
        return "", ""
    return parts[0], " ".join(parts[1:])


def find_date(blocks: list[dict]) -> str:
    for b in blocks[:60]:
        m = DATE_RE.search(b["text"])
        if m:
            return f"{int(m.group(3)):04d}-{MONTHS[m.group(2)]:02d}-{int(m.group(1)):02d}"
    return "1900-01-01"


def looks_like_top_heading(text: str) -> bool:
    """Title-ish docling section_header: a real agenda item, not a stray header."""
    t = text.strip()
    if not (10 <= len(t) <= 160):
        return False
    if t.endswith(":") or HEADING_NOISE.match(t) or TITLE_STOP.match(t):
        return False
    return t[:1].isupper()


def is_continuation_marker(text: str, known_names: set[str]) -> bool:
    """SH prints the current speaker's name in parens at the top of each column
    ('(Christopher Vogt)'). Those are not interjections."""
    if not (text.startswith("(") and text.endswith(")")):
        return False
    inner = text[1:-1].strip()
    inner = re.sub(r"^(Minister(?:in)?|Präsident(?:in)?|Vizepräsident(?:in)?)\s+", "", inner)
    return (inner in known_names) and not INCIDENT_KW.search(text)


class Speaker:
    __slots__ = ("name", "faction", "role", "is_chair", "is_gov", "context")

    def __init__(self, name, faction=None, role=None, is_chair=False, is_gov=False, context=None):
        self.name = name
        self.faction = faction
        self.role = role
        self.is_chair = is_chair
        self.is_gov = is_gov
        self.context = context


def classify_block(text: str, cfg) -> tuple[str, Speaker | None, str | None]:
    """Return (kind, speaker, rest). kind in {chair, gov, mp, top, incident,
    body}. `rest` is the speech text after an inline label (cfg.speaker_inline),
    else None."""
    m = cfg.chair_speaker.match(text)
    if m:
        spk = Speaker(m.group("name").strip(), role=m.group("role"),
                      is_chair=True, context=pdf_chair_context(m.group("role")))
        return "chair", spk, (m.groupdict().get("rest") if cfg.speaker_inline else None)
    m = cfg.gov_speaker.match(text)
    if m:
        spk = Speaker(m.group("name").strip(), role=m.group("role").strip(), is_gov=True)
        return "gov", spk, (m.groupdict().get("rest") if cfg.speaker_inline else None)
    m = cfg.mp_speaker.match(text)
    if m:
        spk = Speaker(m.group("name").strip(), faction=m.group("faction").strip())
        return "mp", spk, (m.groupdict().get("rest") if cfg.speaker_inline else None)
    if cfg.top_announce and cfg.top_announce.search(text):
        return "top", None, None
    if text[:1] in getattr(cfg, "incident_open", "("):
        return "incident", None, None
    return "body", None, None


def build_tei(sid: str, cfg, blocks: list[dict],
              agenda_mode: str = "regex", pdf_path=None):
    """Build the TEI tree (+ person/faction registries) for one session.

    ``cfg`` is a :class:`~optv.shared.pdf2tei.config.ParliamentConfig`;
    ``cfg.parliament_id`` names the parliament. ``sid`` is the
    ``{period:02d}{sitzung:03d}`` session id.
    """
    parliament = cfg.parliament_id
    period = int(sid[:2])
    session = int(sid[2:])
    date = find_date(blocks)

    persons: dict[str, Speaker] = {}      # id -> Speaker (first seen)
    factions: dict[str, str] = {}         # slug -> display label
    has_gov = False

    # --- pass 1: walk blocks, group into sections / turns -------------------
    sections: list[dict] = [{"head": None, "turns": []}]
    cur_turn = None
    pending_top = False     # saw a chair TOP-announce; capture following title blocks
    top_title_parts: list[str] = []
    known_names: set[str] = set()

    def flush_top():
        nonlocal pending_top, top_title_parts
        if pending_top and top_title_parts:
            title = " ".join(top_title_parts).strip()
            sections.append({"head": title, "turns": []})
        pending_top = False
        top_title_parts = []

    for b in blocks:
        text = b["text"]
        kind, spk, rest = classify_block(text, cfg)

        # Heading-based TOP detection (docling only): a section_header reaching
        # here is a title. PyMuPDF has no such signal and relies on chair
        # "Ich rufe ... auf" announcements or the TOC.
        if kind == "body" and b.get("label") == "section_header" \
                and looks_like_top_heading(text):
            flush_top()
            sections.append({"head": text.strip(), "turns": []})
            cur_turn = None
            continue

        if kind in ("chair", "gov", "mp"):
            flush_top()
            pid = person_id(spk.name)
            if pid not in persons:
                persons[pid] = spk
            if spk.faction:
                slug = faction_slug(spk.faction)
                factions.setdefault(slug, spk.faction)
            if spk.is_gov:
                has_gov = True
            known_names.add(strip_title(spk.name))
            # Inline label ("Name (CSU): speech…"): the <note>/label is the part
            # before the colon; `rest` is the first speech paragraph.
            if cfg.speaker_inline and rest is not None:
                label = text[: len(text) - len(rest)].rstrip().rstrip(":").strip()
            else:
                label = text.rstrip(":").strip()
            cur_turn = {"spk": spk, "id": pid, "label": label,
                        "ana": "#chair" if spk.is_chair else "#regular",
                        "segs": [], "incidents": [], "page": b.get("page", 0)}
            if cfg.speaker_inline and rest and rest.strip():
                cur_turn["segs"].append(rest.strip())
            sections[-1]["turns"].append(cur_turn)
            continue

        if kind == "top":
            pending_top = True
            top_title_parts = []
            cur_turn = None
            continue

        if pending_top:
            # Collect the agenda title that follows the announcement, stopping at
            # the first procedural-metadata block (Drucksache, dazu, Antrag, ...).
            if kind == "incident":
                continue
            if TITLE_STOP.match(text):
                pending_top = "done"  # keep section pending-flush but stop capturing
                continue
            if pending_top != "done" and len(" ".join(top_title_parts)) < 160:
                top_title_parts.append(text)
            continue

        if kind == "incident":
            if is_continuation_marker(text, known_names):
                continue
            if cur_turn is not None:
                cur_turn["incidents"].append((len(cur_turn["segs"]), text))
            continue

        # body paragraph
        if cur_turn is not None:
            cur_turn["segs"].append(text)

    flush_top()
    # Drop empty leading section.
    sections = [s for s in sections if s["turns"]]

    # --- agenda mode: TOC page-anchoring ------------------------------------
    if agenda_mode == "toc" and pdf_path is not None:
        from . import toc as toc_mod
        toc_entries, offset = toc_mod.parse_toc(pdf_path, cfg)
        flat = [t for sec in sections for t in sec["turns"]]
        sections = []
        last_title = None
        for t in flat:
            title = toc_mod.title_for_printed_page(toc_entries, t["page"] + offset)
            if not sections or title != last_title:
                sections.append({"head": title, "turns": []})
                last_title = title
            sections[-1]["turns"].append(t)

    # --- build TEI tree -----------------------------------------------------
    root = etree.Element(f"{{{TEI_NS}}}TEI", nsmap={None: TEI_NS, "xml": XML_NS})
    root.set(f"{{{XML_NS}}}id", f"PDF2TEI-{parliament}_{date}-{period:02d}-{session:03d}")
    hdr = _sub(root, "teiHeader")
    fdesc = _sub(hdr, "fileDesc")
    tstmt = _sub(fdesc, "titleStmt")
    _sub(tstmt, "title", f"{parliament} {period}/{session}")
    sdesc = _sub(fdesc, "sourceDesc")
    bibl = _sub(sdesc, "bibl")
    _sub(bibl, "idno", f"pdf2tei:{parliament}:{sid}")
    pdesc = _sub(hdr, "profileDesc")
    setting = _sub(pdesc, "settingDesc")
    s_setting = _sub(setting, "setting")
    _sub(s_setting, "date", date, ana="#parla.sitting", when=date)
    _sub(s_setting, "meeting", f"Wahlperiode {period}", ana="#parla.term", n=str(period))
    _sub(s_setting, "meeting", f"Sitzung {session}", ana="#parla.sitting", n=str(session))

    text_el = _sub(root, "text")
    body = _sub(text_el, "body")
    div = _sub(body, "div", type="debateSection")
    speech_n = 0
    for sec in sections:
        # New debateSection per detected agenda head (first one reuses div).
        if sec["head"] is not None and len(div) > 0:
            div = _sub(body, "div", type="debateSection")
        if sec["head"]:
            _sub(div, "head", sec["head"])
        for turn in sec["turns"]:
            _sub(div, "note", turn["label"], type="speaker")
            speech_n += 1
            u = _sub(div, "u", who=f"#{turn['id']}", ana=turn["ana"],
                     xmlid=f"u{sid}_{speech_n}")
            inc_at = dict()
            for pos, txt in turn["incidents"]:
                inc_at.setdefault(pos, []).append(txt)
            for i, seg in enumerate(turn["segs"]):
                for txt in inc_at.get(i, []):
                    _emit_incident(u, txt)
                _sub(u, "seg", seg)
            for txt in inc_at.get(len(turn["segs"]), []):
                _emit_incident(u, txt)
    return root, persons, factions, has_gov, period, session, date


def _emit_incident(u, text):
    inc = _sub(u, "incident")
    _sub(inc, "desc", text.strip("()[]{}").strip())


def build_registries(persons, factions, has_gov):
    org_root = etree.Element(f"{{{TEI_NS}}}listOrg", nsmap={None: TEI_NS, "xml": XML_NS})
    lo = _sub(org_root, "listOrg")
    for slug, label in sorted(factions.items()):
        org = _sub(lo, "org", xmlid=f"parliamentaryGroup.{slug}")
        _sub(org, "orgName", label, full="yes")
        _sub(org, "orgName", slug, full="abb")
    if has_gov:
        gov = _sub(lo, "org", xmlid="government.DE")
        _sub(gov, "orgName", "Landesregierung", full="yes")

    person_root = etree.Element(f"{{{TEI_NS}}}listPerson", nsmap={None: TEI_NS, "xml": XML_NS})
    lp = _sub(person_root, "listPerson")
    for pid, spk in persons.items():
        p = _sub(lp, "person", xmlid=pid)
        pn = _sub(p, "persName")
        fn, ln = split_name(spk.name)
        if fn:
            _sub(pn, "forename", fn)
        if ln:
            _sub(pn, "surname", ln)
        if spk.faction:
            _sub(p, "affiliation", ref=f"#parliamentaryGroup.{faction_slug(spk.faction)}", role="member")
        if spk.is_gov:
            _sub(p, "affiliation", ref="#government.DE", role="minister")
    return person_root, org_root


def write_xml(path, root):
    etree.ElementTree(root).write(str(path), encoding="utf-8",
                                  xml_declaration=True, pretty_print=True)
