"""Cross-field and cross-item semantic rules for OPTV Stage 2.

Each rule emits a finding with severity "error" or "warning" and a stable
rule id. See ../schema/README.md for the rule list and the rationale.
"""

import re
from datetime import datetime

KNOWN_PARLIAMENT_CODES = {"DE"}  # fallback when optv.parliaments isn't importable


def _known_parliament_codes() -> set[str]:
    try:
        from optv.parliaments import list_parliaments
        return {p.upper() for p in list_parliaments()}
    except Exception:
        return KNOWN_PARLIAMENT_CODES

# Presidents/VPs speak in their role, not as a faction member — faction is not
# expected. Restrict the "missing faction" warning to these speaker contexts.
FACTION_EXPECTED_CONTEXTS = {"main-speaker", "speaker", "main-proceeding-speaker"}

WID_RE = re.compile(r"^Q\d+$")
URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _parse_iso(s):
    """Best-effort ISO 8601 parse. Returns None on failure."""
    if not isinstance(s, str) or not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _warn(rule, path, message):
    return {"severity": "warning", "rule": rule, "path": path, "message": message}


def _err(rule, path, message):
    return {"severity": "error", "rule": rule, "path": path, "message": message}


def validate_semantic(doc):
    findings = []
    findings.extend(_rule_parliament_code(doc))
    findings.extend(_rule_period_and_session_numbers(doc))
    findings.extend(_rule_date_ordering(doc))
    findings.extend(_rule_speech_index(doc))
    findings.extend(_rule_people(doc))
    findings.extend(_rule_deprecated_agenda_speech_index(doc))
    findings.extend(_rule_text_contents_source_uri(doc))
    findings.extend(_rule_media_source_page_unique(doc))
    findings.extend(_rule_sentence_times(doc))
    findings.extend(_rule_speech_origin_text_id_deprecated(doc))
    findings.extend(_rule_original_language_consistency(doc))
    return findings


def _rule_speech_origin_text_id_deprecated(doc):
    """Speech-level ``originTextID`` is deprecated in favour of ``originID``
    (the textContents-level field of the same name is fine). Warn so the
    parser gets migrated; old/not-yet-re-emitted data still validates."""
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        if "originTextID" in sp:
            out.append(_warn(
                "semantic.speech.originTextID_deprecated",
                f"data/{i}/originTextID",
                "Speech-level 'originTextID' is deprecated. The merger normalizer "
                "promotes it to 'originID' (a joint speech id, kept only when "
                "distinct from media.originMediaID / textContents[].originTextID). "
                "The textContents-level 'originTextID' is unaffected.",
            ))
    return out


def _rule_original_language_consistency(doc):
    """``originalLanguage`` selects the original text out of a multi-language
    ``textContents[]`` array, so it must match the ``language`` of one entry and
    share its code standard. Warning-only (does not block publish)."""
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        lang = sp.get("originalLanguage")
        texts = sp.get("textContents") or []
        if not lang or not texts:
            continue
        langs = {t.get("language") for t in texts if t.get("language")}
        if langs and lang not in langs:
            out.append(_warn(
                "semantic.speech.originalLanguage_mismatch",
                f"data/{i}/originalLanguage",
                f"originalLanguage {lang!r} matches no textContents[].language "
                f"({sorted(langs)!r}); it must select the original text entry "
                f"and use the same code standard.",
            ))
    return out


def _rule_parliament_code(doc):
    out = []
    known = _known_parliament_codes()
    for i, sp in enumerate(doc.get("data") or []):
        code = sp.get("parliament")
        if code and code not in known:
            out.append(_warn(
                "semantic.parliament.unknown",
                f"data/{i}/parliament",
                f"Unknown parliament code {code!r}; add to KNOWN_PARLIAMENT_CODES if legitimate.",
            ))
    return out


def _rule_period_and_session_numbers(doc):
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        ep = (sp.get("electoralPeriod") or {}).get("number")
        if isinstance(ep, int) and ep < 1:
            out.append(_err(
                "semantic.electoralPeriod.nonpositive",
                f"data/{i}/electoralPeriod/number",
                f"electoralPeriod.number must be >= 1, got {ep}.",
            ))
        sn = (sp.get("session") or {}).get("number")
        if isinstance(sn, int) and sn < 1:
            out.append(_err(
                "semantic.session.nonpositive",
                f"data/{i}/session/number",
                f"session.number must be >= 1, got {sn}.",
            ))
    return out


def _rule_date_ordering(doc):
    out = []
    meta = doc.get("meta") or {}
    m_start = _parse_iso(meta.get("dateStart"))
    m_end = _parse_iso(meta.get("dateEnd"))
    if m_start and m_end and m_end < m_start:
        out.append(_err(
            "semantic.date.order",
            "meta",
            f"meta.dateEnd ({meta.get('dateEnd')}) is before meta.dateStart ({meta.get('dateStart')}).",
        ))
    for i, sp in enumerate(doc.get("data") or []):
        s_start = _parse_iso(sp.get("dateStart"))
        s_end = _parse_iso(sp.get("dateEnd"))
        if s_start and s_end and s_end < s_start:
            out.append(_err(
                "semantic.date.order",
                f"data/{i}",
                f"dateEnd ({sp.get('dateEnd')}) is before dateStart ({sp.get('dateStart')}).",
            ))
    return out


def _rule_speech_index(doc):
    out = []
    indices = []
    for i, sp in enumerate(doc.get("data") or []):
        idx = sp.get("speechIndex")
        if idx is None:
            continue
        indices.append((i, idx))
    if not indices:
        return out
    seen = {}
    for i, idx in indices:
        if idx in seen:
            out.append(_err(
                "semantic.speechIndex.duplicate",
                f"data/{i}/speechIndex",
                f"Duplicate speechIndex {idx} (also at data[{seen[idx]}]).",
            ))
        else:
            seen[idx] = i
    values = sorted(v for _, v in indices)
    if values[0] != 1:
        out.append(_warn(
            "semantic.speechIndex.not1indexed",
            "data",
            f"speechIndex should start at 1; first value is {values[0]}.",
        ))
    expected = list(range(values[0], values[0] + len(values)))
    if values != expected:
        gaps = [e for e in expected if e not in set(values)]
        out.append(_warn(
            "semantic.speechIndex.gaps",
            "data",
            f"speechIndex is not sequential; missing: {gaps[:10]}{'...' if len(gaps) > 10 else ''}.",
        ))
    return out


def _rule_people(doc):
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        people = sp.get("people")
        if not people:
            out.append(_warn(
                "semantic.people.missing",
                f"data/{i}/people",
                "Speech has no people array (period-17 legacy allows this).",
            ))
            continue
        for j, person in enumerate(people):
            base = f"data/{i}/people/{j}"
            ctx = person.get("context")
            if ctx == "Unknown":
                out.append(_warn(
                    "semantic.people.context.unknown",
                    f"{base}/context",
                    "Speaker context is 'Unknown'; investigate upstream parser.",
                ))
            wid = person.get("wid")
            if wid is None or wid == "":
                # Always warn — every person should resolve to a Wikidata ID.
                out.append(_warn(
                    "semantic.people.wid.missing",
                    f"{base}/wid",
                    f"Person {person.get('label', '?')!r} (context={ctx!r}) has no Wikidata ID.",
                ))
            elif not WID_RE.match(wid):
                out.append(_err(
                    "semantic.people.wid.invalid",
                    f"{base}/wid",
                    f"wid {wid!r} does not match ^Q\\d+$.",
                ))
            fac = person.get("faction")
            if isinstance(fac, str):
                out.append(_err(
                    "semantic.people.faction.type.invalid",
                    f"{base}/faction",
                    f"faction must be an object, got string {fac!r} (pre-NEL parser output leaked through).",
                ))
            elif not fac or not (fac.get("wid") or fac.get("label")):
                if ctx in FACTION_EXPECTED_CONTEXTS:
                    out.append(_warn(
                        "semantic.people.faction.missing",
                        f"{base}/faction",
                        f"Person {person.get('label', '?')!r} (context={ctx!r}) has no faction.",
                    ))
            else:
                fwid = fac.get("wid")
                if fwid and not WID_RE.match(fwid):
                    out.append(_err(
                        "semantic.people.faction.wid.invalid",
                        f"{base}/faction/wid",
                        f"faction.wid {fwid!r} does not match ^Q\\d+$.",
                    ))
    return out


def _rule_deprecated_agenda_speech_index(doc):
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        agenda = sp.get("agendaItem") or {}
        if "speechIndex" in agenda:
            out.append(_warn(
                "semantic.agendaItem.speechIndex.deprecated",
                f"data/{i}/agendaItem/speechIndex",
                "agendaItem.speechIndex is deprecated; use top-level data[].speechIndex instead.",
            ))
    return out


def _rule_text_contents_source_uri(doc):
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        for k, tc in enumerate(sp.get("textContents") or []):
            uri = tc.get("sourceURI")
            if uri and not URL_RE.match(uri):
                out.append(_warn(
                    "semantic.textContents.sourceURI.notUrl",
                    f"data/{i}/textContents/{k}/sourceURI",
                    f"sourceURI {uri!r} is not an http(s) URL (likely a local path leaked into output).",
                ))
    return out


def _rule_media_source_page_unique(doc):
    """media.sourcePage must be unique per speech within a session.

    The platform's media.php import keys speech identity on ``sourcePage`` to
    detect already-imported speeches, so two speeches sharing a sourcePage
    collapse into one (silent data loss). Parliaments with one video per
    session/debate/part must therefore make sourcePage distinct per speech —
    e.g. by appending the per-speech start offset (SE: ``?pos=``) or a
    per-speech id (DE-SH). Empty sourcePage is left to the schema's
    minLength check.
    """
    out = []
    seen = {}  # sourcePage -> first index
    for i, sp in enumerate(doc.get("data") or []):
        page = (sp.get("media") or {}).get("sourcePage")
        if not page:
            continue
        if page in seen:
            out.append(_warn(
                "semantic.media.sourcePage.duplicate",
                f"data/{i}/media/sourcePage",
                f"sourcePage {page!r} duplicates data[{seen[page]}]; the platform "
                "keys speech identity on sourcePage, so duplicates collapse "
                "distinct speeches at import. Append a per-speech token (start "
                "offset or speech id).",
            ))
        else:
            seen[page] = i
    return out


def _rule_sentence_times(doc):
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        for k, tc in enumerate(sp.get("textContents") or []):
            for m, tb in enumerate(tc.get("textBody") or []):
                for n, sent in enumerate(tb.get("sentences") or []):
                    ts = sent.get("timeStart")
                    te = sent.get("timeEnd")
                    if ts is None or te is None:
                        continue
                    try:
                        ts_f, te_f = float(ts), float(te)
                    except (TypeError, ValueError):
                        continue
                    if te_f < ts_f:
                        out.append(_err(
                            "semantic.sentence.time.order",
                            f"data/{i}/textContents/{k}/textBody/{m}/sentences/{n}",
                            f"sentence timeEnd ({te}) < timeStart ({ts}).",
                        ))
    return out
