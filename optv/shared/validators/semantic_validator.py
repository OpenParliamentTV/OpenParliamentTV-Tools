"""Cross-field and cross-item semantic rules for OPTV Stage 2.

Rules are derived from _planning/stage2-discrepancy.md §Semantic-validator rules.
Each rule emits findings with severity "error" or "warning" and a stable rule id.
"""

import re
from datetime import datetime

KNOWN_PARLIAMENT_CODES = {"DE"}  # extend as more parliaments are onboarded

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
    findings.extend(_rule_sentence_times(doc))
    return findings


def _rule_parliament_code(doc):
    out = []
    for i, sp in enumerate(doc.get("data") or []):
        code = sp.get("parliament")
        if code and code not in KNOWN_PARLIAMENT_CODES:
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
