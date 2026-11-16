"""Group per-utterance proceedings turns into spine-granularity "redes".

The video spine and the proceedings text are at different granularities. A spine
clip may bundle a chair intro plus one speaker (DE-BY), or a whole back-and-forth
incl. Zwischenfragen (DE-BW). To align text to the spine we first group the
per-``<u>`` turns from :func:`optv.shared.pdf2tei.tei2json.tei_to_turns` into
redes that match the clip granularity, then the merger does the surname NW join.

The grouping rule is per-parliament:

- ``chain=False`` — close the rede at every new non-chair speaker (one clip =
  chair + one speaker turn; DE-BY).
- ``chain=True``, ``K`` — keep a speaker's consecutive turns and bounded
  Zwischenfragen (the main resumes within ``K`` non-chair turns) in one rede
  (one clip = the whole exchange; DE-BW).

Chair turns always attach their text to the current rede. Every absorbed speech
body keeps who said it (``speakerName``/``speakerKey``, plus ``chairContext`` for
a chair turn), and each rede lists its presiding officers in ``chairs``
(``{name, firstname, lastname, context}``), so the spine join can attribute
chair text to the chair and add them to ``people[]``.
"""
from __future__ import annotations


def _new(turn: dict) -> dict:
    return {"main": None, "personId": "", "sentences": [], "bodies": [], "chairs": [],
            "agendaTitle": turn.get("agendaTitle", ""),
            "originTextID": turn.get("originTextID", ""), "speaker": turn.get("speaker", "")}


def _chair(u: dict) -> dict | None:
    """The presiding officer of a chair turn as ``{name, firstname, lastname,
    context}``, or None for a non-chair turn or one without a name."""
    name = (u.get("name") or "").strip()
    if not u.get("isChair") or not name:
        return None
    surname = (u.get("surname") or "").strip()
    first = name[:-len(surname)].strip() if surname and name.endswith(surname) else ""
    chair = {"name": name, "context": u.get("context") or "president"}
    if first:
        chair["firstname"] = first
    if surname:
        chair["lastname"] = surname
    return chair


def _absorb(cur: dict, u: dict) -> None:
    """Append a turn's speech sentences and full typed bodies into the rede,
    tagging each speech body with the turn's speaker."""
    cur["sentences"] += u.get("sentences", [])
    chair = _chair(u)
    for b in u.get("bodies", []):
        if b.get("type") == "comment":
            cur["bodies"].append(b)
            continue
        tagged = {**b, "speakerName": (u.get("name") or "").strip(),
                  "speakerKey": u.get("matchKey") or ""}
        if chair:
            tagged["chairContext"] = chair["context"]
        cur["bodies"].append(tagged)
    if chair and all(c["name"] != chair["name"] for c in cur["chairs"]):
        cur["chairs"].append(chair)


def merge_turns(turns: list[dict], *, chain: bool, K: int = 2) -> list[dict]:
    """Group ``tei_to_turns`` output into redes; return merged turn dicts with a
    single ``matchKey`` and the concatenated sentence list per rede."""
    redes: list[dict] = []
    cur: dict | None = None
    n = len(turns)
    for i, u in enumerate(turns):
        if u.get("isChair"):
            if cur is None:
                cur = _new(u)
            _absorb(cur, u)
            continue
        spk = u.get("matchKey", "")
        if cur is None or cur["main"] is None:
            if cur is None:
                cur = _new(u)
            cur["main"] = spk
            cur["personId"] = u.get("personId", "")
            _absorb(cur, u)
            cur["agendaTitle"] = cur["agendaTitle"] or u.get("agendaTitle", "")
            cur["originTextID"] = cur["originTextID"] or u.get("originTextID", "")
            cur["speaker"] = u.get("speaker", "")
        elif spk == cur["main"] and chain:
            _absorb(cur, u)
        elif not chain:
            redes.append(cur)
            cur = _new(u)
            cur["main"] = spk
            cur["personId"] = u.get("personId", "")
            _absorb(cur, u)
            cur["speaker"] = u.get("speaker", "")
        else:
            # Absorb as a Zwischenfrage only if the current main resumes within
            # K non-chair turns; otherwise this speaker opens a new rede.
            cnt = resumes = 0
            for j in range(i + 1, n):
                if turns[j].get("isChair"):
                    continue
                cnt += 1
                if turns[j].get("matchKey", "") == cur["main"]:
                    resumes = 1
                    break
                if cnt >= K:
                    break
            if resumes:
                _absorb(cur, u)
            else:
                redes.append(cur)
                cur = _new(u)
                cur["main"] = spk
                cur["personId"] = u.get("personId", "")
                _absorb(cur, u)
                cur["speaker"] = u.get("speaker", "")
    if cur and cur["main"]:
        redes.append(cur)
    return [{
        "index": i + 1,
        "matchKey": r["main"],
        "personId": r["personId"],
        "speaker": r["speaker"],
        "agendaTitle": r["agendaTitle"],
        "originTextID": r["originTextID"],
        "sentences": r["sentences"],
        "bodies": r["bodies"],
        "chairs": r["chairs"],
    } for i, r in enumerate(redes)]
