"""Group per-utterance proceedings turns into spine-granularity "redes".

The video spine and the proceedings text are at different granularities. A spine
clip may bundle a chair intro plus one speaker (DE-BY), or a whole back-and-forth
incl. Zwischenfragen (DE-BW). To align text to the spine we first group the
per-``<u>`` turns from :func:`optv.shared.pdf2tei.tei2json.tei_to_turns` into
redes that match the clip granularity, then the merger does the surname NW join.

The grouping rule (derived from the Whisper diagnostic, see
``_planning/pdf2tei/join_merge_proto.py``) is per-parliament:

- ``chain=False`` — close the rede at every new non-chair speaker (one clip =
  chair + one speaker turn; DE-BY).
- ``chain=True``, ``K`` — keep a speaker's consecutive turns and bounded
  Zwischenfragen (the main resumes within ``K`` non-chair turns) in one rede
  (one clip = the whole exchange; DE-BW).

Chair turns always attach their text to the current rede.
"""
from __future__ import annotations


def _new(turn: dict) -> dict:
    return {"main": None, "sentences": [], "agendaTitle": turn.get("agendaTitle", ""),
            "originTextID": turn.get("originTextID", ""), "speaker": turn.get("speaker", "")}


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
            cur["sentences"] += u.get("sentences", [])
            continue
        spk = u.get("matchKey", "")
        if cur is None or cur["main"] is None:
            if cur is None:
                cur = _new(u)
            cur["main"] = spk
            cur["sentences"] += u.get("sentences", [])
            cur["agendaTitle"] = cur["agendaTitle"] or u.get("agendaTitle", "")
            cur["originTextID"] = cur["originTextID"] or u.get("originTextID", "")
            cur["speaker"] = u.get("speaker", "")
        elif spk == cur["main"] and chain:
            cur["sentences"] += u.get("sentences", [])
        elif not chain:
            redes.append(cur)
            cur = _new(u)
            cur["main"] = spk
            cur["sentences"] += u.get("sentences", [])
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
                cur["sentences"] += u.get("sentences", [])
            else:
                redes.append(cur)
                cur = _new(u)
                cur["main"] = spk
                cur["sentences"] += u.get("sentences", [])
                cur["speaker"] = u.get("speaker", "")
    if cur and cur["main"]:
        redes.append(cur)
    return [{
        "index": i + 1,
        "matchKey": r["main"],
        "speaker": r["speaker"],
        "agendaTitle": r["agendaTitle"],
        "originTextID": r["originTextID"],
        "sentences": r["sentences"],
    } for i, r in enumerate(redes)]
