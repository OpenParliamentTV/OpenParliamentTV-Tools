"""PT open data Intervenções: link discovery, per-reunião split, file writes.

No network: the folder pages are synthetic snippets and the legislatura file is
pre-seeded in the cache, which ``download_opendata`` reuses while it is fresh.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json

from optv.parliaments.PT.common import Config
from optv.parliaments.PT.scraper.fetch_opendata import (
    compact_record, download_opendata, find_file_url, find_folder_url, roman,
    split_by_session,
)


def _t(name: str) -> str:
    return binascii.hexlify(base64.b64encode(name.encode())).decode()


def test_roman():
    assert [roman(n) for n in (1, 4, 9, 14, 16, 17, 40)] == \
        ["I", "IV", "IX", "XIV", "XVI", "XVII", "XL"]


def test_find_links():
    index = (f'<a href="/Cidadania/Paginas/DAIntervencoes.aspx?t={_t("XVI Legislatura")}'
             f'&amp;Path=abc">XVI</a>'
             f'<a href="/Cidadania/Paginas/DAIntervencoes.aspx?t={_t("XVII Legislatura")}'
             f'&amp;Path=def%2b">XVII</a>')
    folder = find_folder_url(index, 17)
    assert folder.startswith("https://www.parlamento.pt/Cidadania/Paginas/DAIntervencoes.aspx?t=")
    assert folder.endswith("&Path=def%2b")
    assert find_folder_url(index, 15) is None

    page = ('<a href="https://app.parlamento.pt/webutils/docs/doc.xml?path=x&amp;'
            'fich=IntervencoesXVII.xml&amp;Inline=true">xml</a>'
            '<a href="https://app.parlamento.pt/webutils/docs/doc.txt?path=y&amp;'
            'fich=IntervencoesXVII_json.txt&amp;Inline=true">json</a>')
    assert find_file_url(page, 17) == ("https://app.parlamento.pt/webutils/docs/doc.txt?"
                                       "path=y&fich=IntervencoesXVII_json.txt&Inline=true")


def _record(av_url, rid="1", pag="12-13", cadastro="4197"):
    return {
        "Id": rid, "TipoIntervencao": "Intervenção", "DataReuniaoPlenaria": "2026-09-16",
        "Deputados": {"GP": "PCP", "idCadastro": cadastro, "nome": "Paula Santos"},
        "MembrosGoverno": {"cargo": None, "governo": None, "nome": None},
        "DadosAudiovisual": [{"url": av_url, "assunto": "Apresenta o Projeto de Lei."}],
        "Publicacao": [{"pag": [pag], "URLDiario": "https://debates.parlamento.pt/x"}],
        "IdDebate": "82605", "Debate": "Tribunais Administrativos", "Sumario": "Altera.",
    }


def test_compact_record_and_split():
    recs = [
        _record("https://av.parlamento.pt/videos/Plenary/17/2/1/16"),
        _record("https://av.parlamento.pt/videos/Plenary/17/2/1/3", rid="2", pag="3"),
        _record("https://av.parlamento.pt/videos/Plenary/16/1/5/2", rid="3"),
        {"Id": "4", "DadosAudiovisual": None},
    ]
    by = split_by_session(recs, 17)
    assert list(by) == ["17-2-001"]
    assert set(by["17-2-001"]) == {"16", "3"}
    rec = by["17-2-001"]["16"]
    assert rec["personID"] == "4197" and rec["party"] == "PCP"
    assert rec["pages"] == [12, 13]
    assert rec["debateID"] == "82605"
    assert rec["subject"] == "Apresenta o Projeto de Lei."
    assert by["17-2-001"]["3"]["pages"] == [3, 3]
    assert "governmentRole" not in rec        # empty fields are dropped
    assert compact_record({}) == {}


def test_download_writes_known_sessions_only_on_change(tmp_path):
    config = Config(tmp_path)
    config.dir("media", create=True)
    config.raw_av("17-2-001").write_text("{}")
    cache = config.dir("opendata_cache", create=True) / "IntervencoesXVII.json"
    cache.write_text(json.dumps([
        _record("https://av.parlamento.pt/videos/Plenary/17/2/1/16"),
        _record("https://av.parlamento.pt/videos/Plenary/17/2/2/4", rid="9"),
    ]))
    args = argparse.Namespace(period=17, pt_session=[], limit_session="", force=False,
                              retry_count=1, retry_delay_max=1)
    download_opendata(config, args)
    out = config.raw_od("17-2-001")
    doc = json.loads(out.read_text())
    assert list(doc["interventions"]) == ["16"]
    # 17-2-002 has no av JSON here: no file for it
    assert not config.raw_od("17-2-002").exists()
    # unchanged records leave the file (and its mtime) alone
    mtime = out.stat().st_mtime_ns
    download_opendata(config, args)
    assert out.stat().st_mtime_ns == mtime
