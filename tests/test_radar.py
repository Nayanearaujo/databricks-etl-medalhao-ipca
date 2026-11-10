from decimal import Decimal
from io import BytesIO

import pytest

from etl import radar


def linha(mes="202501", taxa="0.45"):
    return {"D1C": "1", "D3C": mes, "D4C": "2", "V": taxa}


def test_consulta(monkeypatch):
    def abrir(url, timeout):
        assert "/t/7060/n1/1/v/63/p/202501/c315/2" in url
        assert timeout == 30
        return BytesIO(b'[{}]')

    monkeypatch.setattr(radar, "urlopen", abrir)
    assert radar.consultar("1", ["1"], ["2"], ["202501"])[1] == [{}]


def test_normaliza_deflacao():
    assert radar.normalizar(
        [{}, linha(taxa="-0.45")], ["1"], ["2"], ["202501"]
    ) == {("1", "2", "202501"): Decimal("-0.45")}


@pytest.mark.parametrize("payload", [
    [{}], [{}, {}], [{}, linha(taxa="-")], [{}, linha(taxa="NaN")],
    [{}, linha(taxa="-100")], [{}, linha(), linha()], [{}, linha("202502")],
])
def test_rejeita_lote(payload):
    with pytest.raises(radar.DadosInvalidos):
        radar.normalizar(payload, ["1"], ["2"], ["202501"])


def test_acumulado_e_revisao():
    meses = radar.janela_12m("202511")
    assert meses[0] == "202412" and meses[-1] == "202511" and len(meses) == 12
    taxas = {("1", "2", mes): Decimal("1") for mes in meses}
    chave = ("1", "2", "202511")
    antes = radar.acumulados(taxas, ["1"], ["2"], ["202511"])
    assert antes[chave] == (Decimal("1.01") ** 12 - 1) * 100
    assert radar.acumulados(taxas, ["1"], ["2"], ["202511"]) == antes
    taxas["1", "2", meses[0]] = Decimal("0")
    assert radar.acumulados(taxas, ["1"], ["2"], ["202511"])[chave] < antes[chave]
    del taxas["1", "2", meses[0]]
    with pytest.raises(radar.DadosInvalidos):
        radar.acumulados(taxas, ["1"], ["2"], ["202511"])
