from datetime import date
from decimal import Decimal, InvalidOperation
from itertools import product
from json import load
from math import prod
from urllib.request import urlopen


class DadosInvalidos(ValueError):
    pass


def consultar(nivel, areas, itens, meses):
    url = (
        f"https://apisidra.ibge.gov.br/values/t/7060/n{nivel}/"
        f"{','.join(areas)}/v/63/p/{','.join(meses)}/c315/{','.join(itens)}"
    )
    with urlopen(url, timeout=30) as resposta:
        return url, load(resposta)


def normalizar(payload, areas, itens, meses):
    esperado = set(product(areas, itens, meses))
    taxas = {}
    for linha in payload[1:]:
        try:
            chave = linha["D1C"], linha["D4C"], linha["D3C"]
            taxa = Decimal(linha["V"])
        except (KeyError, TypeError, InvalidOperation) as erro:
            raise DadosInvalidos("Linha fora do contrato do SIDRA") from erro
        if chave not in esperado or chave in taxas:
            raise DadosInvalidos(f"Chave inesperada ou repetida: {chave}")
        if not taxa.is_finite() or taxa <= -100:
            raise DadosInvalidos(f"Taxa inválida: {chave}")
        taxas[chave] = taxa
    if taxas.keys() != esperado:
        raise DadosInvalidos(f"Chaves ausentes: {sorted(esperado - taxas.keys())}")
    return taxas


def janela_12m(competencia):
    fim = date(int(competencia[:4]), int(competencia[4:]), 1)
    indice = fim.year * 12 + fim.month - 1
    return [
        f"{mes // 12:04d}{mes % 12 + 1:02d}"
        for mes in range(indice - 11, indice + 1)
    ]


def acumulados(taxas, areas, itens, competencias):
    resultado = {}
    for area, item, fim in product(areas, itens, competencias):
        try:
            fatores = [
                1 + taxas[area, item, mes] / 100 for mes in janela_12m(fim)
            ]
        except KeyError as erro:
            raise DadosInvalidos(f"Janela incompleta para {area}/{item}/{fim}") from erro
        resultado[area, item, fim] = (prod(fatores) - 1) * 100
    return resultado
