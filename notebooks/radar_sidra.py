# Databricks notebook source
from datetime import datetime, timezone
from decimal import Decimal

from etl.radar import DadosInvalidos, acumulados, consultar, janela_12m, normalizar

# COMMAND ----------

dbutils.widgets.text("areas_n7", "")
dbutils.widgets.text("itens_c315", "")
dbutils.widgets.text("competencias_fim", "")

areas_regionais = dbutils.widgets.get("areas_n7").split(",")
itens = dbutils.widgets.get("itens_c315").split(",")
fins = dbutils.widgets.get("competencias_fim").split(",")
for valores in (areas_regionais, itens, fins):
    if any(not valor.isascii() or not valor.isdecimal() for valor in valores):
        raise ValueError("Informe códigos ou competências numéricas separados por vírgula")
    if len(set(valores)) != len(valores):
        raise ValueError("Os parâmetros não podem conter valores repetidos")
if len(areas_regionais) < 2 or "1" in areas_regionais:
    raise ValueError("Informe ao menos duas áreas regionais distintas do código Brasil")
if any(len(fim) != 6 for fim in fins):
    raise ValueError("Use AAAAMM nas competências")

meses = sorted({mes for fim in fins for mes in janela_12m(fim)})
areas = ["1", *areas_regionais]
coleta = datetime.now(timezone.utc)

# COMMAND ----------

from json import dumps

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze_economia")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver_economia")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold_economia")

lotes = []
for nivel, codigos in (("1", ["1"]), ("7", areas_regionais)):
    url, payload = consultar(nivel, codigos, itens, meses)
    spark.createDataFrame(
        [(url, dumps(payload, ensure_ascii=False), coleta)],
        "url STRING, payload STRING, data_coleta TIMESTAMP",
    ).write.format("delta").mode("append").saveAsTable("workspace.bronze_economia.sidra_raw")
    lotes.append((codigos, payload))

taxas = {}
for codigos, payload in lotes:
    try:
        taxas.update(normalizar(payload, codigos, itens, meses))
    except DadosInvalidos as erro:
        spark.createDataFrame(
            [(coleta, ",".join(codigos), str(erro))],
            "data_coleta TIMESTAMP, areas STRING, motivo STRING",
        ).write.format("delta").mode("append").saveAsTable("workspace.silver_economia.sidra_rejeicoes")
        raise

resultado = acumulados(taxas, areas, itens, fins)

# COMMAND ----------

for tabela, janela, coluna, dados in (
    ("workspace.silver_economia.ipca_alimentos", "competencia", "taxa_mensal", taxas),
    ("workspace.gold_economia.radar_alimentos", "competencia_fim", "acumulado_12m", resultado),
):
    esquema = f"cod_area STRING, cod_item STRING, {janela} STRING, {coluna} DECIMAL(24,12)"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tabela} ({esquema}) USING DELTA")
    escala = Decimal("0.000000000001")
    linhas = [
        (area, item, mes, valor.quantize(escala))
        for (area, item, mes), valor in dados.items()
    ]
    spark.createDataFrame(linhas, esquema).createOrReplaceTempView("radar_lote")
    spark.sql(f"""
        MERGE INTO {tabela} AS destino
        USING radar_lote AS origem
        ON destino.cod_area = origem.cod_area
           AND destino.cod_item = origem.cod_item
           AND destino.{janela} = origem.{janela}
        WHEN MATCHED AND NOT (destino.{coluna} <=> origem.{coluna}) THEN
          UPDATE SET destino.{coluna} = origem.{coluna}
        WHEN NOT MATCHED THEN INSERT *
    """)
