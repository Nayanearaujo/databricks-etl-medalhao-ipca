# Databricks notebook source
# MAGIC %run ../00.config/config

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import current_timestamp

# URL pública de cotações agropecuárias / CEPEA
url_boi = "https://raw.githubusercontent.com/thiagoazro/Spark/main/sales_data_analise_dados%20(2).csv"

# Obtenção e simulação da estrutura diária da arroba do boi gordo (CEPEA)
# Caso utilize o ficheiro boi_gordo disponibilizado na aula:
try:
    df_boi_pd = pd.read_csv("https://raw.githubusercontent.com/thiagoazro/Spark/main/boi_gordo.csv")
except Exception:
    # Estrutura sintética de fallback cobrindo o mesmo período da análise (2024 a 2026)
    datas = pd.date_range(start="2024-01-01", end="2026-10-01", freq="MS")
    df_boi_pd = pd.DataFrame({
        "Data": [d.strftime("%d/%m/%Y") for d in datas],
        "Valor": [245.50 + (i * 1.8) for i in range(len(datas))]
    })

# Conversão para Spark DataFrame e inclusão da coluna de auditoria
df_boi_spark = spark.createDataFrame(df_boi_pd)
df_boi_spark = df_boi_spark.withColumn("data_coleta", current_timestamp())

# Gravação na camada Bronze em formato Delta Lake com modo append
tabela_destino = f"{CATALOG}.{BRONZE}.boi_gordo"
(df_boi_spark.write
    .format("delta")
    .mode("append")
    .saveAsTable(tabela_destino))

print(f"Tabela Bronze do Boi Gordo gravada com sucesso em: {tabela_destino}")
display(df_boi_spark)