# Databricks notebook source
# MAGIC %run ../00.config/config

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import current_timestamp

# Endpoint SGS do Banco Central do Brasil - Série 433 (IPCA mensal)
url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
params = {
    "formato": "json",
    "dataInicial": "01/01/2024",
    "dataFinal": datetime.now().strftime("%d/%m/%Y")
}

response = requests.get(url, params=params)
data = response.json()

# Converte JSON em DataFrame Pandas para tratamento inicial do decimal
df_pd = pd.DataFrame(data)
df_pd.columns = ["data", "ipca"]

# Ajuste do padrão decimal brasileiro para numérico float
df_pd["ipca"] = df_pd["ipca"].str.replace(",", ".").astype(float)

# Conversão para Spark DataFrame e inclusão do metadado de auditoria exigido
df_spark = spark.createDataFrame(df_pd)
df_spark = df_spark.withColumn("data_coleta", current_timestamp())

# Gravação na camada Bronze com Delta Lake em modo append
tabela_destino = f"{CATALOG}.{BRONZE}.ipca"
(df_spark.write
    .format("delta")
    .mode("append")
    .saveAsTable(tabela_destino))

print(f"Tabela Bronze do IPCA gravada com sucesso em: {tabela_destino}")
display(df_spark)