# Databricks notebook source
# MAGIC %run ../00.config/config

# COMMAND ----------

from pyspark.sql import functions as F

df_ipca = spark.read.table(f"{CATALOG}.{BRONZE}.ipca")
df_boi = spark.read.table(f"{CATALOG}.{BRONZE}.boi_gordo")

df_ipca_silver = (df_ipca
    .withColumn("mes_ano", F.date_format(F.to_date("data", "dd/MM/yyyy"), "yyyy-MM"))
    .withColumn("ipca", F.col("ipca").cast("double"))
    .select("mes_ano", "ipca"))

df_boi_silver = (df_boi
    .withColumn("data_fmt", F.to_date("Data", "dd/MM/yyyy"))
    .withColumn("mes_ano", F.date_format("data_fmt", "yyyy-MM"))
    .withColumn("preco_boi", F.regexp_replace(F.col("Valor"), ",", ".").cast("double"))
    .groupBy("mes_ano")
    .agg(F.round(F.avg("preco_boi"), 2).alias("boi_gordo")))

df_silver_final = (df_ipca_silver
    .join(df_boi_silver, on="mes_ano", how="inner")
    .withColumn("data", F.to_date(F.concat(F.col("mes_ano"), F.lit("-01")), "yyyy-MM-dd"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("data_coleta", F.current_timestamp())
    .select("data", "ipca", "boi_gordo", "mes", "ano", "data_coleta")
    .orderBy("data"))

(df_silver_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER}.economia"))

display(df_silver_final)