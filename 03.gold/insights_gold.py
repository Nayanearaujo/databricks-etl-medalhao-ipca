# Databricks notebook source
# MAGIC %run ../00.config/config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_economia = spark.read.table(f"{CATALOG}.{SILVER}.economia")

janela = Window.orderBy("data")

df_insights = (df_economia
    .withColumn("ipca_ant", F.lag("ipca", 1).over(janela))
    .withColumn("boi_ant", F.lag("boi_gordo", 1).over(janela))
    .withColumn("variacao_ipca", 
        F.when(F.col("ipca_ant").isNotNull(), 
               F.round(((F.col("ipca") - F.col("ipca_ant")) / F.col("ipca_ant")) * 100, 2)
        ).otherwise(None))
    .withColumn("variacao_boi", 
        F.when(F.col("boi_ant").isNotNull(), 
               F.round(((F.col("boi_gordo") - F.col("boi_ant")) / F.col("boi_ant")) * 100, 2)
        ).otherwise(None))
    .select("data", "ipca", "boi_gordo", "variacao_ipca", "variacao_boi")
    .orderBy("data"))

(df_insights.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD}.insights"))

display(df_insights)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     data,
# MAGIC     ipca,
# MAGIC     boi_gordo,
# MAGIC     variacao_ipca,
# MAGIC     variacao_boi
# MAGIC FROM workspace.gold_economia.insights
# MAGIC ORDER BY data;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.gold_economia.vw_gold_dashboard AS
# MAGIC SELECT 
# MAGIC     data,
# MAGIC     ipca,
# MAGIC     boi_gordo,
# MAGIC     variacao_ipca,
# MAGIC     variacao_boi
# MAGIC FROM workspace.gold_economia.insights;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ROUND(corr(variacao_ipca, variacao_boi), 4) AS correlacao_global
# MAGIC FROM workspace.gold_economia.vw_gold_dashboard
# MAGIC WHERE variacao_ipca IS NOT NULL AND variacao_boi IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     data,
# MAGIC     variacao_ipca,
# MAGIC     variacao_boi,
# MAGIC     CASE 
# MAGIC         WHEN variacao_ipca > 0 AND variacao_boi > 0 THEN 'Ambos em Alta'
# MAGIC         WHEN variacao_ipca < 0 AND variacao_boi < 0 THEN 'Ambos em Queda'
# MAGIC         WHEN variacao_boi > variacao_ipca THEN 'Preço do Boi cresce mais'
# MAGIC         ELSE 'Inflação cresce mais'
# MAGIC     END AS classe_impacto
# MAGIC FROM workspace.gold_economia.vw_gold_dashboard
# MAGIC WHERE variacao_ipca IS NOT NULL
# MAGIC ORDER BY data;