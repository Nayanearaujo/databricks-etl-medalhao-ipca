# Databricks notebook source
# ==============================================================
# 00.config - Setup do Unity Catalog para Arquitetura Medalhão
# ==============================================================
CATALOG = "workspace"

# Definição dos Schemas de cada camada do Lakehouse
BRONZE = "bronze_economia"
SILVER = "silver_economia"
GOLD = "gold_economia"

# Ativação do Catálogo padrão
spark.sql(f"USE CATALOG {CATALOG}")

# Criação física dos esquemas se não existirem
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")

print("Estrutura Medalhão provisionada com sucesso no Unity Catalog!")
print(f"Camadas criadas: {CATALOG}.{BRONZE} | {CATALOG}.{SILVER} | {CATALOG}.{GOLD}")

# COMMAND ----------

