# 🚀 Lakehouse ETL Pipeline: IPCA vs. Commodities Agropecuárias

Pipeline de Engenharia de Dados em larga escala no Databricks utilizando Apache Spark, PySpark, Delta Lake e Unity Catalog sob a Arquitetura Medalhão (Bronze, Silver e Gold).

## 🎯 Conceitos Chave
* **In-Memory Distributed Processing**: Execução paralela em cluster com DataFrames.
* **Catalyst Optimizer & Lazy Evaluation**: DAGs otimizados antes de ações de escrita.
* **Delta Lake ACID**: Transações atômicas, time travel e versionamento de tabelas.
* **Window Functions**: Uso de `F.lag()` cronológico para cálculo de variação percentual sem gargalos de Shuffle.
* **Governança com Unity Catalog**: Organização em 3 níveis (catalog.schema.table).

## 🏗️ Estrutura
* `00.config`: Schemas no Unity Catalog.
* `01.bronze`: Ingestão REST API (IPCA) e Commodities (Delta raw).
* `02.silver`: Limpeza, casting (Date/Double) e Inner Join.
* `03.gold`: Window Functions, Correlação de Pearson e Views analíticas.
