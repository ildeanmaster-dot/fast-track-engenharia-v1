# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Analytics: queries SQL dos 6 entregaveis
# MAGIC
# MAGIC Este notebook consome as tabelas Delta gold geradas no notebook 02
# MAGIC e roda queries representativas de cada um dos 6 entregaveis.

# COMMAND ----------

LAKEHOUSE = "/Volumes/workspace/ftkeng_v1/lakehouse"
GOLD = f"{LAKEHOUSE}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Atlas das frentes (Top 10 mais diversas)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_frente, nome_frente, n_membros, n_partidos, hhi
# MAGIC FROM delta.`/Volumes/workspace/ftkeng_v1/lakehouse/gold/gold_frente_diversidade`
# MAGIC ORDER BY hhi ASC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Calendario: top 10 deputados por taxa de presenca

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_deputado, nome, sigla_partido, sigla_uf, taxa_presenca
# MAGIC FROM delta.`/Volumes/workspace/ftkeng_v1/lakehouse/gold/gold_taxa_presenca`
# MAGIC ORDER BY taxa_presenca DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Frentes mais alinhadas (alinhamento_medio do voto)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_frente, nome_frente, alinhamento_medio, qtd_votacoes
# MAGIC FROM delta.`/Volumes/workspace/ftkeng_v1/lakehouse/gold/gold_resumo_alinhamento_frente`
# MAGIC ORDER BY alinhamento_medio DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - Top fornecedores CEAP (valor total)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cnpj_fornecedor, nome_fornecedor, valor_total, qtd_documentos, qtd_deputados
# MAGIC FROM delta.`/Volumes/workspace/ftkeng_v1/lakehouse/gold/gold_ceap_ranking_fornecedor`
# MAGIC ORDER BY valor_total DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 - CPIs identificadas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_orgao, sigla_orgao, nome_orgao, tipo_orgao,
# MAGIC        data_inicio, data_fim, duracao_dias, excedeu_prazo
# MAGIC FROM delta.`/Volumes/workspace/ftkeng_v1/lakehouse/gold/gold_cpis`
# MAGIC ORDER BY data_inicio DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 - Engajamento - top 10 mais engajados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_deputado, nome, sigla_partido, sigla_uf,
# MAGIC        n_presencas, n_votos, engajamento, percentil
# MAGIC FROM delta.`/Volumes/workspace/ftkeng_v1/lakehouse/gold/gold_engajamento`
# MAGIC ORDER BY engajamento DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6b - Absenteismo - quem mais faltou em votacoes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nome, sigla_partido, sigla_uf, n_ausencias, taxa_ausencia
# MAGIC FROM delta.`/Volumes/workspace/ftkeng_v1/lakehouse/gold/gold_absenteismo`
# MAGIC WHERE taxa_ausencia > 0
# MAGIC ORDER BY taxa_ausencia DESC
# MAGIC LIMIT 10
