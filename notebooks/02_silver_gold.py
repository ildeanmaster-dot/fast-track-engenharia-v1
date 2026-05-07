# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver + Gold
# MAGIC
# MAGIC Le Delta da bronze, aplica transformacoes (silver) e materializa
# MAGIC dimensoes/fatos + 6 entregaveis (gold).

# COMMAND ----------

LAKEHOUSE = "/Volumes/workspace/ftkeng_v1/lakehouse"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver - rename + dedup
# MAGIC
# MAGIC Versao Spark inline (sem importar do src/). Reaplica as mesmas regras
# MAGIC do silver.py local mas em DataFrames Spark.

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, to_date

def read_bronze(name):
    return spark.read.format("delta").load(f"{LAKEHOUSE}/bronze/{name}")


def silver_deputados():
    df = read_bronze("deputados")
    return df.selectExpr(
        "id as id_deputado",
        "nome",
        "siglaPartido as sigla_partido",
        "siglaUf as sigla_uf",
        "idLegislatura as id_legislatura",
        "uri",
    ).dropDuplicates(["id_deputado"])


def silver_partidos():
    df = read_bronze("partidos")
    return df.selectExpr(
        "id as id_partido",
        "sigla as sigla_partido",
        "nome as nome_partido",
    ).dropDuplicates(["id_partido"])


def silver_frentes():
    df = read_bronze("frentes")
    return df.selectExpr(
        "id as id_frente",
        "titulo as nome_frente",
        "idLegislatura as id_legislatura",
    ).dropDuplicates(["id_frente"])


def silver_frente_membros():
    df = read_bronze("frente_membros")
    return df.selectExpr(
        "_parent_id as id_frente",
        "id as id_deputado",
        "nome as nome_deputado",
        "siglaPartido as sigla_partido",
        "siglaUf as sigla_uf",
    ).dropDuplicates(["id_frente", "id_deputado"])


def silver_orgaos():
    df = read_bronze("orgaos")
    # campos data podem nao vir em todos os registros; coalesce ate ter algo
    cols_avail = set(df.columns)
    selects = [
        "id as id_orgao",
        "sigla as sigla_orgao",
        "nome as nome_orgao",
        "tipoOrgao as tipo_orgao",
        "codTipoOrgao as cod_tipo_orgao",
    ]
    if "dataInicio" in cols_avail:
        selects.append("to_date(dataInicio) as data_inicio")
    if "dataFim" in cols_avail:
        selects.append("to_date(dataFim) as data_fim")
    return df.selectExpr(*selects).dropDuplicates(["id_orgao"])


def silver_eventos():
    df = read_bronze("eventos")
    return df.selectExpr(
        "id as id_evento",
        "to_timestamp(dataHoraInicio) as data_hora_inicio",
        "to_timestamp(dataHoraFim) as data_hora_fim",
        "descricaoTipo as descricao_tipo",
        "situacao",
    ).dropDuplicates(["id_evento"])


def silver_evento_deputados():
    df = read_bronze("evento_deputados")
    return df.selectExpr(
        "_parent_id as id_evento",
        "id as id_deputado",
        "siglaPartido as sigla_partido",
        "siglaUf as sigla_uf",
    ).dropDuplicates(["id_evento", "id_deputado"])


def silver_votacoes():
    df = read_bronze("votacoes")
    return df.selectExpr(
        "id as id_votacao",
        "to_timestamp(dataHoraRegistro) as data_hora_registro",
        "descricao",
    ).dropDuplicates(["id_votacao"])


def silver_votacao_votos():
    df = read_bronze("votacao_votos")
    # campo 'deputado_' aninhado
    return df.selectExpr(
        "_parent_id as id_votacao",
        "deputado_.id as id_deputado",
        "deputado_.siglaPartido as sigla_partido",
        "deputado_.siglaUf as sigla_uf",
        "tipoVoto as tipo_voto",
    ).dropDuplicates(["id_votacao", "id_deputado"])


def silver_deputado_despesas():
    df = read_bronze("deputado_despesas")
    return df.selectExpr(
        "_parent_id as id_deputado",
        "ano",
        "mes",
        "tipoDespesa as tipo_despesa",
        "codDocumento as cod_documento",
        "valorLiquido as valor_liquido",
        "nomeFornecedor as nome_fornecedor",
        "cnpjCpfFornecedor as cnpj_fornecedor",
        "to_timestamp(dataDocumento) as data_documento",
    ).dropDuplicates(["id_deputado", "cod_documento"])


SILVER = {
    "deputados": silver_deputados,
    "partidos": silver_partidos,
    "frentes": silver_frentes,
    "frente_membros": silver_frente_membros,
    "orgaos": silver_orgaos,
    "eventos": silver_eventos,
    "evento_deputados": silver_evento_deputados,
    "votacoes": silver_votacoes,
    "votacao_votos": silver_votacao_votos,
    "deputado_despesas": silver_deputado_despesas,
}

silver_counts = {}
for name, fn in SILVER.items():
    try:
        df = fn()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
              .save(f"{LAKEHOUSE}/silver/{name}")
        silver_counts[name] = df.count()
        print(f"  silver.{name:<22s} {silver_counts[name]:>5d} rows")
    except Exception as e:
        print(f"  silver.{name}: FAIL {str(e)[:120]}")

print(f"\nSilver: {sum(silver_counts.values())} rows total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold - dimensoes e fatos

# COMMAND ----------

def read_silver(name):
    return spark.read.format("delta").load(f"{LAKEHOUSE}/silver/{name}")


def write_gold(name, df):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
          .save(f"{LAKEHOUSE}/gold/{name}")
    n = df.count()
    print(f"  gold.{name:<32s} {n:>5d} rows")
    return n


dep = read_silver("deputados")
part = read_silver("partidos")
fr = read_silver("frentes")
fm = read_silver("frente_membros")
orgs = read_silver("orgaos")
ev = read_silver("eventos")
ed = read_silver("evento_deputados")
vt = read_silver("votacoes")
vv = read_silver("votacao_votos")
desp = read_silver("deputado_despesas")

write_gold("dim_deputado", dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf", "id_legislatura"))
write_gold("dim_partido", part)
write_gold("dim_frente", fr)
write_gold("dim_orgao", orgs.select("id_orgao", "sigla_orgao", "nome_orgao", "tipo_orgao", "cod_tipo_orgao"))
write_gold("dim_evento", ev.select("id_evento", "data_hora_inicio", "data_hora_fim", "descricao_tipo", "situacao"))
write_gold("fato_voto", vv.select("id_votacao", "id_deputado", "tipo_voto"))
write_gold("fato_presenca", ed.select("id_evento", "id_deputado", "sigla_partido", "sigla_uf"))
write_gold("fato_despesa", desp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregavel 1 - Atlas das Frentes (HHI)

# COMMAND ----------

from pyspark.sql import functions as F

atlas = (fm.alias("m")
           .join(dep.alias("d"), "id_deputado", "left")
           .join(fr.alias("f"), "id_frente", "left")
           .select(
               "id_frente",
               F.col("f.nome_frente"),
               "id_deputado",
               F.col("d.nome").alias("nome"),
               F.col("d.sigla_partido"),
               F.col("d.sigla_uf"),
           ))
write_gold("gold_atlas_frentes", atlas)

# HHI por frente
participacao = (atlas.groupBy("id_frente", "sigla_partido")
                     .count()
                     .withColumnRenamed("count", "n_partido"))
total_frente = (atlas.groupBy("id_frente").count().withColumnRenamed("count", "total"))
hhi = (participacao.join(total_frente, "id_frente")
                   .withColumn("share", F.col("n_partido") / F.col("total"))
                   .groupBy("id_frente")
                   .agg(F.sum(F.pow("share", 2)).alias("hhi"),
                        F.first("total").alias("n_membros"),
                        F.countDistinct("sigla_partido").alias("n_partidos"))
                   .join(fr.select("id_frente", "nome_frente"), "id_frente", "left")
                   .orderBy("hhi"))
write_gold("gold_frente_diversidade", hhi)

display(hhi.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregavel 2 - Calendario / presenca

# COMMAND ----------

total_eventos = ev.select("id_evento").distinct().count()
presencas = (ed.groupBy("id_deputado").count().withColumnRenamed("count", "n_presencas"))
taxa = (presencas.join(dep, "id_deputado")
                 .withColumn("total_eventos", F.lit(total_eventos))
                 .withColumn("taxa_presenca", F.col("n_presencas") / F.col("total_eventos"))
                 .select("id_deputado", "nome", "sigla_partido", "sigla_uf",
                         "n_presencas", "total_eventos", "taxa_presenca")
                 .orderBy(F.col("taxa_presenca").desc()))
write_gold("gold_taxa_presenca", taxa)

# densidade semanal
densidade = (ev.withColumn("ano", F.year("data_hora_inicio"))
               .withColumn("semana", F.weekofyear("data_hora_inicio"))
               .groupBy("ano", "semana").count()
               .withColumnRenamed("count", "qtd_eventos")
               .orderBy("ano", "semana"))
write_gold("gold_densidade_semanal", densidade)

# eventos futuros
futuros = ev.filter(F.col("data_hora_inicio") > F.current_timestamp())
write_gold("gold_eventos_futuros", futuros)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregavel 3 - Alinhamento frente x partido

# COMMAND ----------

votos_validos = vv.filter(F.col("tipo_voto").isin("Sim", "Nao"))

def _alinhamento(df, group_col):
    pivot = (df.groupBy("id_votacao", group_col, "tipo_voto").count()
               .groupBy("id_votacao", group_col)
               .pivot("tipo_voto", ["Sim", "Nao"])
               .sum("count")
               .na.fill(0))
    pivot = (pivot.withColumn("total", F.col("Sim") + F.col("Nao"))
                  .filter(F.col("total") >= 2)
                  .withColumn("alinhamento",
                              F.greatest(F.col("Sim"), F.col("Nao")) / F.col("total")))
    return pivot

ali_partido = _alinhamento(votos_validos, "sigla_partido")
resumo_partido = (ali_partido.groupBy("sigla_partido")
                              .agg(F.avg("alinhamento").alias("alinhamento_medio"),
                                   F.countDistinct("id_votacao").alias("qtd_votacoes"))
                              .orderBy(F.col("alinhamento_medio").desc()))
write_gold("gold_resumo_alinhamento_partido", resumo_partido)

votos_frente = votos_validos.join(fm.select("id_frente", "id_deputado"), "id_deputado")
ali_frente = _alinhamento(votos_frente, "id_frente")
resumo_frente = (ali_frente.groupBy("id_frente")
                            .agg(F.avg("alinhamento").alias("alinhamento_medio"),
                                 F.countDistinct("id_votacao").alias("qtd_votacoes"))
                            .join(fr.select("id_frente", "nome_frente"), "id_frente", "left")
                            .orderBy(F.col("alinhamento_medio").desc()))
write_gold("gold_resumo_alinhamento_frente", resumo_frente)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregavel 4 - Raio-X CEAP

# COMMAND ----------

from pyspark.sql.window import Window

# top n por categoria
w = Window.partitionBy("tipo_despesa").orderBy(F.col("valor_liquido").desc())
top_categoria = (desp.join(dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf"), "id_deputado", "left")
                     .withColumn("rank", F.row_number().over(w))
                     .filter(F.col("rank") <= 10))
write_gold("gold_ceap_top_categoria", top_categoria)

# ranking fornecedor
ranking_forn = (desp.groupBy("cnpj_fornecedor", "nome_fornecedor")
                    .agg(F.sum("valor_liquido").alias("valor_total"),
                         F.count("cod_documento").alias("qtd_documentos"),
                         F.countDistinct("id_deputado").alias("qtd_deputados"))
                    .orderBy(F.col("valor_total").desc()))
write_gold("gold_ceap_ranking_fornecedor", ranking_forn.limit(100))

# mensal por partido
mensal_partido = (desp.join(dep.select("id_deputado", "sigla_partido"), "id_deputado", "left")
                      .groupBy("ano", "mes", "sigla_partido")
                      .agg(F.sum("valor_liquido").alias("total"),
                           F.count("cod_documento").alias("qtd"))
                      .orderBy("ano", "mes", F.col("total").desc()))
write_gold("gold_ceap_mensal_partido", mensal_partido)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregavel 5 - CPIs

# COMMAND ----------

cpis = orgs.filter(F.col("nome_orgao").rlike("(?i)CPI|CPMI|Inqu[eé]rito"))
# garante colunas data_inicio/data_fim mesmo se silver nao trouxer
if "data_inicio" not in cpis.columns:
    cpis = cpis.withColumn("data_inicio", F.lit(None).cast("date"))
if "data_fim" not in cpis.columns:
    cpis = cpis.withColumn("data_fim", F.lit(None).cast("date"))
cpis = (cpis.withColumn("duracao_dias", F.datediff("data_fim", "data_inicio"))
            .withColumn("excedeu_prazo", F.coalesce(F.col("duracao_dias") > 180, F.lit(False))))
write_gold("gold_cpis", cpis)

display(cpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entregavel 6 - Engajamento + Absenteismo

# COMMAND ----------

# engajamento simples: presencas + votos validos normalizados
n_pres = ed.groupBy("id_deputado").count().withColumnRenamed("count", "n_presencas")
n_vot = vv.groupBy("id_deputado").count().withColumnRenamed("count", "n_votos")

eng = (dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf")
          .join(n_pres, "id_deputado", "left")
          .join(n_vot, "id_deputado", "left")
          .na.fill(0, ["n_presencas", "n_votos"]))

# normaliza min-max
def _norm_col(df, c):
    rng = df.agg((F.max(c) - F.min(c)).alias("rng"), F.min(c).alias("mn")).collect()[0]
    rng_v = rng["rng"] or 1
    mn = rng["mn"] or 0
    return df.withColumn(f"{c}_norm", (F.col(c) - F.lit(mn)) / F.lit(rng_v))

eng = _norm_col(eng, "n_presencas")
eng = _norm_col(eng, "n_votos")
eng = eng.withColumn("engajamento", (F.col("n_presencas_norm") + F.col("n_votos_norm")) / 2)
eng = eng.withColumn("percentil", F.percent_rank().over(Window.orderBy("engajamento")))
write_gold("gold_engajamento", eng.orderBy(F.col("engajamento").desc()))

# absenteismo
total_v = vv.select("id_votacao").distinct().count()
votou = vv.groupBy("id_deputado").agg(F.countDistinct("id_votacao").alias("n_votou"))
absent = (dep.select("id_deputado", "nome", "sigla_partido", "sigla_uf")
             .join(votou, "id_deputado", "left")
             .na.fill(0, ["n_votou"])
             .withColumn("n_ausencias", F.lit(total_v) - F.col("n_votou"))
             .withColumn("taxa_ausencia", F.col("n_ausencias") / F.lit(total_v))
             .orderBy(F.col("taxa_ausencia").desc()))
write_gold("gold_absenteismo", absent)

# COMMAND ----------

print("=== Pipeline concluido ===")
print(f"Schema: workspace.ftkeng_v1")
print(f"Volume: {LAKEHOUSE}")
