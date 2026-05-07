# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingestao Bronze
# MAGIC
# MAGIC Le os JSONL de `data/samples/` (que ja foram coletados localmente)
# MAGIC e converte pra Delta no Volume UC `workspace.ftkeng_v1.lakehouse`.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO_PATH = "/Workspace/Users/developer@yottaflow.com.br/FAST-TRACK-ENGENHARIA-V1"
if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema e Volume

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ftkeng_v1")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ftkeng_v1.lakehouse")

LAKEHOUSE = "/Volumes/workspace/ftkeng_v1/lakehouse"
SAMPLES = f"{LAKEHOUSE}/samples"
dbutils.fs.mkdirs(f"{LAKEHOUSE}/bronze")
dbutils.fs.mkdirs(f"{LAKEHOUSE}/silver")
dbutils.fs.mkdirs(f"{LAKEHOUSE}/gold")
dbutils.fs.mkdirs(SAMPLES)

print("LAKEHOUSE:", LAKEHOUSE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copia samples do repo para o Volume

# COMMAND ----------

import shutil

src_dir = f"{REPO_PATH}/data/samples"
if os.path.exists(src_dir):
    for fname in os.listdir(src_dir):
        if fname.endswith(".jsonl"):
            src = f"{src_dir}/{fname}"
            if os.path.getsize(src) > 0:
                shutil.copy(src, f"{SAMPLES}/{fname}")

print("Arquivos no Volume:")
for f in dbutils.fs.ls(SAMPLES):
    print(f"  {f.name}  {f.size} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSONL -> Delta Bronze

# COMMAND ----------

from pyspark.sql.functions import lit

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
ingest_ts = datetime.now(timezone.utc).isoformat()

bronze_counts = {}
for entry in dbutils.fs.ls(SAMPLES):
    fname = entry.name
    if not fname.endswith(".jsonl"):
        continue
    name = fname[:-6]
    try:
        df = spark.read.json(f"{SAMPLES}/{fname}")
        df = (df.withColumn("ingest_ts", lit(ingest_ts))
                .withColumn("run_id", lit(run_id))
                .withColumn("endpoint", lit(name)))
        path = f"{LAKEHOUSE}/bronze/{name}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        bronze_counts[name] = df.count()
        print(f"  bronze.{name:<22s} {bronze_counts[name]:>5d} rows")
    except Exception as e:
        print(f"  bronze.{name}: FAIL {str(e)[:120]}")

print(f"\nBronze total: {sum(bronze_counts.values())} rows em {len(bronze_counts)} tabelas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificacao

# COMMAND ----------

df = spark.read.format("delta").load(f"{LAKEHOUSE}/bronze/deputados")
display(df.limit(5))
