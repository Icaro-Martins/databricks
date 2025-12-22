# Databricks notebook source
# MAGIC %md
# MAGIC ## Descrição Database

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Describe Database
# MAGIC %sql
# MAGIC
# MAGIC -- DESCRIBE DATABASE db_midway_mkt_crm;

# COMMAND ----------

display(dbutils.fs.ls("abfss://databricksmetadata@stomktanalyticsprod.dfs.core.windows.net/db_midway_mkt_crm/tb_mensuracao_seguro_assistencia_ccr"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separando Deltas e não Deltas
# MAGIC
# MAGIC * Aqui separa tabelas que são deltas e as que não são deltas;
# MAGIC * Cada tabela é inserida em um lista
# MAGIC

# COMMAND ----------

show_table = spark.sql("SHOW TABLES IN db_midway_mkt_crm")
lista_tables = show_table.select(col("tableName")).rdd.flatMap(lambda x: x).collect()

isDelta = []
notDelta = []
for table in lista_tables:
  try:
    DeltaTable.forName(spark, f"db_midway_mkt_crm.{table}")
    isDelta.append(table)
  except:
    notDelta.append(table)

# COMMAND ----------

# isDelta_teste = ["tb_mensuracao_preventivas",
#                  "tb_mensuracao_seguro_assistencia_ccr",
#                  "tb_mensuracao_regua_cobranca_sa"]

# COMMAND ----------

for tableName in isDelta:
  df_describe = spark.sql(f"describe history db_midway_mkt_crm.{tableName}")
  df_describe = (
      df_describe.withColumn("tableName", lit(tableName))
      .select("userName", 
              "tableName", 
              substring(col("timestamp"), 0, 10).alias("data_atualizacao")
              )
      .distinct()
  )

  df_detail = spark.sql(f"describe detail db_midway_mkt_crm.{tableName}")
  df_detail = df_detail.withColumn("tableName", lit(tableName))

  df_tabelas_compiladas = (
      df_detail.alias("a")
      .join(df_describe.alias("b"), ["tableName"])
      .select(
          "a.tableName",
          "a.format",
          "a.id",
          "a.name",
          "a.location",
          "a.createdAt",
          "a.lastModified",
          "a.numFiles",
          "sizeInBytes",
          "b.userName",
          "b.data_atualizacao",
      )
  )
  df_tabelas_compiladas.write.mode("append").format("delta").saveAsTable("db_midway_mkt_crm.managing_tables")


#Table or view 'backup_acoes_crm_midway_v2' not found in database 'db_midway_mkt_crm'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from db_midway_mkt_crm.managing_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC distinct
# MAGIC tableName,
# MAGIC format,
# MAGIC id,
# MAGIC name,
# MAGIC location,
# MAGIC createdAt,
# MAGIC lastModified,
# MAGIC numFiles,
# MAGIC sizeInBytes
# MAGIC From db_midway_mkt_crm.managing_tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from db_midway_mkt_crm._detalhe_hist;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join entre base de Describe e History
# MAGIC
# MAGIC * Uni informações relevantes de cada tabela, data de criação e atulização

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Describe History

# COMMAND ----------

# df_describe = spark.sql("describe history db_midway_mkt_crm.tb_mensuracao_preventivas")
# df_describe = (
#     df_describe.withColumn(
#         "tableName", lit("db_midway_mkt_crm.tb_mensuracao_preventivas")
#     )
#     .select("userName", "tableName", substring(col("timestamp"), 0, 10).alias("data_atualizacao"))
#     .distinct()
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Describe Detail

# COMMAND ----------

# df_detail = spark.sql("describe detail db_midway_mkt_crm.tb_mensuracao_preventivas")
# df_detail = df_detail.withColumn("tableName", lit("db_midway_mkt_crm.tb_mensuracao_preventivas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join Detail e Describe de cada tabela

# COMMAND ----------

# df_detail.alias("a").join(df_describe.alias("b"), ["tableName"]).select(
#     "a.tableName",
#     "a.format",
#     "a.id",
#     "a.name",
#     "a.location",
#     "a.createdAt",
#     "a.lastModified",
#     "a.numFiles",
#     "sizeInBytes",
#     "b.userName",
#     "b.data_atualizacao",
# ).display()
