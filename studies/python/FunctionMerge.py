# Databricks notebook source
# MAGIC %md
# MAGIC ### Merge Tables
# MAGIC
# MAGIC **Chamada da Função:** _mergeSaveTable(dataframe, table, mergeCondition, columnPartition)
# MAGIC
# MAGIC | Parameters | Description | 
# MAGIC | ----------- | ----------- | 
# MAGIC | **dataframe** | dataframe final realizado | 
# MAGIC | **table**| Nome da tabela que será criada ou já criada |
# MAGIC | **mergeCondition** | A condição do merge é necessário, add *tgt* e *src* como alias: `tgt.column = src.column`|
# MAGIC | **columnPartition**| Nome da columa para particionar a tabela |

# COMMAND ----------

def _mergeSaveTable(dataframe, table, mergeCondition, columnPartition):
  try:
    from delta.tables import DeltaTable

    if (spark.catalog.tableExists(f"db_midway_mkt_crm.{table}")):
      deltaTable = DeltaTable.forName(spark,f"db_midway_mkt_crm.{table}")

      (deltaTable.alias("tgt").merge(dataframe.alias("src"), mergeCondition)
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())

      deltaTable.optimize().executeCompaction()
      #deltaTable.vaccum()

      print("Merge Sucess!")
    else:
      dataframe.write.mode("overwrite").format("delta").partitionBy(columnPartition).saveAsTable(f"db_midway_mkt_crm.{table}")
  except:
    print("Merge Erro! Revisar!")
