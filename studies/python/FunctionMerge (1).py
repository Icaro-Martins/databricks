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

def _mergeSaveTable(dataframe, table, mergeCondition):
  from delta.tables import DeltaTable

  try:
    print("Inicio Merge...")
    if (spark.catalog.tableExists(f"db_midway_mkt_crm.{table}")):
      try:
        deltaTable = DeltaTable.forName(spark,f"db_midway_mkt_crm.{table}")

        (deltaTable.alias("tgt").merge(dataframe.alias("src"), mergeCondition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
              
        # deltaTable.optimize().executeCompaction()

        print("Merge Sucess!")
      except:
        print("Merge Erro! Revisar!")
        print(erro.message)
    else:
      dataframe.write.mode("overwrite").format("delta").saveAsTable(f"db_midway_mkt_crm.{table}")
      print("Tabela Criada")
  except:
    print("Erro ao criar tabela")
    print(erro.message)
     
