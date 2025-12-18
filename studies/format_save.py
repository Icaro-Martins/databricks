

# Isso é semelhante ao CREATE EXISTIS em SQL

if (spark._jspark.Session.catalog().tableExists("nome_da_tabela")):
    df_final_write.mode("overwrite").insertInto("nome_da_tabela")
else:
    df_final.write.mode("overwrite").partitionBy("nome_coluna").format("parquet").saveAsTable("nome_da_tabela")
    