# Databricks notebook source
import datetime
from pyspark.sql.functions import year, month, dayofmonth, current_date, current_timestamp, lpad, lit, pandas_udf, date_add, last_day, add_months
from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

def merge_deltatable(df, table, keys, filter=None):
  if filter is not None:
    raise NotImplementedError("Filtro ainda nao implementado")
  
  if isinstance(keys, str):
    keys = [keys]
  
  if '.' in table:
    tempTable = "temp_{0}_for_merge".format(table.split('.')[1])
  else:
    tempTable = "temp_{0}_for_merge"
    
  df.registerTempTable(tempTable)
  
  keys_string = '\n AND '.join(["destino.{0} = origem.{0}".format(column_name) for column_name in keys])
  mergeQuery = "MERGE INTO {0} as destino \n USING {1} origem ON \n {2} \n WHEN MATCHED THEN UPDATE SET * \n WHEN NOT MATCHED THEN INSERT * ".format(table, tempTable, keys_string)
  
  print("MergeQuery:", mergeQuery)
  
  try:
    spark.sql(mergeQuery)
  except:
    print("Merge com falha. Tentando sem broadcast")
    broadcastDefaultValue = spark.conf.get('spark.sql.autoBroadcastJoinThreshold')
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
    spark.sql(mergeQuery)
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', broadcastDefaultValue)
  
  print("Merge realizado na tabela {0} pelas chaves:".format(table), keys)
  
  return None

# COMMAND ----------

def rename_columns_to_upper(df, columns=None):
    if columns is None:
        columns = df.columns

    for column in columns:
          df = df.withColumnRenamed(column, column.upper())

    return df

# COMMAND ----------

def vacuum_deltatable(tableName, retainHours=720):
  spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')
  spark.sql("VACUUM {0} RETAIN {1} HOURS".format(tableName, str(retainHours)))
  print("Vacuum Executed on Table {0} with RETAIN {1} hours.".format(tableName, retainHours))

# COMMAND ----------

def get_first_day_of_month(column):
  return date_add(last_day(add_months(col(column), -1)), 1)

# COMMAND ----------

def lpad_string(string):
    """
    Padronizacao das particoes de datas com um '0' na frente.
    :param string: parametro de entrada que ira sofrer o lpad (de preferencia uma string)
    :return: Retorna a string com um '0' preenchido na esquerda caso seja uma string com somente 1 valor
    Ex: lpad_string('2') -> '02'
        lpad_string('22') -> '22'
    """
    string = str(string)

    if len(string) == 1:
        return '0' + string

    return string

# COMMAND ----------

# def get_current_timestamp(df, column_name='DataAtualizacao'):
def get_current_timestamp(df, column_name='DATA_ATUALIZACAO'):
  return df.withColumn(column_name, current_timestamp())

# COMMAND ----------

def create_date_partitions(df, ano=None, mes=None, dia=None):
    """
    Criacao das particoes ano, mes e dia
    :param df: DataFrame de entrada
    :param ano: Valor para ser preenchido na coluna ano. Caso nulo, retorna o ano do Current_timestamp
    :param mes: Valor para ser preenchido na coluna mes. Caso nulo, retorna o ano do Current_timestamp
    :param dia: Valor para ser preenchido na coluna dia. Caso nulo, retorna o ano do Current_timestamp
    :return: Retorna o DataFrame com as colunas ano, mes, e dia (aplicando lpad com zero na esquerda)
    """
    
    if ano is not None:
        df = df.withColumn('ano', lit(ano))
        
    if mes is not None:
        df = df.withColumn('mes', lit(mes))
        
    if dia is not None:
        df = df.withColumn('dia', lit(dia))
        
    return df

# COMMAND ----------

def padroniza_data(df, coluna, formato_entrada, formato_saida='yyyyMMdd'):
  return df.withColumn(coluna, to_date(col(coluna).cast('string'), formato_entrada))\
           .withColumn(coluna, date_format(col(coluna), formato_saida))

# COMMAND ----------

def renameColumns(df, columns):
  for old, new in columns.items():
    df = df.withColumnRenamed(old, new)
  return df

def rename_columns(df, columns):
  return renameColumns(df, columns)

# COMMAND ----------

def remove_from_all_columns_name(df, rename_value):
  for column in df.columns:
    if rename_value in column:
      df = df.withColumnRenamed(column, column.replace(rename_value, ''))
  
  return df

# COMMAND ----------

def widget_is_default(parameter, default_parameter):
  return dbutils.widgets.get(parameter) == default_parameter

# COMMAND ----------

def get_datafactory_date(datafactory_date, format_date="%Y-%m-%d", len_string=10):
  print("Parametro recebido:", datafactory_date)
  return_date = datetime.datetime.strptime(datafactory_date[:len_string], format_date)
  print("Parametro retornado:", return_date)
  return return_date

# COMMAND ----------

def join_dataframes(df1, df2, join_keys, how='inner', fields_to_get=None):
  
  JOIN_STRING = "__JOIN__KEY__"
  
  if not isinstance(join_keys, dict):
    raise TypeError(
      "join_keys must be of type <dict>. Found: %s".format(type(join_keys)))
  
  if fields_to_get is not None:
    df2 = df2.select(*list(set(list(join_keys.values()) + fields_to_get)))
    
  for k,v in join_keys.items():
    df1 = df1.withColumnRenamed(k, v + JOIN_STRING)
    df2 = df2.withColumnRenamed(v, v + JOIN_STRING)
    
  joined_df = df1.join(df2, [join_key + JOIN_STRING for join_key in join_keys.values()], how=how)
  
  for k, v in join_keys.items():
    joined_df = joined_df.withColumnRenamed(v + JOIN_STRING, k)
  
  return joined_df

# COMMAND ----------

def lpad_particoes(df, particoes):
    
    if isinstance(particoes, str):
      particoes = [particoes]
    
    for particao in particoes:

        df = df.withColumn(particao, lpad(col(particao).cast('string'), 2, '0'))
        
    return df

# COMMAND ----------

def insertAndCreateTableIfNotExists(df, outputPath, tableName, partitionBy=None):
    
    if len(tableName.split('.')) >= 2:
        database = tableName.split('.')[0]
        spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(database))
    
    try:
        dbutils.fs.ls(outputPath)
        needCreateTable = False
    except:
        needCreateTable = True
    if needCreateTable:
        print("Diretorio {0} nao existente: Criando primeiros dados.".format(outputPath))
        dfWriter = df.write.format('delta').mode('append')
        spark.sql("DROP TABLE IF EXISTS {0}".format(tableName))
        if partitionBy is not None:
            dfWriter = dfWriter.partitionBy(partitionBy)

        dfWriter.save(outputPath)

    spark.sql("CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}'".format(tableName, outputPath))

    return needCreateTable

# COMMAND ----------

@pandas_udf(IntegerType())
def convertJulianDate(data_string):
  JULIAN_EPOCH = datetime.datetime(2000, 1, 1, 12)
  J2000_JD = datetime.timedelta(2451545)
  julian_day = data_string.apply(
    lambda x: int((datetime.datetime.strptime(x, '%Y-%m-%d').replace(hour=12) - JULIAN_EPOCH + J2000_JD).days))
  return julian_day

# COMMAND ----------

def renameColumnsToUpperCamelCase(df, columns=None, sep='_'):
    
    if columns is None:
        columns = df.columns

    for column in columns:
        if column != 'DataAtualizacao':
          upperCaseColumn = column.split(sep)[0].title() + ''.join(x.title() for x in column.split(sep)[1:])
          df = df.withColumnRenamed(column, upperCaseColumn)

    return df

# COMMAND ----------

def teradata_to_lake_valida(dwUser, dwPass, dwDatabase, query, tableName, columnName=None, lowerBound=None, upperBound=None, numPartitions=None, partitionBy=None):
    dwServer = "tera-scan.riachuelo.net"
    dwJdbcPort = "1025"
    dwDatabase = dwDatabase
    dwUser = dwUser
    dwPass = dwPass
    #####
    outputPath = ABFS_PATH_SILVER + '/Validacoes_Tera/'+ tableName +'/'
    #####
    jdbcUrl = "jdbc:teradata://{0}/{1}".format(dwServer, dwDatabase)
    connectionProperties = {
      "user" : dwUser,
      "password" : dwPass,
      "jars" : "terajdbc4.jar", 
      "driver" : "com.teradata.jdbc.TeraDriver",
      "characterEncoding" : "UTF-8",
      "fetchsize": "10000",
      "batchsize": "10000",
      "numPartitions" : "4",
      "TYPE": "FASTEXPORT",
      "TMODE": "TERA"
    }
    
    ####
    query = query
    ####
    df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties, column=columnName, lowerBound=lowerBound, upperBound=upperBound, numPartitions=numPartitions)
    print('Recebendo dados do Teradata: tabela ' + tableName)
    insertAndCreateTableIfNotExists(df, outputPath, tableName, partitionBy)

# COMMAND ----------

def generate_map(string):
    mapColumns = {}
    mapColumns["type"] = 'TabularTranslator'
    mapColumns["mappings"] = []

    split_string =  string.replace('\n', '').split(',')
    i = 0
    for string in split_string:
        columnName = string.split(' ')[0]
        datatype = string.split(' ')[1]
        new_dict = {}
        new_dict['source'] = {}
        new_dict['source']['name'] = "Prop_{0}".format(str(i))
        new_dict['source']['type'] = datatype
        new_dict['sink'] = {}
        new_dict['sink']['name'] = columnName
        mapColumns['mappings'].append(new_dict)
        i+=1

    return str(mapColumns).replace("'", '"')

# COMMAND ----------

def deduplicar_df(df, partitionedBy, orderBy):
  """
  Realiza a deduplicacao do dataframe'.
  :param df: o dataframe em que sera realizada a deduplicacao.
  :param partitionedBy: Pode receber uma string/col, ou uma lista de colunas.
  :return: o dataframe deduplicado
  """
  from pyspark.sql.window import Window
  from pyspark.sql.functions import rank
 
  w = Window.partitionBy(partitionedBy).orderBy(orderBy)
 
  _df = df.withColumn('__rank__', rank().over(w))
 
  return _df.filter(col('__rank__') == 1).drop('__rank__')

# COMMAND ----------

def rank_df(df,partioning_columns,order_by_columns,order_mode=None,rank_column_name='rank'):
   """
    Retorna o dataframe com uma coluna de rank, para achar casos de duplicidade
    :param df: DataFrame de entrada
    :param partioning_columns: colunas de partição/chave para que seja feito a janela de ranking.
    :param order_by_columns: Colunas usadas na ordenação do rank.
    :param order_mode: modo de ordenação deve ser desc ou asc
    :param rank_column_name: Nome da coluna de rank que será criada. Personalizável
    :return: Retorna o DataFrame com as colunas ano, mes, e dia (aplicando lpad com zero na esquerda)
    """
   if order_mode not in ['desc','asc']:
     raise ProcessLookupError("Parâmetro order_mode deve ser 'desc' ou 'asc'")
   else:
     if order_mode == 'desc':
       return df.withColumn(rank_column_name,rank().over(Window.partitionBy(partioning_columns).orderBy(col(order_by_columns).desc())))
     else:
       return df.withColumn(rank_column_name,rank().over(Window.partitionBy(partioning_columns).orderBy(col(order_by_columns).asc())))

# COMMAND ----------

def table_max(database, tablelike):
  spark.sql(database)
  df_show = spark.sql("show tables")
  df = df_show.filter(col('tableName').like(tablelike))
  max_table = df.agg(_max(col("tableName")).alias("tableName"))
  name_table = max_table.first()[0]
  df_resul = spark.sql("""select * from {0}""".format(name_table))
  return df_resul, name_table

