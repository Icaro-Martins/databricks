# Databricks notebook source
# MAGIC %md
# MAGIC ### Bibliotecas

# COMMAND ----------

import json
import requests


# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerando o Token 

# COMMAND ----------

def token():

    url = "https://test.s360web.com/api/login"
    payload = "{\"username\":\"integracao.bpbunge@s360web.com\",\"password\":\"e3v8md0k\"}"
    headers = {
    'Content-Type': 'application/json;charset=UTF-8',
    'Accept': 'application/json, text/plain, */*'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response_dict = json.loads(response.text)
    tk = response_dict['access_token']
    token = 'Bearer' + ' ' + tk
    
    return token

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pegando Lista de Numero Amostra

# COMMAND ----------

url = "https://test.s360web.com/api/v3/resultadoAmostra/list/view?statusLeitura=false"
headers = {'Authorization':token()}
response = requests.request('POST',url, headers=headers,verify=False)

response_resultadoAmostra = json.loads(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 - Pegando Coalta de Numero Amostra - Modelo

# COMMAND ----------

response_data = []
repository_data = []
url  =  f"https://test.s360web.com/api/v3/resultadoAmostra/view?numeroAmostra=2200402860"
headers = {'Authorization': token()}
response = requests.request("GET", url, headers=headers, verify=False)
response_data = json.loads(response.text)
repository_data.append(response_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1 - View Data

# COMMAND ----------

data = []
for k in repository_data:
    data.append(k)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando Estrutura para os dados

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType

schema_view_simple = StructType([
        StructField('dadosColetaEquipamento', StructType([
             StructField('equipamento', StructType([
             	StructField('frota', StringType(), True),
             	StructField('fabricanteEquipamento', StructType([
             		StructField('nome', StringType(), True),
             		StructField('id', StringType(), True),
             		])),
             	StructField('familiaEquipamento', StructType([
             		StructField('nome', StringType(), True),
             		StructField('id', StringType(), True),
             		])),
             StructField('chassiSerie', StringType(), True),
             StructField('id', StringType(), True),
              StructField('unidadeControle', StringType(), True),
             StructField('modelo', StringType(), True),
             	])),
             StructField('compartimento', StructType([
             	StructField('tipoCompartimento', StructType([
             		StructField('nome', StringType(), True),
             		StructField('id', StringType(), True),
             		])),
             	StructField('nome', StringType(), True),
                StructField('id', StringType(), True)
             	])),
             ])),
        StructField('mensagem', StringType(),True),
		StructField('idioma', StringType(),True),
		StructField('foiSegregado', StringType(),True),
		StructField('codigoErro', StringType(),True),
		StructField('dadosColetaGeral', StructType([
			StructField('horasOleo', StringType(),True),
			StructField('oleo', StructType([
				StructField('fabricanteOleo', StructType([
					StructField('nome', StringType(), True),
                	StructField('id', StringType(), True)
				])),
				StructField('viscosidade', StructType([
					StructField('nome', StringType(), True),
                	StructField('id', StringType(), True)
				])),				
			])),
			StructField('oleoTrocado', StringType(),True),
			StructField('dataColeta', StringType(),True),
			StructField('comentario', StringType(),True)
		])),
		StructField('planosAnalise', StructType([
			StructField('planosAnalise', MapType(StringType(),StructType([
				StructField('nome', StringType(), True),
				StructField('id', StringType(), True)
			])),True)
		])),
		StructField('houvePreRegistro', StringType(),True),
		StructField('acoesInspecao', StringType(),True),
        StructField('resultadosAnalise', ArrayType(StringType()), True),
		StructField('responsavel', StringType(),True),
        StructField('comentarios', ArrayType(StringType()), True),
		StructField('dataRecebimento', StringType(),True),
		StructField('numeroCaixa', StringType(),True),
		StructField('avaliacao', StringType(),True),
		StructField('numeroAmostra', StringType(),True),
		StructField('dataFinalizacao', StringType(),True),
        StructField('cliente', MapType(StringType(),StringType()), True),
        StructField('obra', MapType(StringType(),StringType()), True),
		StructField('notaFiscalFrasco', StringType(),True),
		StructField('tipoColeta', StringType(),True),
		StructField('statusAmostra', StringType(),True)
  ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando Dataframe

# COMMAND ----------

df = spark.createDataFrame(data, schema_view_simple)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Acessando cada parte da estrutura

# COMMAND ----------

display(df)

# COMMAND ----------

#data_view = df.withColumn("planosAnalise2", df.planosAnalise.getItem("planosAnalise"))

df2 =  df.withColumn("planosAnalise2", df.planosAnalise.getItem("planosAnalise"))

display(df2.withColumn("planosAnalise.nome", df2.planosAnalise2.planoAnalise.getItem("nome"))\
           .withColumn("planosAnalise.id", df2.planosAnalise2.planoAnalise.getItem("id")))

# COMMAND ----------

data_view = df.withColumn("dadosColetaEquipamento.frota", df.dadosColetaEquipamento.equipamento.getItem("frota"))\
		      .withColumn("dadosColetaEquipamento.nome", df.dadosColetaEquipamento.equipamento.fabricanteEquipamento.getItem("nome"))\
		      .withColumn("dadosColetaEquipamento.id", df.dadosColetaEquipamento.equipamento.fabricanteEquipamento.getItem("id"))\
		      .withColumn("dadosColetaEquipamento.nome", df.dadosColetaEquipamento.equipamento.familiaEquipamento.getItem("nome"))\
		      .withColumn("dadosColetaEquipamento.id", df.dadosColetaEquipamento.equipamento.familiaEquipamento.getItem("id"))\
		      .withColumn("dadosColetaEquipamento.chassiSerie", df.dadosColetaEquipamento.equipamento.getItem("chassiSerie"))\
		      .withColumn("dadosColetaEquipamento.id", df.dadosColetaEquipamento.equipamento.getItem("id"))\
		      .withColumn("dadosColetaEquipamento.unidadeControle", df.dadosColetaEquipamento.equipamento.getItem("unidadeControle"))\
		      .withColumn("dadosColetaEquipamento.modelo", df.dadosColetaEquipamento.equipamento.getItem("modelo"))\
		      .withColumn("dadosColetaEquipamento.nome", df.dadosColetaEquipamento.compartimento.tipoCompartimento.getItem("nome"))\
		      .withColumn("dadosColetaEquipamento.id", df.dadosColetaEquipamento.compartimento.tipoCompartimento.getItem("id"))\
		      .withColumn("dadosColetaEquipamento.nome", df.dadosColetaEquipamento.compartimento.getItem("nome"))\
		      .withColumn("dadosColetaEquipamento.id", df.dadosColetaEquipamento.compartimento.getItem("id"))\
              .withColumn("dadosColetaGeral.horasOleo", df.dadosColetaGeral.getItem("horasOleo"))\
		      .withColumn("dadosColetaGeral.nome", df.dadosColetaGeral.oleo.fabricanteOleo.getItem("nome"))\
		      .withColumn("dadosColetaGeral.id", df.dadosColetaGeral.oleo.fabricanteOleo.getItem("id"))\
		      .withColumn("dadosColetaGeral.nome", df.dadosColetaGeral.oleo.viscosidade.getItem("nome"))\
		      .withColumn("dadosColetaGeral.id", df.dadosColetaGeral.oleo.viscosidade.getItem("id"))\
		      .withColumn("dadosColetaGeral.oleoTrocado",df.dadosColetaGeral.getItem("oleoTrocado"))\
		      .withColumn("dadosColetaGeral.dataColeta",df.dadosColetaGeral.getItem("dataColeta"))\
		      .withColumn("dadosColetaGeral.comentario",df.dadosColetaGeral.getItem("comentario"))\
		      .withColumn("planosAnalise.nome", df.planosAnalise.planosAnalise.getItem("nome"))\
 		      .withColumn("planosAnalise.id", df.planosAnalise.planosAnalise.getItem("id"))\
 		      .withColumn("cliente.nome",df.cliente.getItem("nome")) \
              .withColumn("cliente.id", df.cliente.getItem("id")) \
              .withColumn("obra.nome", df.obra.getItem("nome")) \
              .withColumn("obra.id", df.obra.getItem("id"))\
              .drop("dadosColetaEquipamento")\
              .drop("dadosColetaGeral")\
              .drop("planosAnalise")\
              .drop("cliente")\
              .drop("obra")

# COMMAND ----------

display(data_view)
