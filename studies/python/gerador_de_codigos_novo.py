# Databricks notebook source
# MAGIC %md
# MAGIC ##### GERADOR DE CÓDIGOS
# MAGIC

# COMMAND ----------

# DBTITLE 1,Importando Bibliotecas
import datetime
from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Import Código do Merge
# MAGIC %run /Users/icaro.martins.e@midway.com.br/functions/FunctionMerge

# COMMAND ----------

# DBTITLE 1,Dicionário de Atributos
ACAO = {
    "REGUA": "REG",
    "CAMPANHA": "CAM",
    "TRANSACIONAL": "TRS",
    "TESTE": "TST",
    "INFORMATIVO": "INF",
    "PESQUISA": "PES",
    "COMPORTAMENTAL": "COM",
}

EMPRESA = {"RIACHUELO": "RIACHUELO",
           "MIDWAY": "MIDWAY"}

TRIGGER = {"MANUAL": "MANUAL",
           "AUTOMATICO": "AUTOMATICO"}

PRODUTO = {"N/A":"N/A",
           "EP":"EMP",
           "SEGUROS":"SEG",
           "CARTÃO":"CAR",
           "PARCELAMENTO DE FATURA":"PAC",
           "CONTA DIGITAL":"CDB",
           "LOYALTY":"LYT"
        #    "COBRANÇA":"COB",#>>> é produto midway? qual o subproduto?
        }

SUB_PRODUTO ={"PL":"PL",
              "BANDEIRA":"BD",
              "USO":"USO",
              "ASSISTÊNCIA AUTOMÓVEL":"AA",
              "ASSISTÊNCIA MOTO":"AM",
              "ASSISTÊNCIA ODONTOLÓGICA":"AO",
              "ASSISTÊNCIA RESIDÊNCIA":"AR",
              "MAIS PROTEÇÃO PREMIÁVEL":"MP",
              "MAIS SAÚDE":"MS",
              "SEGURO ACIDENTES PESSOAIS":"AP",
              "SEGURO CONTA PAGA":"CP",
              "SEGURO RESIDENCIAL CASA PROTEGIDA":"RC",
              "SEGURO CELULAR":"SC",
              "VIDA":"SV",
              "EP":"EP",
              "FGTS":"FGT",
              "PIX":"PIX",
              "BOLETO":"BLT",
              "CDB":"CDB",
              "CONTA":"CNT", # Sanar Duvida
              "LIMITE GARANTIDO":"LGT", # Sanar Duvida
              "SENHA":"SEN",
              "NA":"NA",
              "ON US":"ON",
              "OFF US":"OFF",
              "FEMININO":"FEM",
              "MASCULINO":"MAS",
              "CROSS":"CRS",
              "INFANTIL E TEEN":"INF",
              "CBA":"CBA",
              "BELEZA E PERFUME":"BEP",
              "ELETRÔNICOS":"ELT",
              "JEANS":"JEA",
              "CASA":"CSA",
              "PERSONAGENS":"FAN",
              "ACESSÓRIOS E RELÓGIOS":"ACS",
              "3P":"3P",
              "CUPONERIA":"CPN",
              "LGP":"LGP",
              "RFC":"RFC",
              "VJ":"VJ",
              "PARCELAMENTO DE FATURA":"PAC"
              }


TIPO = {"OFERTA":"OFERTA",
        "ENGAJAMENTO":"ENGAJAMENTO",
        "PROMOÇÃO":"PROMOÇÃO",
        "SAZONAL":"SAZONAL",
        "INFORMATIVO":"INFORMATIVO",
        "PESQUISA":"PESQUISA",
        "CUPOM":"CUPOM",
        "PRECO":"PRECO"
        }

CDV = {"AQUISIÇÃO":"AQUISIÇÃO",
       "ATIVAÇÃO":"ATIVAÇÃO",
       "ENGAJAMENTO":"ENGAJAMENTO",
       "RETENÇÃO":"RETENÇÃO",
       "PREVENTIVO":"PREVENTIVO",
       "REATIVAÇÃO":"REATIVAÇÃO"
       }


CANAL ={"EMAIL":"EMAIL",
        "SMS":"SMS",
        "PUSH":"PUSH",
        "BANNER":"BANNER",
        "WHATSAPP":"WHATSAPP",
        "MIDIAS":"MIDIAS",
        "CCR":"CCR",
        }

INDICADORES = {
        "INDICADOR01":"INDICADOR-01",
        "INDICADOR02":"INDICADOR-02"
        }

JORNADA = {"N/A":"N/A",
           "D0":"D0",
           "D1":"D1", 
           "D5":"D5",
           "D7":"D7"
        }

# COMMAND ----------

# DBTITLE 1,Widgets & Atributos
# Retorna o empresa especificada
dbutils.widgets.dropdown("Empresa", "MIDWAY", ["RIACHUELO", "MIDWAY"])
empresa = dbutils.widgets.get("Empresa")

# Retorna o produto especificado
dbutils.widgets.dropdown("Produto",  "CARTÃO", ["N/A","CARTÃO", "LOYALTY", "SEGUROS", "EP", "CONTA DIGITAL","PARCELAMENTO DE FATURA"])
produto_selecionado = dbutils.widgets.get("Produto")

subProduto = [] 

if empresa == "MIDWAY":
    # Entra na condição de acordo com o produto selecionado
    # selecionado lista de sub-produto de cartão
    if produto_selecionado == "CARTÃO":
        subProduto = [
            "PL",
            "BANDEIRA",
            "ON US",
            "OFF US",
            "VJ",
            "CASHBACK BANDEIRA",
            "COMPRA FUTURA",
        ]

    # selecionado lista de sub-produto de loyalty
    elif produto_selecionado == "LOYALTY":
        subProduto = ["RFC"]

    # selecionado lista de sub-produto de seguros
    elif produto_selecionado == "SEGUROS":
        subProduto = [
            "ASSISTÊNCIA AUTOMÓVEL",
            "ASSISTÊNCIA MOTO",
            "ASSISTÊNCIA ODONTOLÓGICA",
            "ASSISTÊNCIA RESIDÊNCIA",
            "MAIS PROTEÇÃO PREMIÁVEL",
            "MAIS SAÚDE",
            "SEGURO ACIDENTES PESSOAIS",
            "SEGURO CONTA PAGA",
            "SEGURO RESIDENCIAL CASA PROTEGIDA",
            "SEGURO CELULAR",
            "VIDA",
        ]
    # selecionado lista de sub-produto de ep
    elif produto_selecionado == "EP":
        subProduto = ["EP", "FGTS", "SAQUE FACIL"]

    # selecionado lista de sub-produto de conta digital
    elif produto_selecionado == "CONTA DIGITAL":
        subProduto = ["PIX", "BOLETO", "SENHA", "CDB", "CONTA", "LIMITE GARANTIDO"]

    # selecionado lista de sub-produto de conta digital
    elif produto_selecionado == "PARCELAMENTO DE FATURA":
        subProduto = ["PARCELAMENTO DE FATURA"]

    # Remove o widget antigo, se existir
    dbutils.widgets.remove("Sub_Produto")
    # Cria um widgets noovo com nova lista de sub-produtos
    dbutils.widgets.dropdown("Sub_Produto", subProduto[0], subProduto)

elif empresa == "RIACHUELO":
     subProduto = ["FEMININO",
                   "MASCULINO",
                   "CROSS",
                   "INFANTIL E TEEN",
                   "CBA",
                   "BELEZA E PERFUME",
                   "ELETRÔNICOS",
                   "JEANS",
                   "CASA",
                   "PERSONAGENS",
                   "ACESSÓRIOS E RELÓGIOS",
                   "3P",
                   "CUPONERIA",
                   "LGP"
                ]
     
        # Retorna o produto especificado
     dbutils.widgets.dropdown("Produto",  "N/A", ["N/A"])
     produto_selecionado = dbutils.widgets.get("Produto")

     # Remove o widget antigo, se existir
     dbutils.widgets.remove("Sub_Produto")
     # Cria um widgets novo com nova lista de sub-produtos
     dbutils.widgets.dropdown("Sub_Produto", subProduto[0], subProduto)


# Dicionatio de Ação
dbutils.widgets.dropdown(
    "Ação",
    "REGUA",
    [
        "REGUA",
        "CAMPANHA",
        "TRANSACIONAL",
        "TESTE",
        "INFORMATIVO",
        "PESQUISA",
        "COMPORTAMENTAL",
    ],
)

# Dicionatio de Tpo
dbutils.widgets.dropdown(
    "Tipo",
    "OFERTA",
    [
        "OFERTA",
        "ENGAJAMENTO",
        "PROMOÇÃO",
        "SAZONAL",
        "INFORMATIVO",
        "PESQUISA",
        "CUPOM",
        "PRECO",
    ],
)

# Dicionatio de Cdv
dbutils.widgets.dropdown(
    "Ciclo De Vida",
    "AQUISIÇÃO",
    ["AQUISIÇÃO", "ATIVAÇÃO", "ENGAJAMENTO", "RETENÇÃO", "PREVENTIVO", "REATIVAÇÃO"],
)

# Dicionatio de Canal de Comunicação
dbutils.widgets.multiselect(
    "Canal", "EMAIL", ["EMAIL", "SMS", "PUSH", "BANNER", "WHATSAPP", "MIDIAS", "CCR"]
)

dbutils.widgets.text("Nome da campanha", "")

dbutils.widgets.multiselect("Indicador(es)", "INDICADOR01", ["INDICADOR01","INDICADOR02"])

dbutils.widgets.dropdown("Trigger", "MANUAL", ["MANUAL", "AUTOMATICO"])

dbutils.widgets.multiselect("Jornada", "D0", ["N/A","D0","D1", "D5","D7"])


# Pegando valores das chaves dos dicionarios atribuidos aos widgets
v_data_criacao   = datetime.date.today().strftime("%Y-%m-%d")

v_nome_campanha  = dbutils.widgets.get("Nome da campanha").upper().replace(" ", "_")
v_area_produto   = ACAO[dbutils.widgets.get("Ação")]
v_empresa        = EMPRESA[dbutils.widgets.get("Empresa")]
v_produtos       = PRODUTO[dbutils.widgets.get("Produto")]
v_sub_produto    = SUB_PRODUTO[dbutils.widgets.get("Sub_Produto")]
v_tipo           = TIPO[dbutils.widgets.get("Tipo")]
v_cdv            = CDV[dbutils.widgets.get("Ciclo De Vida")]
v_trigger        = TRIGGER[dbutils.widgets.get("Trigger")]

v_canal_keys     = dbutils.widgets.get("Canal").split(",")
v_canal          = [CANAL[key] for key in v_canal_keys]
v_jornada_keys   = dbutils.widgets.get("Jornada").split(",")
v_jornada        = [JORNADA[key] for key in v_jornada_keys]
v_indicador_keys = dbutils.widgets.get("Indicador(es)").split(",")
v_indicador      = [INDICADORES[key] for key in v_indicador_keys]

# COMMAND ----------

# DBTITLE 1,Validando Ultimo Código & Criando Novo
# Validando o Ultimos codigo criado
ultimo_codigo = spark.sql("""select int(row) from db_midway_mkt_crm.gerador_de_codigos order by row desc limit 1 """).collect()[0][0]

if v_empresa == "MIDWAY":
    # Criando Codigo Novo apartir do ultimo
    cod_campanha = (
        datetime.date.today().strftime("%y%m%d")
        + "_"
        + str(int(ultimo_codigo) + 10001)
        + "_"
        + v_area_produto
        + "_"
        + v_produtos
        + "_"
        + v_sub_produto
        + "_"
        + v_nome_campanha
    )

elif v_empresa == "RIACHUELO":
        cod_campanha = (
        datetime.date.today().strftime("%y%m%d")
        + "_"
        + str(int(ultimo_codigo) + 10001)
        + "_"
        + v_area_produto
        + "_"
        + "RCH"
        + "_"
        + v_sub_produto
        + "_"
        + v_nome_campanha
    )

v_row = ultimo_codigo + 1 # Incrementando o ultimo codigo
v_data_criacao = str(datetime.date.today().strftime("%Y-%m-%d")) # Data de criacao
v_usuario = (dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user")) # Usuario que criou a campanha


# COMMAND ----------

# O Codigo da campanha tem que ter 40 caracteres
len_cod_campanha = len(cod_campanha)
deu_certo = ''
nao_certo = ''
v_indicador_str = ', '.join(v_indicador)# Junta os indicadores em formato string
v_jornada_str = ', '.join(v_jornada)
v_canal_str = ', '.join(v_canal)

jornada_nome_list = []
if "N/A" in v_jornada:
    jornada_nome_str = v_jornada[0]
else:
    for i in v_jornada:
        jornada_nome = f"{cod_campanha}_{i}"
        jornada_nome_list.append(jornada_nome)

        jornada_nome_str = ', '.join(jornada_nome_list)

if len_cod_campanha <= 40:

    df_nova_campanha = spark.sql(
        f"""select  '{cod_campanha}' as codigo_campanha,
                    '{v_row}' as row,
                    date('{v_data_criacao}') as data_criacao,
                    '{v_nome_campanha}' as nome_campanha,
                    '{v_empresa}' as empresa,
                    '{v_area_produto}' as acao,
                    '{v_produtos}' as produto,
                    '{v_sub_produto}' as sub_produto,
                    '{v_tipo}' as tipo_comunicacao,
                    '{v_canal_str}' as canal,
                    '{v_cdv}' as ciclo_de_vida,
                    '{v_indicador_str}' as indicador_sucesso,                    
                    '{v_jornada_str}' as jornada,
                    '{jornada_nome_str}' as touchpoint,
                    '{v_trigger}' as trigger,
                    '{v_usuario}' as criado_por"""
    )
    
    # deu_certo = f"Código criado com sucesso!\nCódigo: " + cod_campanha + "\nTotal de caracteres: " + str(len_cod_campanha)

    deu_certo = f"""Código criado com sucesso!\nCódigo: {cod_campanha}\nTotal de caracteres: {len_cod_campanha}\nÁrvore/Jornada: {v_jornada_str}\nTouchpoint: {jornada_nome_str}"""

    #Salva a os dados
    table = "tb_gerador_de_codigos_insider"    
    mergeCondition = "tgt.codigo_campanha = src.codigo_campanha and tgt.data_criacao = src.data_criacao"
    columnPartition = "empresa"

    # _mergeSaveTable(df_nova_campanha, table, mergeCondition, columnPartition)

else:
    nao_certo = f"Código não foi criado!\nCódigo da campanha tem mais de 40 caracteres!\nTotal de caracteres:" + str(len_cod_campanha)


# COMMAND ----------

df_nova_campanha.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### GERADOR DE CÓDIGOS

# COMMAND ----------

# DBTITLE 1,Código da Campanha
print(deu_certo)
print(nao_certo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unificando o Novo Formato de Codigo campanha com antigo gerador de codigo

# COMMAND ----------

df_gerador_old = spark.table("db_midway_mkt_crm.gerador_de_codigos")

df_nova_campanha = (df_nova_campanha.withColumn("tipo_campanha",lit("N/A"))
                .withColumn("disparo",lit("N/A"))
                .withColumn("vigencia",lit("N/A"))
                .withColumn("tipo_oferta",lit("N/A"))
                .withColumn("recorrente_adhoc",lit("N/A"))
                .withColumn("mensuravel",lit("N/A"))
                .withColumn("detalhe_acao",lit("N/A"))
                )

df = (
    df_gerador_old.withColumnRenamed("Codigo", "codigo_campanha")
    .withColumnRenamed("NomeCampanha", "nome_campanha")
    .withColumnRenamed("DataCriacao", "data_criacao")
    .withColumn("acao", lit("N/A"))
    .withColumnRenamed("AreaProduto", "produto")
    .withColumnRenamed("Subproduto", "sub_produto")
    .withColumnRenamed("TipoComunicacao", "tipo_comunicacao")
    .withColumn("empresa", lit("N/A"))
    .withColumn("ciclo_de_vida", lit("N/A"))
    .withColumn("indicador_sucesso", lit("N/A"))
    .withColumn("canal", lit("N/A"))
    .withColumn("jornada", lit("N/A"))
    .withColumn("touchpoint", lit("N/A"))
    .withColumn("trigger", lit("N/A"))
    .withColumnRenamed("usuario", "criado_por")
    .withColumnRenamed("Tipo_Campanha","tipo_campanha")
    .withColumnRenamed("Disparo","disparo")
    .withColumnRenamed("Vigencia","vigencia")
    .withColumnRenamed("TipoOferta","tipo_oferta")
    .withColumnRenamed("RecorrenteAdhoc","recorrente_adhoc")
    .withColumnRenamed("Mensuravel","mensuravel")
    .withColumnRenamed("Detalhe_Acao","detalhe_acao")
    .select(
        "codigo_campanha",
        "row",
        col("data_criacao").cast("date"),
        "nome_campanha",
        "empresa",
        "acao",
        "produto",
        "sub_produto",
        "tipo_comunicacao",
        "canal",
        "ciclo_de_vida",
        "indicador_sucesso",
        "jornada",
        "touchpoint",
        "trigger",
        "criado_por",
        "tipo_campanha",
        "disparo",
        "vigencia",
        "tipo_oferta",
        "recorrente_adhoc",
        "mensuravel",
        "detalhe_acao"
    )
)

df_final = df.unionByName(df_nova_campanha)
df_final.display()

# COMMAND ----------


# - **NÃO MEXER NO CÓDIGO FONTE!**
# - **Tabela:** 
# - **Documentação:** [Wiki](https://dev.azure.com/DEVOPS-RCHLO/Tribo%20CRM/_wiki/wikis/Tribo-CRM.wiki/6437/Gerador-de-C%C3%B3digos)
