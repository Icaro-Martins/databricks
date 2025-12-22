# Databricks notebook source
# MAGIC %md
# MAGIC ##### **GERADOR DE CÓDIGOS**
# MAGIC - DESENVOLVIMENTO

# COMMAND ----------

# DBTITLE 1,Dicionário de Atributos
# Ação que será relizada na campanha
ACAO = {
    "REGUA": "REG",
    "CAMPANHA": "CAM",
    "TRANSACIONAL": "TRS",
    "TESTE": "TST",
    "INFORMATIVO": "INF",
    "PESQUISA": "PES",
    "COMPORTAMENTAL": "COM",
    "OFERTAS":"OFE",
    "EXCLUSIVOS":"EXC"
}

# Empresa que ação será criada
EMPRESA = {"RIACHUELO": "RIACHUELO",
           "MIDWAY": "MIDWAY"}

# Se na Insider a campanha rodará automatica ou se será enviada manualmente 
TRIGGER = {"MANUAL": "MANUAL",
           "AUTOMATICO": "AUTOMATICO"}

# Produto da campanha
PRODUTO = {"RCH": "RCH",
           "EMPRETSTIMO":"EMP",
           "SEGUROS":"SEG",
           "CARTÃO":"CAR",
           "PARCELAMENTO DE FATURA":"PAC",
           "CONTA DIGITAL":"CND",
           "LOYALTY":"LYT"
        }

# SubProduto do produto e empresa escolhida, todos os produto midway e riachuelo estão aqui
SUB_PRODUTO ={
              "NA":"NA",
              "PL":"PL",
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
              "EMPRETSTIMO":"EP",
              "FGTS":"FGT",
              "PIX":"PIX",
              "BOLETO":"BLT",
              "CDB":"CDB",
              "CONTA":"CNT",
              "LIMITE GARANTIDO":"LGT",
              "SENHA":"SEN",
              "ON US":"ON",
              "OFF US":"OFF",
              "ON/OFF":"ON/OFF",
              "FEMININO":"FEM",
              "MASCULINO":"MAS",
              "INFANTIL E TEEN":"INF",
              "CBA":"CBA",
              "BELEZA E PERFUME":"BEP",
              "JEANS":"JEA",
              "CASA":"CSA",
              "PERSONAGENS":"FAN",
              "ACESSÓRIOS E RELÓGIOS":"ACS",
              "3P":"3P",
              "CUPONERIA":"CPN",
              "LPG":"LPG",
              "RFC":"RFC",
              "VJ":"VJ",
              "PARCELAMENTO DE FATURA":"PAC",
              "COMPRA FUTURA":"CPF",
              "GERAL/CROSS":"GRS",
              "CARTERS":"CAR",
              "TECH":"TEC",
              "BÁSICO":"BAS",
              "LICENCIADOS":"LIC"
              }

# Tipo da campanha
TIPO = {"OFERTA":"OFERTA",
        "INFORMATIVO":"INFORMATIVO",
        "PESQUISA":"PESQUISA",
        "DESCONTO":"DESCONTO",
        "CUPOM CUPONEIRA":"CUPOM CUPONEIRA",
        "CUPOM CRM":"CUPOM CRM",
        "PREÇO":"PREÇO",
        "PEÇAS":"PEÇAS",
        "OUTROS":"OUTROS"
        }

# Ciclo de Vida
CDV = {"AQUISIÇÃO":"AQUISIÇÃO",
       "ATIVAÇÃO":"ATIVAÇÃO",
       "ENGAJAMENTO":"ENGAJAMENTO",
       "RETENÇÃO":"RETENÇÃO",
       "PREVENTIVO":"PREVENTIVO",
       "REATIVAÇÃO":"REATIVAÇÃO",
       "SAZONAL":"SAZONAL"
       }


# Canal de disparo da campanha, pode ser mais de um canal
CANAL ={"EMAIL":"EMAIL",
        "SMS":"SMS",
        "PUSH":"PUSH",
        "BANNER":"BANNER",
        "WHATSAPP":"WHATSAPP",
        "MIDIAS":"MIDIAS",
        "CCR":"CCR",
        }

# Canal de contratação
CANALCONTRACAO = {
        "CCR-WHATS":"CCR-WHATS",
        "CCR-ATIVO":"CCR-ATIVO",
        "CCR-RECEPTIVO":"CCR-RECEPTIVO",
        "LOJA":"LOJA",
        "APP-MIDWAY":"APP-MIDWAY",
        "APP-RIACHUELO":"APP-RIACHUELO"
        }


# Indicadores de sucesso para 'quantidade'
KPI_QUANTIDADE = {
        "N/A":"N/A",
        "Contr Cartão PL":"Contr Cartão PL",
        "Contr Cartão BD":"Contr Cartão BD",
        "Desb Cartão PL":"Desb Cartão PL",
        "Desb Cartão BD":"Desb Cartão BD",
        "Atv Cartão PL":"Atv Cartão PL",
        "Atv Cartão BD":"Atv Cartão BD",
        "Uso Spending ON":"Uso Spending ON",
        "Uso Spending OFF":"Uso Spending OFF",
        "Uso Speding ON/OFF":"Uso Speding ON/OFF",
        "Compra com VJ":"Compra com VJ",
        "Atg meta cashback":"Atg meta cashback",
        "Uso do saldo CF":"Uso do saldo CF",
        "Contr Parc Fatura":"Contr Parc Fatura",
        "Contr EP":"Contr EP",
        "Contr FGTS":"Contr FGTS",
        "Contr Saque Fácil":"Contr Saque Fácil",
        "Contr Seguro ":"Contr Seguro ",
        "Voucher utililizados":"Voucher utililizados"
        }

# Indicadores de sucesso para 'volume'
KPI_VOLUME = {
        "N/A":"N/A",
        "Spending ON":"Spending ON",
        "Spending OFF":"Spending OFF",
        "Valor do VJ":"Valor do VJ",
        "Valor EP":"Valor EP",
        "Valor FGTS":"Valor FGTS",
        "Valor Saque fácil":"Valor Saque fácil",
        "Valor Seguro":"Valor Seguro",
        "Speding ON/OFF":"Speding ON/OFF",
        "Spending On CF":"Spending On CF",
        "Valor Parc Fatura":"Valor Parc Fatura"
        }

# Define as vezes que o cliente recebera a campanha
JORNADA = {"N/A":"N/A",
           "0":"0",
           "1":"1", 
           "5":"5",
           "7":"7"
        }

# Horario que sera disparado, valido somente para o canal de PUSH
HORARIO = {
        "N/A":"N/A",
        "9H":"9H",
        "13H":"13H",
        "17H":"17H",
        "20H":"20H",
        "22H":"22H"
}

# COMMAND ----------

# DBTITLE 1,Widgets & Atributos

import datetime
from pyspark.sql.functions import col, lit, substring

# Retorna o empresa especificada
dbutils.widgets.dropdown("Empresa", "MIDWAY", ["RIACHUELO", "MIDWAY"])
empresa = dbutils.widgets.get("Empresa")

# Retorna o produto especificado
dbutils.widgets.dropdown("Produto",  "CARTÃO", ["CARTÃO", "LOYALTY", "SEGUROS", "EMPRETSTIMO", "CONTA DIGITAL","PARCELAMENTO DE FATURA"])
produto_selecionado = dbutils.widgets.get("Produto")

subProduto = []
kip_quantidade = []
kip_volume = []
horario = []# ele só para push

if empresa == "MIDWAY":
    # Entra na condição de acordo com o produto selecionado
    # selecionado lista de sub-produto de cartão
    if produto_selecionado == "CARTÃO":
        subProduto = [
            "PL",
            "BANDEIRA",
            "ON US",
            "OFF US",
            "ON/OFF",
            "VJ",
            "CASHBACK BANDEIRA",
            "COMPRA FUTURA",
        ]

        kip_quantidade = [
            "Contr Cartão PL",
            "Contr Cartão BD",
            "Desb Cartão PL",
            "Desb Cartão BD",
            "Atv Cartão PL",
            "Atv Cartão BD",
            "Uso Spending ON",
            "Uso Spending OFF",
            "Uso Speding ON/OFF",
            "Compra com VJ",
            "Atg meta cashback",
            "Uso do saldo CF"
            ]
        
        kip_volume = [
            "Spending ON",
            "Spending OFF",
            "Valor do VJ",
            "Speding ON/OFF",
            "Spending On CF"
            ]

    # selecionado lista de sub-produto de loyalty
    elif produto_selecionado == "LOYALTY":
        subProduto = ["RFC"]
        kip_quantidade = ["N/A"]
        kip_volume = ["N/A"]

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

        kip_quantidade = ["Contr Seguro"]
        kip_volume = ["Valor Seguro"]

    # selecionado lista de sub-produto de ep
    elif produto_selecionado == "EMPRETSTIMO":
        subProduto = ["EMPRETSTIMO", "FGTS", "SAQUE FACIL"]
        kip_quantidade = ["Contr EP", "Contr FGTS", "Contr Saque Fácil"]
        kip_volume = ["Valor EP", "Valor FGTS", "Valor Saque fácil"]

    # selecionado lista de sub-produto de conta digital
    elif produto_selecionado == "CONTA DIGITAL":
        subProduto = ["PIX", "BOLETO", "SENHA", "CDB", "CONTA", "LIMITE GARANTIDO"]
        kip_quantidade = ["N/A"]
        kip_volume = ["N/A"]

    # selecionado lista de sub-produto de conta digital
    elif produto_selecionado == "PARCELAMENTO DE FATURA":

        subProduto = ["PARCELAMENTO DE FATURA"]
        kip_quantidade = ["Contr Parc Fatura"]
        kip_volume = ["Valor Parc Fatura"]

    # Remove o widget antigo, se existir
    # dbutils.widgets.remove("Sub_Produto")
    # Cria um widgets novo com nova lista de sub-produtos
    dbutils.widgets.dropdown("Sub_Produto", subProduto[0], subProduto)

    # Indicadores de Sucesso Quantidade
    dbutils.widgets.remove("Kpi Quantidade")
    dbutils.widgets.multiselect("Kpi Quantidade", kip_quantidade[0], kip_quantidade)

    #Indicadores de Sucesso Volume
    dbutils.widgets.remove("Kpi Volume")
    dbutils.widgets.multiselect("Kpi Volume", kip_volume[0], kip_volume)

    retorno_nao_encontrado = ""

elif empresa == "RIACHUELO":
     subProduto = ["GERAL/CROSS",
                   "CASA",
                   "3P",
                   "CBA",
                   "CARTERS",
                   "FEMININO",
                   "TECH",
                   "MASCULINO",
                   "INFANTIL E TEEN",
                   "BELEZA E PERFUME",
                   "LPG",
                   "BÁSICO",
                   "LICENCIADOS",
                   "RFC"
                ]
     
    # Retorna o produto especificado
     dbutils.widgets.dropdown("Produto",  "RCH", ["RCH"])
     produto_selecionado = dbutils.widgets.get("Produto")

     if produto_selecionado != "RCH":
        is_nao_encontrado = True 
        retorno_nao_encontrado = "Produto não encontrado"
        

     # Remove o widget antigo, se existir
     dbutils.widgets.remove("Sub_Produto")
     # Cria um widgets novo com nova lista de sub-produtos
     dbutils.widgets.dropdown("Sub_Produto", subProduto[0], subProduto)

     dbutils.widgets.multiselect("Kpi Quantidade", 'N/A', ['N/A'])
     dbutils.widgets.multiselect("Kpi Volume", 'N/A', ['N/A'])



# Dicionario de Ação
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
        "OFERTAS",
        "EXCLUSIVOS"
    ],
)


# Dicionatio de Tipo
dbutils.widgets.dropdown(
    "Tipo",
    "OFERTA",
    [
        "OFERTA",
        "INFORMATIVO",
        "PESQUISA",
        "DESCONTO",
        "CUPOM CUPONEIRA",
        "CUPOM CRM",
        "PREÇO",
        "PEÇAS",
        "OUTROS"
    ],
)

# Dicionario de ciclo de Vida(Cdv)
dbutils.widgets.dropdown(
    "Ciclo De Vida",
    "AQUISIÇÃO",
    ["AQUISIÇÃO", "ATIVAÇÃO", "ENGAJAMENTO", "RETENÇÃO", "PREVENTIVO", "REATIVAÇÃO","SAZONAL"]
)

# Dicionario de Canal de Comunicação
dbutils.widgets.multiselect(
    "Canal", "EMAIL", ["EMAIL", "SMS", "PUSH", "BANNER", "WHATSAPP", "MIDIAS", "CCR"]
)

# Dicionario de Canal de Comunicação
dbutils.widgets.multiselect(
    "Canal Contratação", "CCR-WHATS", ["CCR-WHATS", "CCR-ATIVO", "CCR-RECEPTIVO", "LOJA", "APP-MIDWAY", "APP-RIACHUELO"]
)

# Nome da Campanha
dbutils.widgets.text("Nome da campanha", "")


#Define se a campanha será dorada de forma autometica ou manual
dbutils.widgets.dropdown("Trigger", "MANUAL", ["MANUAL", "AUTOMATICO"])

# Jornda do Cliente, ou quantidades de contas que o cliente recebera
dbutils.widgets.multiselect("Qtd de Contato", "0", ["N/A","0","1","5","7"])

# Verifica se nos canais selecionados tem 'push' e aplica a condição de horairo, somente se ouver push
canal = dbutils.widgets.get("Canal").split(",")

if "PUSH" in canal:
  horario = ["N/A", "9H", "13H", "17H", "20H", "22H"]
  dbutils.widgets.remove("Horario")
  dbutils.widgets.dropdown("Horario", horario[0], horario)

elif "PUSH" not in canal:
    horario = ["N/A"]
    dbutils.widgets.dropdown("Horario", horario[0], horario)


# Pegando valores das chaves dos dicionarios atribuidos aos widgets
v_data_criacao   = datetime.date.today().strftime("%Y-%m-%d")
# pega os valores dos widgets
v_nome_campanha  = dbutils.widgets.get("Nome da campanha").upper().replace(" ", "_")
v_area_produto   = ACAO[dbutils.widgets.get("Ação")]
v_empresa        = EMPRESA[dbutils.widgets.get("Empresa")]
v_produtos       = PRODUTO[dbutils.widgets.get("Produto")]
v_sub_produto    = SUB_PRODUTO[dbutils.widgets.get("Sub_Produto")]
v_tipo           = TIPO[dbutils.widgets.get("Tipo")]
v_cdv            = CDV[dbutils.widgets.get("Ciclo De Vida")]
v_trigger        = TRIGGER[dbutils.widgets.get("Trigger")]
v_horario        = HORARIO[dbutils.widgets.get("Horario")]

v_canal_keys     = dbutils.widgets.get("Canal").split(",")
v_canal          = [CANAL[key] for key in v_canal_keys]

v_canal_contratacao_keys = dbutils.widgets.get("Canal Contratação").split(",")
v_canal_contratao = [CANALCONTRACAO[key] for key in v_canal_contratacao_keys]

v_jornada_keys   = dbutils.widgets.get("Qtd de Contato").split(",")
v_jornada        = [JORNADA[key] for key in v_jornada_keys]

v_kpi_quantidade_keys = dbutils.widgets.get("Kpi Quantidade").split(",")
v_kpi_quantidade      = [KPI_QUANTIDADE[key] for key in v_kpi_quantidade_keys]

v_kpi_volume_keys = dbutils.widgets.get("Kpi Volume").split(",")
v_kpi_volume      = [KPI_VOLUME[key] for key in v_kpi_volume_keys]

# COMMAND ----------

print(v_sub_produto)
print(v_canal)
print(v_kpi_quantidade)

# COMMAND ----------

# DBTITLE 1,Validando Ultimo Código & Criando Novo
# Validando o Ultimos codigo criado
ultimo_codigo = spark.sql("""select int(row) from db_midway_mkt_crm.gerador_de_codigos_dev order by row desc limit 1 """).collect()[0][0]

#id
id_codigo = str(int(ultimo_codigo) + 10001)

# Cria o código da campanha de acordo com a empresa
    cod_campanha = (id_codigo + "_" + v_area_produto + "_" + v_produtos + "_" + v_sub_produto + "_" + v_nome_campanha)

v_row = ultimo_codigo + 1 # Incrementando o ultimo codigo
v_data_ultima_criacao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M") # Informatimo para mostrar ao usuário
v_data_criacao = str(datetime.date.today().strftime("%Y-%m-%d")) # Data de criacao
v_usuario = (dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user")) # Usuario que criou a campanha

# COMMAND ----------

# DBTITLE 1,Salva a Campanha
# Variuaveis
len_cod_campanha = len(cod_campanha) # O Codigo da campanha tem que ter 40 caracteres
deu_certo = ''
nao_certo = ''
v_jornada_str = ', '.join(v_jornada)
v_canal_str = ', '.join(v_canal)
v_kpi_quantidade_str = ','.join(v_kpi_quantidade)
v_kpi_volume_str = ','.join(v_kpi_volume)
v_canal_contratao_str = ','.join(v_canal_contratao)


# cria os touchpoint
jornada_nome_list = []
if "N/A" in v_jornada:
    jornada_nome_str = v_jornada[0]
else:
    for i in v_jornada:
        jornada_nome = f"{cod_campanha}_{i}"
        jornada_nome_list.append(jornada_nome)
        jornada_nome_str = ', '.join(jornada_nome_list)

if len_cod_campanha <= 40:
    if v_nome_campanha != '':

       df_nova_campanha = spark.sql(
           f"""select  '{id_codigo}' as id_codigo,
                       '{cod_campanha}' as codigo_campanha,
                       '{v_row}' as row,
                       date('{v_data_criacao}') as data_criacao,
                       '{v_nome_campanha}' as nome_campanha,
                       '{v_empresa}' as empresa,
                       '{v_area_produto}' as acao,
                       '{v_produtos}' as produto,
                       '{v_sub_produto}' as sub_produto,
                       '{v_tipo}' as tipo_comunicacao,
                       '{v_canal_str}' as canal,
                       '{v_horario}' as horario_push,
                       '{v_cdv}' as ciclo_de_vida,
                       '{v_kpi_quantidade_str}' as kpi_quantidade,
                       '{v_kpi_volume_str}' as kpi_volume,
                       '{v_jornada_str}' as jornada,
                       '{jornada_nome_str}' as touchpoint,
                       '{v_canal_contratao_str}' as canal_contratacao,
                       '{v_trigger}' as trigger,
                       '{v_usuario}' as criado_por"""
       )

       # Retora o resultado para o usuário
       deu_certo = f"""Código criado com SUCESSO!\nHora {v_data_ultima_criacao}\n\nCódigo: {cod_campanha}\nTotal de caracteres: {len_cod_campanha}\nQtd Contato: {v_jornada_str}\nTouchpoint: {jornada_nome_str}"""

       # Salva a os dados
       df_nova_campanha.write.mode("append").option("mergeSchema", True).format("delta").saveAsTable(f"db_midway_mkt_crm.gerador_de_codigos_dev")
    else:
        nao_certo = f"ERRO!\nNome da campanha vazia!"
else:
    # Retora o resultado para o usuário caso não dado certo.
    nao_certo = f"ERRO!\nCódigo da campanha tem mais de 40 caracteres!\nTotal de caracteres, {str(len_cod_campanha)} ou {retorno_nao_encontrado}"


# COMMAND ----------

# MAGIC %md
# MAGIC ##### **RESULTADO**

# COMMAND ----------

# DBTITLE 1,Código da Campanha
# print(deu_certo)
# print(nao_certo)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_midway_mkt_crm.gerador_de_codigos_dev
# MAGIC order by id_codigo desc

# COMMAND ----------

# DBTITLE 1,Union Tabela Antiga com Nova
# df_gerador_old = spark.table("db_midway_mkt_crm.gerador_de_codigos")

# df_nova_campanha = (df_nova_campanha.withColumn("tipo_campanha",lit("N/A"))
#                 .withColumn("disparo",lit("N/A"))
#                 .withColumn("vigencia",lit("N/A"))
#                 .withColumn("tipo_oferta",lit("N/A"))
#                 .withColumn("recorrente_adhoc",lit("N/A"))
#                 .withColumn("mensuravel",lit("N/A"))
#                 .withColumn("detalhe_acao",lit("N/A"))
#                 )

# df = (
#     df_gerador_old
#     .withColumnRenamed("Codigo", "codigo_campanha")
#     .withColumnRenamed("NomeCampanha", "nome_campanha")
#     .withColumnRenamed("DataCriacao", "data_criacao")
#     .withColumn("acao", lit("N/A"))
#     .withColumnRenamed("AreaProduto", "produto")
#     .withColumnRenamed("Subproduto", "sub_produto")
#     .withColumnRenamed("TipoComunicacao", "tipo_comunicacao")
#     .withColumn("empresa", lit("N/A"))
#     .withColumn("ciclo_de_vida", lit("N/A"))
#     .withColumn("kpi_quantidade", lit("N/A"))
#     .withColumn("kpi_volume", lit("N/A"))
#     .withColumn("canal", lit("N/A"))
#     .withColumn("jornada", lit("N/A"))
#     .withColumn("touchpoint", lit("N/A"))
#     .withColumn("canal_contratacao", lit("N/A"))
#     .withColumn("trigger", lit("N/A"))
#     .withColumn("horario_push", lit("N/A"))
#     .withColumnRenamed("usuario", "criado_por")
#     .withColumnRenamed("Tipo_Campanha","tipo_campanha")
#     .withColumnRenamed("Disparo","disparo")
#     .withColumnRenamed("Vigencia","vigencia")
#     .withColumnRenamed("TipoOferta","tipo_oferta")
#     .withColumnRenamed("RecorrenteAdhoc","recorrente_adhoc")
#     .withColumnRenamed("Mensuravel","mensuravel")
#     .withColumnRenamed("Detalhe_Acao","detalhe_acao")
#     .select(
#         "id_codigo",
#         "codigo_campanha",
#         "row",
#         col("data_criacao").cast("date"),
#         "nome_campanha",
#         "empresa",
#         "acao",
#         "produto",
#         "sub_produto",
#         "tipo_comunicacao",
#         "canal",
#         "horario_push",
#         "ciclo_de_vida",
#         "kpi_quantidade",
#         "kpi_volume",
#         "jornada",
#         "touchpoint",
#         "canal_contratacao",
#         "trigger",
#         "criado_por",
#         "tipo_campanha",
#         "disparo",
#         "vigencia",
#         "tipo_oferta",
#         "recorrente_adhoc",
#         "mensuravel",
#         "detalhe_acao"
#     )
# )

# df_final = df.unionByName(df_nova_campanha)
# # df_final.display()

# #Salva a os dados
# df_final.write.mode("append").option("mergeSchema", True).format("delta").saveAsTable(f"db_midway_mkt_crm.gerador_de_codigos_dev")
