# Databricks notebook source
import pyspark.sql.functions as f
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import unicodedata
import re
from pyspark.sql import DataFrame, Window

# COMMAND ----------

def move_column(df, move, after):
    """ Move a column after another column. Use to reorganize a specific column."""
    
    col_index = df.columns.index(after) + 1

    after_cols = df.columns[:col_index]
    before_cols = [c for c in df.columns[col_index:] if c != move]

    return df.select(after_cols + [move] + before_cols)

# COMMAND ----------

def move_column_before(df, move, before):
    """ Move a column after another column. Use to reorganize a specific column."""
    
    col_index = df.columns.index(before) - 1

    after_cols = df.columns[col_index:]
    before_cols = [c for c in df.columns[:col_index] if c != move]

    return df.select(before_cols + [move] + after_cols)

# COMMAND ----------

def normalize_column_names(df):
    import re
    cols = []
    
    for col in df.columns:
        col = col.lower().strip()
        col = col.replace('.','_')
        col = col.replace('\\', '_')
        col = col.replace('-','_')
        col = col.replace('³','3')
        # substitui espaçamento " " por "_"
        col = re.sub('\s','_',col)
        col = re.sub('ç','c',col)
        # substitui caracter "_+" no começo ou final por "":
        col = re.sub('^_[+]|_[+]$','',col)
        # substitui caracter "_" no começo ou final por "":
        col = re.sub('^_|_$','',col)
        # substitui caracter "+" no começo ou final por "":
        col = re.sub('^[+]|[+]$','',col)
        # substitui caracter "+" no meio por "_":
        col = re.sub('[+]','_',col)
        
        cols.append(col)

    df = df.toDF(*cols)
    return df

def normalize_column_names_upper(df):
    import re
    cols = []
    
    for col in df.columns:
        col = col.replace('.','_')
        col = col.replace('\\', '_')
        col = col.replace('-','_')
        col = col.replace('³','3')
        # substitui espaçamento " " por "_"
        col = re.sub('\s','_',col)
        col = re.sub('ç','c',col)
        # substitui caracter "_+" no começo ou final por "":
        col = re.sub('^_[+]|_[+]$','',col)
        # substitui caracter "_" no começo ou final por "":
        col = re.sub('^_|_$','',col)
        # substitui caracter "+" no começo ou final por "":
        col = re.sub('^[+]|[+]$','',col)
        # substitui caracter "+" no meio por "_":
        col = re.sub('[+]','_',col)
        col = col.upper().strip()
        cols.append(col)

    df = df.toDF(*cols)
    return df

# COMMAND ----------

def normalize_string(x):
    from unicodedata import normalize
    import re

    s = x.lower().strip()
    s = s.replace('\n', ' ')
    s = s.replace('.', ' ')
    s = s.replace('-', ' ')
    s = s.replace(r'/', '_')
    s = re.sub(' +', '_', s)
    s = re.sub('_+', '_', s)
    s = normalize('NFD', s).encode('ascii', 'ignore').decode("utf-8")
    return s

def string_upper(x):
    from pyspark.sql.functions import upper
    s = x.upper()
    return s

# COMMAND ----------

def normalize_numbers(df, cols, cast_type='float'):
    for col in cols:
        df = df.withColumn(col, f.translate(f.translate(col, '.', ''), ',', '.').cast(cast_type))
    return df

# COMMAND ----------

def correct_excel_date_format(df, col):
    original_column = col
    col = 't_' + col
    
    df = (
        df
        .withColumn(col, f.col(original_column))
        .withColumn(col, f.regexp_replace(col, 'jan', '01'))
        .withColumn(col, f.regexp_replace(col, 'fev', '02'))
        .withColumn(col, f.regexp_replace(col, 'mar', '03'))
        .withColumn(col, f.regexp_replace(col, 'abr', '04'))
        .withColumn(col, f.regexp_replace(col, 'mai', '05'))
        .withColumn(col, f.regexp_replace(col, 'jun', '06'))
        .withColumn(col, f.regexp_replace(col, 'jul', '07'))
        .withColumn(col, f.regexp_replace(col, 'ago', '08'))
        .withColumn(col, f.regexp_replace(col, 'set', '09'))
        .withColumn(col, f.regexp_replace(col, 'out', '10'))
        .withColumn(col, f.regexp_replace(col, 'nov', '11'))
        .withColumn(col, f.regexp_replace(col, 'dez', '12'))
        .withColumn('temp_dia', f.split(col, '/').getItem(0))
        .withColumn('temp_mes', f.split(col, '/').getItem(1))
        .withColumn('temp_ano', f.split(col, '/').getItem(2))
        .withColumn(col, f.when(f.length('temp_ano') == 2, f.concat_ws('/', f.col('temp_dia'), f.col('temp_mes'), f.concat(f.lit('20'), f.col('temp_ano')))))
        .drop('temp_dia', 'temp_mes', 'temp_ano')
    )
    return df

# COMMAND ----------

def normalize_date(df, cols):
    for col in cols:
        df = df.withColumn(col, f.to_date(f.col(col), 'dd/MM/yyyy'))
    return df

# COMMAND ----------

def normalize_currency(df, cols):
    for col in cols:
        df = df.withColumn(col, f.translate(col, '.', ''))
        df = df.withColumn(col, f.translate(col, ',', '.'))
        df = df.withColumn(col, f.translate(col, 'R$ ', ''))
        df = df.withColumn(col, f.round(f.col(col).cast('float'), 2))
    return df

# COMMAND ----------

def get_date_time_now():
    from pytz import timezone
    from datetime import datetime

    tz = timezone('America/Sao_Paulo')
    now = datetime.now().astimezone(tz)

    return now.strftime('%Y%m%d'), now.strftime('%H%M%S')

# COMMAND ----------

replacements = {
  'À':'A',
  'Á':'A',
  'Ã':'A',
  'Â':'A',
  'É':'E',
  'È':'E',
  'Ê':'E',
  'Í':'I',
  'Ì':'I',
  'Ó':'O',
  'Ò':'O',
  'Õ':'O',
  'Ô':'O',
  'Ù':'U',
  'Ú':'U',
  'Û':'U',
  'Ç':'C',
  'à':'a',
  'á':'a',
  'ã':'a',
  'â':'a',
  'é':'e',
  'è':'e',
  'ê':'e',
  'í':'i',
  'ì':'i',
  'ó':'o',
  'ò':'o',
  'õ':'o',
  'ô':'o',
  'ù':'u',
  'ú':'u',
  'û':'u',
  'ç':'c',
}


def remove_accents(text):
    if text:
        chars = list(text)
        for i, char in enumerate(chars):
            if char in replacements and replacements[char] != char:
                text = text.replace(char, replacements[char])
    return text


    
remove_accents_udf = udf(remove_accents, StringType())

# COMMAND ----------

w_space = {
  ' ':''
}

def remove_white(text):
    if text:
        chars = list(text)
        for i, char in enumerate(chars):
            if char in w_space and w_space[char] != char:
                text = text.replace(char, w_space[char])
    return text

    
    
remove_white_space_udf = udf(remove_white, StringType())

# COMMAND ----------

character_replacements = {
  '-':'',
  '_':'',
  '(':'',
  ')':'',
  '/':'',
  '?':'',
  '.':''
}

def remove_specials(text):
    if text:
        chars = list(text)
        for i, char in enumerate(chars):
            if char in character_replacements and character_replacements[char] != char:
                text = text.replace(char, character_replacements[char])
    return text

    
    
remove_specials_udf = udf(remove_specials, StringType())

# COMMAND ----------

# funçao que retira acentuação de valores de colunas STRING:
def retira_acentuacao_df(df, skip_cols=None):
    if skip_cols is None:
        skip_cols = []
        
    # Lista todas as colunas string do df
    string_cols = [col_name for col_name, col_type in df.dtypes 
                   if col_type.startswith('string')]
    
    # Tabela de mapeamento para remover acentos
    map_from = 'ãâáàêéèíìõôóòúùçÃÂÁÀÊÉÈÍÌÕÔÓÒÚÙÇ'
    map_to   = 'aaaaeeeiioooouucAAAAEEEIIOOOOUUC'
    
    # Aplica o translate apenas nas colunas de interesse
    for c in string_cols:
        if c not in skip_cols:
            df = df.withColumn(
                c,
                f.translate(f.col(c), map_from, map_to)
            )
    return df

# COMMAND ----------

# funcao que padroniza nome de coluna como tudo minusculo, sem espaços e sem acentuações:
def trata_nome_coluna(df):
    def normalize_and_replace(col):
        norm_col = unicodedata.normalize('NFKD', col).encode('ascii','ignore').decode('utf-8')
        norm_col = norm_col.lower()
        norm_col = re.sub(r'[^a-zA-Z0-9_]+', '_', norm_col)
        return norm_col.rstrip('_')
    
    df = df.toDF(*[normalize_and_replace(col) for col in df.columns])
    return df

# COMMAND ----------

def casas_decimais(df, cols, nmr_casas):
    for col in cols:
        df = df.withColumn(col, f.round(col, nmr_casas))
    return df

# COMMAND ----------

# Função para renomear colunas duplicadas com sufixos únicos
def rename_duplicate_columns(df, suffix):
    col_names = df.columns
    col_count = {}
    new_col_names = []
    
    for col in col_names:
        if col not in col_count:
            col_count[col] = 1
            new_col_names.append(col)
        else:
            col_count[col] += 1
            new_col_names.append(f"{col}_{suffix}_{col_count[col]}")
    
    return df.toDF(*new_col_names)

# COMMAND ----------

# Função para remover colunas duplicadas
def remove_duplicate_columns(df):
    unique_columns = []
    seen = set()
    for col in df.columns:
        if col not in seen:
            seen.add(col)
            unique_columns.append(col)
    return df.select(*unique_columns)

# COMMAND ----------

#Contrato de dados

def validate_dataframe(df: DataFrame, validation_rules: dict, name_view: str) -> DataFrame:
    """
    Aplica validações em colunas individuais e validações globais a um DataFrame.

    :param df: PySpark DataFrame a ser validado.
    :param validation_rules: Dicionário contendo regras de validação.
        - Chave: Nome da coluna para validação.
        - Valor: Regra de validação (expressão PySpark).
    :param name_view: Nome da view temporária para armazenar os dados validados.
    :return: DataFrame com colunas de validação adicionadas e validação global.
    """
    df = df
    # Iterar sobre as regras de validação para aplicar as condições em cada coluna
    for column_name, rule in validation_rules.items():
        validation_column = f"valid_{column_name}"
        df = df.withColumn(validation_column, f.when(rule, True).otherwise(False))

    # Criar uma coluna de validação global "all_valid"
    validation_columns = [f.col(f"valid_{col_name}") for col_name in validation_rules.keys()]
    df = df.withColumn("all_valid", reduce(lambda x, y: x & y, validation_columns))

    #aqui retorna todas as linhas válidas que passaram pelo contrato de dados
    df_valido = df.filter("all_valid = true").select(*[col for col in df.columns if not col.startswith("valid_")])
    df_invalido = df.filter("all_valid = false")

    # criação das views - para facilitar a visualização
    df_valido.createOrReplaceTempView(name_view)
    df_invalido.createOrReplaceTempView(f'{name_view}_invalido')
    
    print("view criada com nome: " + name_view)
    print("view criada com nome: " + name_view + '_invalido')

    print('valores validos: ', df_valido.count())
    print('valores invalidos: ', df_invalido.count())


    return df_valido, df_invalido


# Funções utilitárias para facilitar a definição de regras

# Avalia se os campos de uma lista não são nulos
def columns_not_null(columns):
    """
    Retorna uma expressão PySpark que avalia se todos os campos de uma lista são não nulos.

    :param columns: Lista de colunas a serem avaliadas.

    :return: Expressão PySpark que avalia se todos os campos são não nulos.
    """


    return reduce(lambda x, y: x & y, [f.col(column).isNotNull() for column in columns])

# Avalia se todos os campos de uma lista são maiores ou iguais a um valor alvo
def columns_ge(columns, value):
    return reduce(lambda x, y: x & y, [f.col(column) >= value for column in columns])

# Avalia se todos os campos de uma lista são menores ou iguais a um valor alvo
def columns_le(columns, value):
    return reduce(lambda x, y: x & y, [f.col(column) <= value for column in columns])

# Avalia se o campo pode ser convertido para numérico, preservando nulos como válidos
def coluna_numerica(column):
    return (
          f.when(
  f.trim(f.col(column)) != '',  # Não está vazio
  f.when(f.col(column).cast("double").isNotNull(), True  # É numérico
          ).otherwise(False)  # Não é numérico
  ).otherwise(  # Está vazio ou nulo
              f.when(
                f.trim(f.col(column)).isNull() or f.trim(f.col(column)) == '' , True  # É nulo
                ).otherwise(False)  # Caso contrário, inválido
              )
    )

def coluna_primaria(column: str):
    """
    Retorna uma expressão lógica PySpark que verifica se uma coluna é uma chave primária (única e não nula).

    Args:
        column: Nome da coluna a ser verificada.

    Returns:
        Expressão lógica PySpark que valida se os valores na coluna são únicos e não nulos.
    """
    # Especificação de janela para verificar duplicados
    window_spec = Window.partitionBy(column)
    
    # Verifica se a contagem na janela é 1 e se o valor não é nulo
    return f.col(column).isNotNull() & (f.count(f.col(column)).over(window_spec) == 1)
