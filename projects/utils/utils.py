# IMPORTAÇÕES RELATIVAS
# Importações relativas podem ser usadas ao invés do comando mágico `%run`.

import re
import unicodedata 
from datetime import datetime
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

# NORMALIZA NOMES DAS COLUNAS PARA LOWER
def normalize_column_names_lower(df):
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
        col = col.lower().strip()
        cols.append(col)

    df = df.toDF(*cols)
    return df

# NORMALIZA NOMES DAS COLUNAS PARA UPPER
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

# NORMALIZA DADOS, REMOVENDO ACENTOS E SUBSTITUINDO CARACTERES ESPECIAIS
def remove_accents(text):

    replacements = {'À':'A','Á':'A','Ã':'A','Â':'A','É':'E','È':'E','Ê':'E','Í':'I','Ì':'I','Ó':'O','Ò':'O','Õ':'O','Ô':'O','Ù':'U','Ú':'U','Û':'U','Ç':'C','à':'a','á':'a','ã':'a','â':'a','é':'e','è':'e','ê':'e','í':'i','ì':'i','ó':'o','ò':'o','õ':'o','ô':'o','ù':'u','ú':'u','û':'u','ç':'c'}

    if text:
        chars = list(text)
        for i, char in enumerate(chars):
            if char in replacements and replacements[char] != char:
                text = text.replace(char, replacements[char])
    return text    
