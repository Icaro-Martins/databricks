# IMPORTAÇÕES RELATIVAS
# Importações relativas podem ser usadas ao invés do comando mágico `%run`.

import re
import unicodedata 
from datetime import datetime
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

def display_teste(nome):
    display(nome)