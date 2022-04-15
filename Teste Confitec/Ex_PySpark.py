# Instalando Biblioteca PySpark
!pip install pyspark

# Importando Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat, regexp_replace, concat_ws

# Definindo sessão Spark
spark = SparkSession \
    .builder \
    .appName("Exemplo de ETL CSV") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
	
# Leitura do Arquivo CSV e colocando em DataFrame
df = spark.read.csv(path='/content/drive/MyDrive/ESTUDOS/CSV/relatorio15.csv',
    sep=';',
    encoding='UTF-8',
    comment=None,
    header=True,
    inferSchema=True)
	
# Mostra Schema
df.printSchema()

# Renomeando Colunas
df = df.toDF('data',
        'dia_semana',
        'operacao',
        'TMR',
        'solicitacao_site',
        'entregues_novos',
        'perc_entregues',
        'atendidos_novos',
        'entregues_reaprov',
        'atendidos_reaprov',
        'perc_atend_nr',
        'perc_atend_novos',
        'perc_ete_novos',
        'entregues_nr',
        'atendidos_nr',
        'perc_atend_total',
        'aband_novos',
        'aband_reaprov',
        'cham_curta_novos',
        'cham_curta_reaprov'
        )