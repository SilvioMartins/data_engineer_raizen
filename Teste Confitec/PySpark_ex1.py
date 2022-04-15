# Importando Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Definindo sessão Spark
spark = SparkSession \
    .builder \
    .appName("Exemplo de ETL CSV") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Pre-definindo o Schema - Muito mais rápido - Não precisa enomear colunas depois
sch = StructType() \
        .add("data", StringType(),True) \
        .add("dia_semana", StringType(),True) \
        .add("operacao", StringType(),True) \
        .add("TMR", StringType(),True) \
        .add("solicitacao_site", StringType(),True) \
        .add("entregues_novos", StringType(),True) \
        .add("perc_entregues", StringType(),True) \
        .add("atendidos_novos", StringType(),True) \
        .add("entregues_reaprov", StringType(),True) \
        .add("atendidos_reaprov", StringType(),True) \
        .add("perc_atend_nr", StringType(),True) \
        .add("perc_atend_novos", StringType(),True) \
        .add("perc_ete_novos", StringType(),True) \
        .add("entregues_nr", StringType(),True) \
        .add("atendidos_nr", StringType(),True) \
        .add("perc_atend_total", StringType(),True) \
        .add("aband_novos", StringType(),True) \
        .add("aband_reaprov", StringType(),True) \
        .add("cham_curta_novos", StringType(),True) \
        .add("cham_curta_reaprov", StringType(),True)

# Leitura CSV
df = spark.read.format("csv") \
     .option("header", True) \
     .option("encoding","UTF-8") \
     .option("delimiter",";") \
     .schema(sch) \
     .load("/content/drive/MyDrive/ESTUDOS/CSV/relatorio15.csv")

# UDF Tratando Campo Data
@udf(returnType=StringType()) 
def udt_trat(str):
    return str[6:10]+'-'+str[3:5]+'-'+str[0:2]

df = df.withColumn('data', udt_trat(col('data')))

# Pode Usar o Translat - newDf = testDF.withColumn('d_id', translate('d_id', 'a', '0'))
# Retirando % das colunas
df = df.withColumn('perc_entregues', regexp_replace('perc_entregues','%',''))
df = df.withColumn('perc_atend_nr', regexp_replace('perc_atend_nr','%',''))
df = df.withColumn('perc_atend_novos', regexp_replace('perc_atend_novos','%',''))
df = df.withColumn('perc_ete_novos', regexp_replace('perc_ete_novos','%',''))
df = df.withColumn('perc_atend_total', regexp_replace('perc_atend_total','%',''))

