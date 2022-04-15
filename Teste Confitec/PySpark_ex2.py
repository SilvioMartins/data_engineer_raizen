#!pip install pyspark

#Instalando Bibliotecas
from pyspark.sql import SparkSession
from  pyspark.sql.functions import regexp_replace
from  pyspark.sql.functions import *
from pyspark.sql import types
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import substring
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import to_date

#Definicacao do Spark Session
spark = SparkSession \
    .builder \
    .appName("Confitec") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
	
 #Leitura do arquivo Parquet
df = spark.read.parquet('/content/drive/MyDrive/Confitec/OriginaisNetflix.parquet')

#Questão 1 - Transformar  os campos Premiere e dt_Inclusao para datetime
df = df.withColumn('mes', substring('Premiere', -6,3)).withColumn('ano', substring('Premiere', -2,3)).withColumn('dia', substring('Premiere', 1,2))
df = df.withColumn("dia", regexp_replace(col("dia"), """-""", ""))

meses = {'Jan': '01',
         'Feb': '02',
         'Mar': '03',
         'Apr': '04',
         'May': '05',
         'Jun': '06',
         'Jul': '07',
         'Aug': '08',
         'Sep': '09',
         'Oct': '10',
         'Nov': '11',
         'Dec': '12'}

df = df.replace(meses,subset=["mes"])
df  = df.withColumn("Premiere",concat(lit("20"),col("ano"),lit("-"),col("mes"),lit("-"),col("dia")))

df = df.withColumn("Premiere",date_format('Premiere', 'yyy-MM-dd'))
df.withColumn("dt_inclusao",to_date(col("dt_inclusao")))
df =  df.withColumn("Premiere",to_date(col("dt_inclusao")))

#Questão 2 - Ordenar por ativos d genero de forma descrecente
df = df.sort(col("Active").desc(),col("Genre").asc())

#Questão 3 - Remoção das duplicatas e troca do resultado TBA por "a ser anunciado"
df = df.distinct()
df = df.withColumn("Seasons", regexp_replace(col("Seasons"), "TBA", "a ser anunciado"))

#Questão 4 - Criação da coluna data de alteração como timestamp
df = df.withColumn('Data de Alteração',current_timestamp())

#Questão 5 - Troca dos nomes das colunas de ingles para Português
df = df.select(
    col("Title").alias("Título"),
    col("Genre").alias("Gênero"),
    col("GenreLabels").alias("Gênero Por Classificação"),
    col("Premiere").alias("Pré Estréia"),
    col("Seasons").alias("Temporadas"),
    col("SeasonsParsed").alias("Avaliação da Temporada"),
    col("EpisodesParsed").alias("Avaliação do Episódio"),
    col("Length").alias("Duração"),
    col("MinLength").alias("Mínima Duração"),
    col("MaxLength").alias("Máxima Duração"),
    col("Status").alias("Situação"),
    col("Active").alias("Ativo"),
    col("Table").alias("Tipo de Gênero"),
    col("Language").alias("Idioma"),
    col("dt_inclusao").alias("Data de Inclusão"),
    col("Data de Alteração")
)

#Salvar o arquivo no formato csv 
df.write.option("header",True).option("delimiter",";").csv("/content/drive/MyDrive/Confitec/OriginaisNetflix")

#URl do S3 s3://confitec/OriginalsNetflix.csv


