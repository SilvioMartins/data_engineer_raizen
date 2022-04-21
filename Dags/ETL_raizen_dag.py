
###########################################################
# OBJETIVO:  Construção Pipeline airflow
# DATA: 06Abr2022
# OWNER: Silvio Martins
###########################################################

#Importanndo Bibliotecas
import datetime
from sys import api_version
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
import pandas as pd
import requests
import os

#Importando dependências
import raizen_extract
import raizen_transform

#Funções tasks

#Extração do arquivo XLS da origem (Site ANP) e armazena na área BRONZE do DataLake 
def file_raw_extract():
    raizen_extract.extract_xls_origin(path_bronze_zone,file_dest_name)

#Função Transformação de dados para Oil_Derivate
def transform_oil_derivate():
    raizen_transform.process_transform(
        path_silver_zone+'//'+file_xlsx_name,
        'DPCache_m3',
        path_gold_zone+'/oil_derivate.parquet'
        )

#Função Transformação de dados para Diesel
def transform_diesel():
    raizen_transform.process_transform(
        path_silver_zone+'//'+file_xlsx_name,
        'DPCache_m3_2',
        path_gold_zone+'/diesel.parquet'
        )
    
#Parâmetros da DAG
default_args = {
    'owner': 'Silvio Martins',
    'start_date': days_ago(1),
    'email': ['scesar.martins@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

#Construção DAG
with DAG(
    'ETL_raizen',
    default_args=default_args,
    description='Desafio Raizen Engenharia de Dados',
    schedule_interval=datetime.timedelta(days=1),
    max_active_runs=1
) as dag:

    #Definindo Variáveis Globais
    file_dest_name = 'sales_anp.xls'
    file_xlsx_name = 'sales_anp.xlsx'
    path_bronze_zone = '/opt/airflow/dags/data_lake/bronze_zone'
    path_silver_zone = '/opt/airflow/dags/data_lake/silver_zone'
    path_gold_zone = '/opt/airflow/dags/data_lake/gold_zone'
    flag_check = [True,True]
    
    #Task Extração de dados da fonte
    extraction_xls_origin = PythonOperator(
        task_id="extraction_xls_origin",
        python_callable=file_raw_extract,
        dag = dag,
    )

    #Task Conversão xls --> xlsx
    convertion_file_xls_xlsx = BashOperator(
        task_id='convertion_file_xls_xlsx',
        bash_command= (
            'cp /opt/airflow/dags/data_lake/bronze_zone/sales_anp.xls /tmp && '
            'docker run --rm -v /tmp:/tmp --name libre-headless ipunktbs/docker-libreoffice-headless:latest --convert-to xlsx "sales_anp.xls" && '
            'cp /tmp/sales_anp.xlsx  /opt/airflow/dags/data_lake/silver_zone '
        ),
        dag = dag,
    )

    #Task Transformação e criação de dados Oil_derivate
    transform_oil_derivate = PythonOperator(
        task_id="transform_oil_derivate",
        python_callable=transform_oil_derivate,
        dag = dag,
    )

    #Task Transformação e criação de dados Diesel
    transform_diesel = PythonOperator(
        task_id="transform_diesel",
        python_callable=transform_diesel,
        dag = dag,
    )


extraction_xls_origin >> convertion_file_xls_xlsx >> transform_oil_derivate >>  transform_diesel