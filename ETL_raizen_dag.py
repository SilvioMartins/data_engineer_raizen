
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
from airflow.operators.docker_operator import DockerOperator
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

def trasnform_final():
    raizen_transform.Dframe_final(path_bronze_zone+'\\'+file_xlsx_name)

#Construção DAG
default_args = {
    'owner': 'Silvio Martins',
    'start_date': days_ago(1),
    'email': ['scesar.martins@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1)
}

with DAG(
    'ETL_raizen',
    default_args=default_args,
    description='Desafio Raizen engenharia de Dados',
    schedule_interval=datetime.timedelta(days=1),
    max_active_runs=1
) as dag:

    file_dest_name = 'sales_anp.xls'
    file_xlsx_name = 'sales_anp.xlsx'
    path_bronze_zone = '/opt/airflow/dags'
    path_silver_zone = '<Path da Silver_Zone>'
    path_gold_zone = '<Path da Gold_Zone>'

    
    extraction_xls_origin = PythonOperator(
        task_id="extraction_xls_origin",
        python_callable=file_raw_extract
    )

    convertion_file_xls_xlsx = DockerOperator(
        task_id='convertion_file_xls_xlsx',
        image='ipunktbs/docker-libreoffice-headless:latest',
        api_version='auto',
        auto_remove=True,
        command='libreoffice --invisible --headless --convert-to xlsx /opt/airflow/dags/sales_anp.xls --outdir /opt/airflow/dags/sales_anp.xls',
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[Mount(source=path_bronze_zone,target=path_bronze_zone, type="bind")]
    )

    trasnform_step = PythonOperator(
        task_id="trasnform_final",
        python_callable=trasnform_final
    )

extraction_xls_origin >> convertion_file_xls_xlsx
convertion_file_xls_xlsx >> trasnform_step