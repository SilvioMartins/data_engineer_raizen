mkdir dags logs plugins dags/data_lake/bronze_zone dags/data_lake/silver_zone dags/data_lake/gold_zone
chmod -R 777 dags logs plugins dags/data_lake/bronze_zone dags/data_lake/silver_zone dags/data_lake/gold_zone /opt/airflow/dags
docker image build -t airflow-review-01:0.1 .
docker-compose up -d --scale worker=3