FROM apache/airflow:2.2.3

USER airflow
RUN pip3 install openpyxl 

USER root
RUN apt-get update && \
    apt-get -y install apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common
RUN curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey
RUN apt-key add /tmp/dkey && \
    add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
    $(lsb_release -cs) \
    stable"
RUN apt-get update && \
    apt-get -y install docker-ce
COPY --chown=airflow:root ./dags/ETL_raizen_dag.py /opt/airflow/dags/
COPY --chown=airflow:root ./dags/raizen_extract.py /opt/airflow/dags/
COPY --chown=airflow:root ./dags/raizen_transform.py /opt/airflow/dags/