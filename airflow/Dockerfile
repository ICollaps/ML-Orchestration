# FROM apache/airflow:2.9.2
# USER root
# RUN apt-get update && apt-get install -y libgomp1
# USER airflow
# COPY requirements.txt /
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt


FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y libgomp1

# Vérifiez si le groupe et l'utilisateur airflow existent avant de les créer
RUN if ! getent group airflow; then groupadd -r airflow; fi \
    && if ! id -u airflow > /dev/null 2>&1; then useradd -r -g airflow airflow; fi

# Créez le répertoire mlflow avec les bonnes permissions
RUN mkdir -p /mlflow/mlruns && chown -R airflow:airflow /mlflow

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt


