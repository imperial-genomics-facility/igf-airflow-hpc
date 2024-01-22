FROM apache/airflow:2.6.2-python3.9
LABEL maintainer="imperialgenomicsfacility"
LABEL version="Airflow_v2.6.2"
LABEL description="Docker image for IGF Airflow"
ADD requirements_v2.6.2.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
