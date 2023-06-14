FROM apache/airflow:2.6.1-python3.9
LABEL maintainer="imperialgenomicsfacility"
LABEL version="scanpy_v1.9.3"
LABEL description="Docker image for scanpy tools"
ADD requirements_v2.6.1.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
