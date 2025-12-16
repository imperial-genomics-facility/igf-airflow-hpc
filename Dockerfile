FROM apache/airflow:2.10.4
LABEL maintainer="imperialgenomicsfacility"
LABEL version="Airflow_v2.10.4"
LABEL description="Docker image for IGF Airflow"
USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install --no-install-recommends --fix-missing -y git tzdata && \
    apt-get purge -y --auto-remove && \
    apt-get clean && \
    rm -f /tmp/* && \
    rm -rf /var/lib/apt/lists/*
ENV TZ=Europe/London
RUN ln -sf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
USER airflow
WORKDIR /rds/general/project/genomics-facility-archive-2019/live/AIRFLOW/airflow_v4/github/
RUN git clone -b 'v2.5.1' --single-branch --depth 1 https://github.com/imperial-genomics-facility/data-management-python.git
WORKDIR /tmp/igf-airflow-hpc
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /rds/general/project/genomics-facility-archive-2019/live/AIRFLOW/airflow_v4/github/
ENV PYTHONPATH=/rds/general/project/genomics-facility-archive-2019/live/AIRFLOW/airflow_v4/github/data-management-python
EXPOSE 8083
EXPOSE 5555
