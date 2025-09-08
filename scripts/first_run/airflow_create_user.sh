#!/bin/bash
docker run -it --rm \
--env-file /home/igf/airflow_test/secrets/airflow_env \
-v /home/igf/airflow_test/logs:/rds/general/user/igf/home/data2/airflow_test/logs:z \
-v /home/igf/airflow_test/github/igf-airflow-hpc:/rds/general/user/igf/home/data2/airflow_test/github/igf-airflow-hpc:z \
-v /home/igf/airflow_test/plugin:/rds/general/user/igf/home/data2/airflow_test/plugin:z \
apache/airflow:1.10.12 create_user --username USERNAME --firstname FIRSTNAME --lastname LASTNAME --role Admin --email EMAIL --password PASSWORD