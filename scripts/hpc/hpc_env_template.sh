source /etc/bashrc

module load anaconda3/personal
source activate airflow1.10.12

export AIRFLOW__WEBSERVER__RBAC=True
export AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE='Europe/London'
export AIRFLOW__WEBSERVER__WEB_SERVER_SSL_CERT=/SSL/airflow.cert
export AIRFLOW__WEBSERVER__WEB_SERVER_SSL_KEY=/SSL/airflow.key
export AIRFLOW__WEBSERVER__DAG_ORIENTATION=TB
export AIRFLOW__SCHEDULER__RUN_DURATION=3600
export AIRFLOW__SCHEDULER__MAX_THREADS=4
export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY=/rds/general/user/igf/ephemeral/airflow_logs/logs/scheduler
export AIRFLOW__CORE__BASE_LOG_FOLDER=/rds/general/user/igf/ephemeral/airflow_logs/logs
export AIRFLOW__CORE__DAG_PROCESSOR_MANAGER_LOG_LOCATION=/rds/general/user/igf/ephemeral/airflow_logs/logs/dag_processor_manager/dag_processor_manager.log
export AIRFLOW__CORE__LOGGING_LEVEL=WARN
export AIRFLOW__CORE__DAGS_FOLDER=/rds/general/user/igf/home/data2/airflow_test/github/igf-airflow-hpc/dags
export AIRFLOW__CORE__PLUGIN_FOLDER=/rds/general/user/igf/home/data2/airflow_test/plugin
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=''
export AIRFLOW__CORE__DEFAULT_TIMEZONE='Europe/London'
export AIRFLOW__CORE__PARALLELISM=100
export AIRFLOW__CORE__DAG_CONCURRENCY=10
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export AIRFLOW__CORE__NON_POOLED_TASK_SLOT_COUNT=10
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=30
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CORE__DEFAULT_TASK_RETRIES=4
export AIRFLOW__CORE__STORE_SERIALIZED_DAGS=True
export AIRFLOW__CELERY__WORKER_CONCURRENCY=1
export AIRFLOW__CELERY__BROKER_URL=''
export AIRFLOW__CELERY__RESULT_BACKEND=''
export AIRFLOW__CELERY__FLOWER_BASIC_AUTH=''
export AIRFLOW__CELERY__BROKER_TRANSPORT_OPTIONS__VISIBILITY_TIMEOUT=86400
export AIRFLOW__SECRETS__BACKEND=airflow.secrets.local_filesystem.LocalFilesystemBackend
export AIRFLOW__SECRETS__BACKEND_KWARGS='{"variables_file_path":"/rds/general/user/igf/home/data2/airflow_test/secrets/var.json","connections_file_path":"/rds/general/user/igf/home/data2/airflow_test/secrets/conn.json"}'
export FERNET_KEY=''
export PYTHONPATH=/rds/general/user/igf/home/data2/airflow_test/github/igf-airflow-hpc:$PYTHONPATH