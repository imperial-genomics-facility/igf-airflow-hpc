from datetime import timedelta
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

## ARGS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

## DAG
dag = \
  DAG(
    dag_id='dag4_lims_metadata',
    catchup=False,
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['hpc'],
    default_args=default_args)

with dag:
  ## TASK
  submit_metadata_fetch_job = \
    BashOperator(
      task_id='submit_metadata_fetch_job',
      dag=dag,
      xcom_push=False,
      queue='hpc_4G',
      bash_command='bash /rds/general/user/igf/home/git_repo/IGF-cron-scripts/hpc/lims_metadata/fetch_lims_metadata_qsub.sh ')

  ## PIPELINE
  submit_metadata_fetch_job