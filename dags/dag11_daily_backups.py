from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

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
    dag_id='dag11_daily_backups',
    catchup=False,
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['hpc'],
    default_args=default_args)

with dag:
  ## TASK
  backup_prod_db = \
    BashOperator(
      task_id='backup_prod_db',
      dag=dag,
      xcom_push=False,
      queue='hpc_4G',
      bash_command='bash /rds/general/user/igf/home/secret_keys/get_dump.sh ')

  ## PIPELINE
  backup_prod_db