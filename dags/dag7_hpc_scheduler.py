from datetime import timedelta

from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = \
  DAG(
    dag_id='dag7_hpc_scheduler',
    catchup=False,
    schedule_interval=None,
    max_active_runs=1,
    tags=['hpc','igf-lims'],
    default_args=default_args)

hpc_hook = SSHHook(ssh_conn_id='hpc_conn')

with dag:
  run_hpc_scheduler = \
    SSHOperator(
      task_id = 'run_hpc_scheduler',
      dag = dag,
      ssh_hook = hpc_hook,
      queue='igf-lims',
      command = 'source /etc/bashrc;qsub /project/tgu/data2/airflow_test/github/igf-airflow-hpc/scripts/hpc/run_hpc_scheduler.sh '
    )
  run_hpc_scheduler