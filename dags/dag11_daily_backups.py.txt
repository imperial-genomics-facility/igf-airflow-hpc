from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

## SSH HOOK
igf_lims_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igf_lims_server_hostname'))

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
    tags=['hpc', "backup"],
    default_args=default_args)

with dag:
  ## TASK
  backup_prod_db = \
    BashOperator(
      task_id='backup_prod_db',
      dag=dag,
      queue='hpc_4G',
      bash_command='bash /rds/general/user/igf/home/secret_keys/get_dump.sh ')
  ## TASK
  backup_portal_db = \
    SSHOperator(
      task_id='backup_portal_db',
      dag=dag,
      ssh_hook=igf_lims_ssh_hook,
      queue='hpc_4G',
      pool='igf_lims_ssh_pool',
      command="bash /home/igf/igf_portal/github/backup_cmd.sh ")
  ## TASK
  copy_portal_backup_to_hpc = \
    BashOperator(
      task_id="copy_portal_backup_to_hpc",
      dag=dag,
      queue='hpc_4G',
      bash_command="bash /rds/general/user/igf/home/secret_keys/copy_portal_dump.sh ")

  ## PIPELINE
  backup_prod_db >> backup_portal_db >> copy_portal_backup_to_hpc