import os
import pendulum
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

## ARG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': pendulum.today('UTC').add(days=2),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
  DAG(
    dag_id=DAG_ID,
    schedule="*/30 * * * *",
    dagrun_timeout=timedelta(minutes=10),
    max_active_runs=1,
    catchup=False,
    tags=['igf-lims','hpc'],
    start_date=pendulum.yesterday(),
    dagrun_timeout=timedelta(minutes=10),
    )

## SSH HOOK
hpc_hook = SSHHook(ssh_conn_id='hpc_conn')

igf_lims_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igf_lims_server_hostname'))

igfportal_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfportal_server_hostname'))

wells_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('wells_server_hostname'))

with dag:
  ## TASK
  run_hpc_scheduler = \
    SSHOperator(
      task_id='run_hpc_scheduler',
      dag=dag,
      retry_delay=timedelta(minutes=5),
      retries=1,
      ssh_hook=hpc_hook,
      pool='generic_pool',
      queue='generic',
      command="""
        source /etc/bashrc; \
        qsub /project/tgu/data2/airflow_v2/github/data-management-python/scripts/hpc/run_hpc_scheduler.sh """)

  ## TASK
  restart_flower_server = \
    SSHOperator(
      task_id='restart_flower_server',
      dag=dag,
      retry_delay=timedelta(minutes=5),
      retries=1,
      ssh_hook=igf_lims_ssh_hook,
      pool='generic_pool',
      queue='hpc_4G',
      command="docker restart airflow_flower_v2")

  ## TASK
  # restart_portal_flower_server = \
  #   SSHOperator(
  #     task_id='restart_portal_flower_server',
  #     dag=dag,
  #     ssh_hook=igf_lims_ssh_hook,
  #     pool='generic_pool',
  #     queue='hpc_4G',
  #     command="docker restart celery_flower")
  restart_portal_flower_server = \
    SSHOperator(
      task_id='restart_portal_flower_server',
      dag=dag,
      retry_delay=timedelta(minutes=5),
      retries=1,
      ssh_hook=igfportal_ssh_hook,
      pool='igfportal_ssh_pool',
      queue='hpc_4G',
      command="docker restart celery_flower")

  ## PIPELNE
  run_hpc_scheduler >> restart_flower_server >> restart_portal_flower_server