import os
import pendulum
from datetime import timedelta
import queue
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

# default_args = {
#     'owner': 'airflow',
#     'start_date': pendulum.today('UTC').add(days=2),
#     'retries': 4,
#     'retry_delay': timedelta(minutes=5),
#     'provide_context': True,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'catchup': False}

igfportal_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfportal_server_hostname'))

igf_lims_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igf_lims_server_hostname'))

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
  DAG(
    dag_id=DAG_ID,
    catchup=False,
    schedule="0 4 * * *",
    dagrun_timeout=timedelta(minutes=10),
    max_active_runs=1,
    tags=['igf-lims','igfportal', 'docker-host'],
    start_date=pendulum.yesterday(),
    provide_context=True,
    owner='airflow',)

with dag:
  ## TASK
  cleanup_igf_lims = \
    SSHOperator(
      task_id='cleanup_igf_lims',
      dag=dag,
      retry_delay=timedelta(minutes=5),
      retries=4,
      ssh_hook=igf_lims_ssh_hook,
      pool='igf_lims_ssh_pool',
      queue='hpc_4G',
      command="bash /home/igf/scripts/docker_prune_script.sh ")

  ## TASK
  cleanup_igf_portal = \
    SSHOperator(
      task_id='cleanup_igf_portal',
      dag=dag,
      retry_delay=timedelta(minutes=5),
      retries=4,
      ssh_hook=igfportal_ssh_hook,
      pool='igfportal_ssh_pool',
      queue='hpc_4G',
      command="/home/igf/scripts/docker_prune_script.sh ")

  ## PIPELNE
  cleanup_igf_lims >> cleanup_igf_portal