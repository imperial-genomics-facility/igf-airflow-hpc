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

orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host='orwell.hh.med.ic.ac.uk')
hpc_hook = SSHHook(ssh_conn_id='hpc_conn')

dag = \
  DAG(
    dag_id='dag5_primary_analysis_and_qc_processing',
    schedule_interval=None,
    max_active_runs=1,
    default_args=default_args)

with dag:
  update_exp_metadata = \
    BashOperator(
      task_id = 'update_exp_metadata',
      dag = dag,
      queue='hpc_4G',
      bash_command = 'bash /rds/general/user/igf/home/git_repo/IGF-cron-scripts/hpc/update_exp_metadata.sh '
    )

  find_new_exp_for_analysis = \
    SSHOperator(
      task_id = 'find_new_exp_for_analysis',
      dag = dag,
      ssh_hook = orwell_ssh_hook,
      queue='hpc_4G',
      command = 'bash /home/igf/igf_code/IGF-cron-scripts/orwell/find_new_exp_for_analysis.sh '
    )

  seed_analysis_pipeline = \
    SSHOperator(
      task_id = 'seed_analysis_pipeline',
      dag = dag,
      ssh_hook=hpc_hook,
      queue='hpc_4G',
      command = 'bash /rds/general/user/igf/home/git_repo/IGF-cron-scripts/hpc/seed_analysis_pipeline.sh '
    )

  update_exp_metadata >> find_new_exp_for_analysis >> seed_analysis_pipeline