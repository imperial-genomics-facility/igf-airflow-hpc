from datetime import timedelta
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

## ARGS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

## DAG
dag = \
  DAG(
    dag_id='dag6_seqrun_processing',
    catchup=False,
    schedule_interval="@hourly",
    max_active_runs=1,
    tags=['hpc','orwell'],
    default_args=default_args)

## SSH HOOK
orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host='orwell.hh.med.ic.ac.uk')

hpc_hook = SSHHook(ssh_conn_id='hpc_conn')

with dag:
  ## TASK
  switch_off_project_barcode = \
    SSHOperator(
      task_id='switch_off_project_barcode',
      dag=dag,
      ssh_hook=orwell_ssh_hook,
      queue='hpc_4G',
      command='bash /home/igf/igf_code/IGF-cron-scripts/orwell/switch_off_project_barcode_check.sh ')
  ## TASK
  change_samplesheet_for_run = \
    SSHOperator(
      task_id='change_samplesheet_for_run',
      dag=dag,
      queue='hpc_4G',
      ssh_hook=orwell_ssh_hook,
      command='bash /home/igf/igf_code/IGF-cron-scripts/orwell/change_samplesheet_for_seqrun.sh ')
  ## TASK
  restart_seqrun_processing = \
    SSHOperator(
      task_id='restart_seqrun_processing',
      dag=dag,
      queue='hpc_4G',
      ssh_hook=orwell_ssh_hook,
      command='bash /home/igf/igf_code/IGF-cron-scripts/orwell/restart_seqrun_processing.sh ')
  ## TASK
  register_project_metadata = \
    SSHOperator(
      task_id='register_project_metadata',
      dag=dag,
      queue='hpc_4G',
      ssh_hook=orwell_ssh_hook,
      command='bash /home/igf/igf_code/IGF-cron-scripts/orwell/register_metadata.sh ')
  ## TASK
  find_new_seqrun = \
    SSHOperator(
      task_id='find_new_seqrun',
      dag=dag,
      queue='hpc_4G',
      ssh_hook=orwell_ssh_hook,
      command='bash /home/igf/igf_code/IGF-cron-scripts/orwell/find_new_seqrun.sh ')
  ## TASK
  seed_demultiplexing_pipe = \
    SSHOperator(
      task_id='seed_demultiplexing_pipe',
      dag=dag,
      ssh_hook=hpc_hook,
      queue='hpc_4G',
      command='bash /rds/general/user/igf/home/git_repo/IGF-cron-scripts/hpc/seed_demultiplexing_pipeline.sh ')

  ## PIPELINE
  switch_off_project_barcode >> change_samplesheet_for_run >> restart_seqrun_processing
  restart_seqrun_processing >> register_project_metadata >> find_new_seqrun >> seed_demultiplexing_pipe