from datetime import timedelta
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import check_and_transfer_run_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import extract_tar_file_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import find_and_split_md5_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import validate_md5_chunk_func

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 4,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 5,
}

## SSH HOOK
orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('orwell_server_hostname'))

dag = \
  DAG(
    dag_id='dag14_crick_seqrun_transfer',
    schedule_interval=None,
    default_args=args,
    tags=['ftp', 'hpc', 'orwell', 'wells'])


with dag:
  ## TASK
  # not working on HPC
  check_and_transfer_run = \
    PythonOperator(
      task_id='check_and_transfer_run',
      dag=dag,
      pool='crick_ftp_pool',
      queue='hpc_4G',
      python_callable=check_and_transfer_run_func)
  ## TASK
  extract_tar_file = \
    PythonOperator(
      task_id='extract_tar_file',
      dag=dag,
      queue='hpc_4G',
      python_callable=extract_tar_file_func)
  ## TASK
  find_and_split_md5 = \
    BranchPythonOperator(
      task_id='find_and_split_md5',
      dag=dag,
      queue='hpc_4G',
      params={'split_count': 50},
      python_callable=find_and_split_md5_func)
  ## PIPELINE
  check_and_transfer_run >> extract_tar_file >> find_and_split_md5
  for chunk_id in range(0, 50):
    t = \
      PythonOperator(
        task_id='md5_validate_chunk_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'chunk_id': chunk_id,
                'xcom_task': 'find_and_split_md5',
                'xcom_key': 'md5_file_chunk'},
        python_callable=validate_md5_chunk_func)
    ## PIPELINE
    find_and_split_md5 >> t
