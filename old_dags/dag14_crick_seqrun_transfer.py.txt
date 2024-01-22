from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import check_and_transfer_run_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import extract_tar_file_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import find_and_split_md5_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import validate_md5_chunk_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import check_and_divide_run_for_remote_copy_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import copy_run_file_to_remote_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import run_interop_dump_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import generate_interop_report_func
from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import send_message_to_channels_with_mention_func

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

dag = \
  DAG(
    dag_id='dag14_crick_seqrun_transfer',
    schedule_interval=None,
    default_args=args,
    orientation='LR',
    tags=['ftp', 'hpc'])

with dag:
  ## TASK
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
      queue='hpc_4G_long',
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
        task_id='md5_validate_chunk_{0}'.format(chunk_id),
        dag=dag,
        queue='hpc_4G',
        params={'chunk_id': chunk_id,
                'xcom_task': 'find_and_split_md5',
                'xcom_key': 'md5_file_chunk'},
        python_callable=validate_md5_chunk_func)
    ## PIPELINE
    find_and_split_md5 >> t
  ## TASK
  check_and_divide_run_for_remote_copy = \
    BranchPythonOperator(
      task_id='check_and_divide_run_for_remote_copy',
      dag=dag,
      queue='hpc_4G',
      python_callable=check_and_divide_run_for_remote_copy_func)
  ## PIPELINE
  extract_tar_file >> check_and_divide_run_for_remote_copy
  ## TASK
  send_message_to_channels_with_mention = \
    PythonOperator(
      task_id='send_message_to_channels_with_mention',
      dag=dag,
      queue='hpc_4G',
      python_callable=send_message_to_channels_with_mention_func)
  ## PIPELINE
  # extract_tar_file >> send_message_to_channels_with_mention
  ## TASK
  for i in range(1, 9):
    t = \
      PythonOperator(
        task_id='copy_bcl_to_remote_for_lane{0}'.format(i),
        dag=dag,
        pool='wells_scp_pool',
        queue='hpc_4G',
        params={'lane_id': i,
                'xcom_task': 'check_and_divide_run_for_remote_copy',
                'xcom_key': 'bcl_files'},
        python_callable=copy_run_file_to_remote_func)
    ## PIPELINE
    check_and_divide_run_for_remote_copy >> t >> send_message_to_channels_with_mention
  copy_additional_file_to_remote = \
    PythonOperator(
      task_id='copy_additional_file_to_remote',
      dag=dag,
      pool='wells_scp_pool',
      queue='hpc_4G',
      params={'xcom_task': 'check_and_divide_run_for_remote_copy',
              'xcom_key': 'additional_files'},
      python_callable=copy_run_file_to_remote_func)
  ## PIPELINE
  check_and_divide_run_for_remote_copy >> copy_additional_file_to_remote
  ## TASK
  generate_interop_dump = \
    PythonOperator(
      task_id='generate_interop_dump',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_interop_dump_func)
  generate_interop_report = \
    PythonOperator(
      task_id='generate_interop_report',
      dag=dag,
      queue='hpc_4G',
      params={'interop_dump_xcom_task': 'generate_interop_dump',
              'timeout': 1200,
              'kernel_name': 'python3'},
      python_callable=generate_interop_report_func)
  ## PIPELINE
  extract_tar_file >> generate_interop_dump >> generate_interop_report
