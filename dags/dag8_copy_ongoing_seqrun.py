from datetime import timedelta
import os,json,logging
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.dummy_operator import DummyOperator
from igf_airflow.seqrun.ongoing_seqrun_processing import fetch_ongoing_seqruns
from igf_airflow.logging.upload_log_msg import send_log_to_channels,log_success,log_failure,log_sleep
from igf_data.utils.fileutils import get_temp_dir,copy_remote_file,check_file_path,read_json_data


## DEFAULT ARGS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}


## CONN HOOKS
orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('seqrun_server'))


## DAG
dag = \
  DAG(
    dag_id='dag8_copy_ongoing_seqrun',
    catchup=False,
    schedule_interval="0 */2 * * *",
    max_active_runs=1,
    tags=['hpc'],
    default_args=default_args)


## FUNCTIONS
def get_ongoing_seqrun_list(context):
  """
  A function for fetching ongoing sequencing run ids
  """
  try:
    ti = context.get('ti')
    seqrun_server = Variable.get('seqrun_server')
    seqrun_base_path = Variable.get('seqrun_base_path')
    database_config_file = Variable.get('database_config_file')
    ongoing_seqruns = \
      fetch_ongoing_seqruns(
        seqrun_server=seqrun_server,
        seqrun_base_path=seqrun_base_path,
        database_config_file=database_config_file)
    ti.xcom_push(key='ongoing_seqruns',value=ongoing_seqruns)
    branch_list = ['generate_seqrun_file_list_{0}'.format(i[0]) 
                     for i in enumerate(ongoing_seqruns)]
    if len(branch_list) == 0:
      branch_list = ['no_ongoing_seqrun']
    else:
      send_log_to_channels(
        slack_conf=Variable.get('slack_conf'),
        ms_teams_conf=Variable.get('ms_teams_conf'),
        task_id=context['task'].task_id,
        dag_id=context['task'].dag_id,
        comment='Ongoing seqruns found: {0}'.format(ongoing_seqruns),
        reaction='pass')
    return branch_list
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')


def copy_seqrun_manifest_file(context):
  """
  A function for copying filesize manifest for ongoing sequencing runs to hpc 
  """
  try:
    remote_file_path = context['params'].get('file_path')
    seqrun_server = context['params'].get('seqrun_server')
    xcom_pull_task_ids = context['params'].get('xcom_pull_task_ids')
    ti = context.get('ti')
    remote_file_path = ti.xcom_pull(task_ids=xcom_pull_task_ids)
    tmp_work_dir = get_temp_dir(use_ephemeral=True)
    local_file_path = \
      os.path.join(
        tmp_work_dir,
        os.path.basename(remote_file_path))
    copy_remote_file(
      remote_file_path,
      local_file_path,
      source_address=seqrun_server)
    return local_file_path
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')


def get_seqrun_chunks(context):
  """
  A function for setting file chunk size for seqrun files copy
  """
  try:
    ti = context.get('ti')
    worker_size = context['params'].get('worker_size')
    child_task_prefix = context['params'].get('child_task_prefix')
    seqrun_chunk_size_key = context['params'].get('seqrun_chunk_size_key')
    xcom_pull_task_ids = context['params'].get('xcom_pull_task_ids')
    file_path = ti.xcom_pull(task_ids=xcom_pull_task_ids)
    check_file_path(file_path)
    file_data = read_json_data(file_path)
    chunk_size = None
    if worker_size is None or \
       worker_size == 0:
      raise ValueError(
              'Incorrect worker size: {0}'.\
                format(worker_size))
    if len(file_data) == 0:
      raise ValueError(
              'No data present in seqrun list file {0}'.\
                format(file_path))
    if len(file_data) < int(5 * worker_size):
      worker_size = 1                                                           # setting worker size to 1 for low input
    if len(file_data) % worker_size == 0:
      chunk_size = int(len(file_data) / worker_size)
    else:
      chunk_size = int(len(file_data) / worker_size)+1
    ti.xcom_push(key=seqrun_chunk_size_key,value=chunk_size)
    worker_branchs = ['{0}_{1}'.format(child_task_prefix,i) 
                        for i in range(worker_size)]
    return worker_branchs
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')


def copy_seqrun_chunk(context):
  """
  A function for copying seqrun chunks
  """
  try:
    ti = context.get('ti')
    file_path_task_ids = context['params'].get('file_path_task_ids')
    seqrun_chunk_size_key = context['params'].get('seqrun_chunk_size_key')
    seqrun_chunk_size_task_ids = context['params'].get('seqrun_chunk_size_task_ids')
    chunk_index_number = context['params'].get('chunk_index_number')
    run_index_number = context['params'].get('run_index_number')
    local_seqrun_path = context['params'].get('local_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    seqrun_server = Variable.get('seqrun_server'),
    seqrun_base_path = Variable.get('seqrun_base_path')
    seqrun_id = ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    file_path = ti.xcom_pull(task_ids=file_path_task_ids)
    chunk_size = ti.xcom_pull(key=seqrun_chunk_size_key,task_ids=seqrun_chunk_size_task_ids)
    check_file_path(file_path)
    file_data = read_json_data(file_path)
    start_index = chunk_index_number*chunk_size
    finish_index = ((chunk_index_number+1)*chunk_size) - 1
    if finish_index > len(file_data) - 1:
      finish_index = len(file_data) - 1
    local_seqrun_path = \
      os.path.join(local_seqrun_path,seqrun_id)
    for entry in file_data[start_index:finish_index]:
      file_path = entry.get('file_path')
      file_size = entry.get('file_size')
      remote_path = \
        os.path.join(
          seqrun_base_path,
          file_path)
      local_path = \
        os.path.join(
          local_seqrun_path,
          file_path)
      if os.path.exists(local_path) and \
         os.path.getsize(local_path) == file_size:
        pass
      else:
        copy_remote_file(
          remote_path,
          local_path,
          source_address=seqrun_server,
          check_file=False)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')


with dag:
  ## TASK
  generate_seqrun_list = \
    BranchPythonOperator(
      task_id='generate_seqrun_list',
      dag=dag,
      queue='hpc_4G',
      python_callable=get_ongoing_seqrun_list)
  ## TASK
  no_ongoing_seqrun = \
    DummyOperator(
      task_id='no_ongoing_seqrun',
      dag=dag,
      queue='hpc_4G',
      on_success_callback=log_sleep)
  ## TASK
  tasks = list()
  for i in range(5):
    t1 = \
      SSHOperator(
        task_id='generate_seqrun_file_list_{0}'.format(i),
        dag=dag,
        pool='orwell_exe_pool',
        do_xcom_push=True,
        queue='hpc_4G',
        params={'source_task_id':'generate_seqrun_list',
                'pull_key':'ongoing_seqruns',
                'index_number':i},
        command="""
          source /home/igf/igf_code/airflow/env.sh; \
          python /home/igf/igf_code/airflow/data-management-python/scripts/seqrun_processing/create_file_list_for_ongoing_seqrun.py \
            --seqrun_base_dir /home/igf/seqrun/illumina \
            --output_path /home/igf/ongoing_run_tracking \
            --seqrun_id {{ ti.xcom_pull(key=params.pull_key,task_ids=params.source_task_id)[ params.index_number ] }}
          """)
    ## TASK
    t2 = \
      PythonOperator(
        task_id='copy_seqrun_file_list_{0}'.format(i),
        dag=dag,
        pool='orwell_scp_pool',
        queue='hpc_4G',
        params={'xcom_pull_task_ids':'generate_seqrun_file_list_{0}'.format(i),
                'seqrun_server':Variable.get('seqrun_server')},
        python_callable=copy_seqrun_manifest_file)
    ## TASK
    t3 = \
      BranchPythonOperator(
        task_id='decide_copy_branch_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'xcom_pull_task_ids':'copy_seqrun_file_list_{0}'.format(i),
                'worker_size':10,
                'seqrun_chunk_size_key':'seqrun_chunk_size',
                'child_task_prefix':'copy_file_run_{0}_chunk_'.format(i)},
        python_callable=get_seqrun_chunks)
    ## TASK
    t4 = list()
    for j in range(10):
      t4j = \
        PythonOperator(
          task_id='copy_file_run_{0}_chunk_{1}'.format(i,j),
          dag=dag,
          queue='hpc_4G',
          pool='orwell_scp_pool',
          params={'file_path_task_ids':'copy_seqrun_file_list_{0}'.format(i),
                  'seqrun_chunk_size_key':'seqrun_chunk_size',
                  'seqrun_chunk_size_task_ids':'decide_copy_branch_{0}'.format(i),
                  'run_index_number':i,
                  'chunk_index_number':j,
                  'seqrun_id_pull_key':'ongoing_seqruns',
                  'seqrun_id_pull_task_ids':'generate_seqrun_list',
                  'local_seqrun_path':Variable.get('hpc_seqrun_path')},
          python_callable=copy_seqrun_chunk)
      t4.append(t4j)
    tasks.append([ t1 >> t2 >> t3 >> t4 ])

  ## PIPELINE
  generate_seqrun_list >> no_ongoing_seqrun
  generate_seqrun_list >> tasks
