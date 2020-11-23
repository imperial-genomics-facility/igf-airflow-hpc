from datetime import timedelta
import os,json,logging,subprocess
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.dummy_operator import DummyOperator
from igf_airflow.seqrun.ongoing_seqrun_processing import fetch_ongoing_seqruns
from igf_airflow.seqrun.ongoing_seqrun_processing import compare_existing_seqrun_files
from igf_airflow.seqrun.ongoing_seqrun_processing import check_for_sequencing_progress
from igf_airflow.logging.upload_log_msg import send_log_to_channels,log_success,log_failure,log_sleep
from igf_data.utils.fileutils import get_temp_dir,copy_remote_file,check_file_path,read_json_data
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import get_ongoing_seqrun_list
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import copy_seqrun_manifest_file
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import reset_manifest_file
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import get_seqrun_chunks
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import copy_seqrun_chunk
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import run_interop_dump
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import check_progress_for_run_func
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import samplesheet_validation_and_branch_func
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import run_tile_demult_list_func

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


## SSH HOOKS
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
    default_args=default_args,
    orientation='LR')

'''
## FUNCTIONS
def get_ongoing_seqrun_list(**context):
  """
  A function for fetching ongoing sequencing run ids
  """
  try:
    ti = context.get('ti')
    seqrun_server = Variable.get('seqrun_server')
    seqrun_base_path = Variable.get('seqrun_base_path')
    seqrun_server_user = Variable.get('seqrun_server_user')
    database_config_file = Variable.get('database_config_file')
    ongoing_seqruns = \
      fetch_ongoing_seqruns(
        seqrun_server=seqrun_server,
        seqrun_base_path=seqrun_base_path,
        user_name=seqrun_server_user,
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
    raise


def copy_seqrun_manifest_file(**context):
  """
  A function for copying filesize manifest for ongoing sequencing runs to hpc 
  """
  try:
    remote_file_path = context['params'].get('file_path')
    seqrun_server = Variable.get('seqrun_server')
    seqrun_server_user = Variable.get('seqrun_server_user')
    xcom_pull_task_ids = context['params'].get('xcom_pull_task_ids')
    ti = context.get('ti')
    remote_file_path = ti.xcom_pull(task_ids=xcom_pull_task_ids)
    if remote_file_path is not None and \
       not isinstance(remote_file_path,str):
      remote_file_path = remote_file_path.decode().strip('\n')
    tmp_work_dir = get_temp_dir(use_ephemeral_space=True)
    local_file_path = \
      os.path.join(
        tmp_work_dir,
        os.path.basename(remote_file_path))
    remote_address = \
      '{0}@{1}'.format(seqrun_server_user,seqrun_server)
    copy_remote_file(
      remote_file_path,
      local_file_path,
      source_address=remote_address)
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
    raise


def reset_manifest_file(**context):
  """
  A function for checking existing files and resetting the manifest json with new files
  """
  try:
    xcom_pull_task_ids = context['params'].get('xcom_pull_task_ids')
    local_seqrun_path = context['params'].get('local_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    run_index_number = context['params'].get('run_index_number')
    ti = context.get('ti')
    json_path = ti.xcom_pull(task_ids=xcom_pull_task_ids)
    if json_path is not None and \
       not isinstance(json_path,str):
      json_path = json_path.decode().strip('\n')
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    compare_existing_seqrun_files(
      json_path=json_path,
      seqrun_id=seqrun_id,
      seqrun_base_path=local_seqrun_path)
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def get_seqrun_chunks(**context):
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
    if file_path is not None and \
       not isinstance(file_path,str):
      file_path = file_path.decode().strip('\n')
    check_file_path(file_path)
    file_data = read_json_data(file_path)
    chunk_size = None
    if worker_size is None or \
       worker_size == 0:
      raise ValueError(
              'Incorrect worker size: {0}'.\
                format(worker_size))
    if len(file_data) == 0:
      worker_branchs = \
        '{0}_{1}'.format(child_task_prefix,'no_work')
    else:
      if len(file_data) < int(5 * worker_size):
        worker_size = 1                                                           # setting worker size to 1 for low input
      if len(file_data) % worker_size == 0:
        chunk_size = int(len(file_data) / worker_size)
      else:
        chunk_size = int(len(file_data) / worker_size)+1
      ti.xcom_push(key=seqrun_chunk_size_key,value=chunk_size)
      worker_branchs = \
        ['{0}_{1}'.format(child_task_prefix,i) 
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
    raise


def copy_seqrun_chunk(**context):
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
    seqrun_server = Variable.get('seqrun_server')
    seqrun_server_user = Variable.get('seqrun_server_user')
    seqrun_base_path = Variable.get('seqrun_base_path')
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    file_path = \
      ti.xcom_pull(task_ids=file_path_task_ids)
    chunk_size = \
      ti.xcom_pull(key=seqrun_chunk_size_key,task_ids=seqrun_chunk_size_task_ids)
    check_file_path(file_path)
    file_data = read_json_data(file_path)
    start_index = chunk_index_number*chunk_size
    finish_index = ((chunk_index_number+1)*chunk_size) - 1
    if finish_index > len(file_data) - 1:
      finish_index = len(file_data) - 1
    local_seqrun_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id)
    remote_seqrun_path = \
      os.path.join(
        seqrun_base_path,
        seqrun_id)
    remote_address = \
      '{0}@{1}'.format(
        seqrun_server_user,
        seqrun_server)
    for entry in file_data[start_index:finish_index]:
      file_path = entry.get('file_path')
      file_size = entry.get('file_size')
      remote_path = \
        os.path.join(
          remote_seqrun_path,
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
          source_address=remote_address,
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
    raise


def run_interop_dump(**context):
  """
  A function for generating InterOp dump for seqrun
  """
  try:
    ti = context.get('ti')
    local_seqrun_path = Variable.get('hpc_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    run_index_number = context['params'].get('run_index_number')
    interop_dumptext_exe = Variable.get('interop_dumptext_exe')
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    seqrun_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id)
    dump_file = \
      os.path.join(
        temp_dir,
        '{0}_interop_dump.csv'.format(seqrun_id))
    check_file_path(interop_dumptext_exe)
    cmd = \
      [interop_dumptext_exe,seqrun_path,'>',dump_file]
    cmd = ' '.join(cmd)
    subprocess.\
      check_call(cmd,shell=True)
    return dump_file
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def check_progress_for_run_func(**context):
  try:
    ti = context.get('ti')
    local_seqrun_path = Variable.get('hpc_seqrun_path')
    seqrun_id_pull_key = context['params'].get('seqrun_id_pull_key')
    seqrun_id_pull_task_ids = context['params'].get('seqrun_id_pull_task_ids')
    run_index_number = context['params'].get('run_index_number')
    interop_dump_pull_task = context['params'].get('interop_dump_pull_task')
    no_job_prefix = context['params'].get('no_job_prefix')
    next_job_prefix = context['params'].get('next_job_prefix')
    job_list = \
      ['{0}_{1}'.format(no_job_prefix,run_index_number)]
    seqrun_id = \
      ti.xcom_pull(key=seqrun_id_pull_key,task_ids=seqrun_id_pull_task_ids)[run_index_number]
    runinfo_path = \
      os.path.join(
        local_seqrun_path,
        seqrun_id,
        'RunInfo.xml')
    interop_dump_path = \
      ti.xcom_pull(task_ids=interop_dump_pull_task)
    if interop_dump_path is not None and \
       not isinstance(interop_dump_path,str):
      interop_dump_path = \
        interop_dump_path.decode().strip('\n')
    current_cycle,index_cycle_status,read_format = \
      check_for_sequencing_progress(
        interop_dump=interop_dump_path,
        runinfo_file=runinfo_path)
    comment = \
      'seqrun: {0}, current cycle: {1}, index cycle: {2}, read format: {3}'.\
        format(seqrun_id,current_cycle,index_cycle_status,read_format)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=comment,
      reaction='pass')
    if index_cycle_status is 'complete':
      job_list = \
        ['{0}_{1}'.format(next_job_prefix,run_index_number)]
    return job_list
  except Exception as e:
    logging.error(e)
    send_log_to_channels(
      slack_conf=Variable.get('slack_conf'),
      ms_teams_conf=Variable.get('ms_teams_conf'),
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise
'''

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
    generate_seqrun_file_list = \
      SSHOperator(
        task_id='generate_seqrun_file_list_{0}'.format(i),
        dag=dag,
        pool='orwell_exe_pool',
        ssh_hook=orwell_ssh_hook,
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
    copy_seqrun_file_list = \
      PythonOperator(
        task_id='copy_seqrun_file_list_{0}'.format(i),
        dag=dag,
        pool='orwell_scp_pool',
        queue='hpc_4G',
        params={'xcom_pull_task_ids':'generate_seqrun_file_list_{0}'.format(i)},
        python_callable=copy_seqrun_manifest_file)
    ## TASK
    compare_seqrun_files = \
      PythonOperator(
        task_id='compare_seqrun_files_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'xcom_pull_task_ids':'copy_seqrun_file_list_{0}'.format(i),
                'seqrun_id_pull_key':'ongoing_seqruns',
                'run_index_number':i,
                'seqrun_id_pull_task_ids':'generate_seqrun_list',
                'local_seqrun_path':Variable.get('hpc_seqrun_path')},
        python_callable=reset_manifest_file)
    ## TASK
    decide_copy_branch = \
      BranchPythonOperator(
        task_id='decide_copy_branch_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'xcom_pull_task_ids':'copy_seqrun_file_list_{0}'.format(i),
                'worker_size':10,
                'seqrun_chunk_size_key':'seqrun_chunk_size',
                'child_task_prefix':'copy_file_run_{0}_chunk'.format(i)},
        python_callable=get_seqrun_chunks)
    ## TASK
    no_copy_seqrun = \
      DummyOperator(
        task_id='copy_file_run_{0}_chunk_{1}'.format(i,'no_work'),
        dag=dag,
        queue='hpc_4G',
        on_success_callback=log_sleep)
    ## TASK
    copy_seqrun_files = list()
    for j in range(10):
      copy_file_chunk = \
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
      copy_seqrun_files.append(copy_file_chunk)
    ## PIPELINE
    generate_seqrun_list >> generate_seqrun_file_list >> copy_seqrun_file_list >> compare_seqrun_files >> decide_copy_branch
    decide_copy_branch >> no_copy_seqrun
    decide_copy_branch >> copy_seqrun_files
    ## TASK
    wait_for_copy_chunk = \
      DummyOperator(
        task_id='wait_for_copy_chunk_run_{0}'.format(i),
        dag=dag,
        trigger_rule='none_failed_or_skipped',
        queue='hpc_4G')
    ## PIPELINE
    copy_seqrun_files >> wait_for_copy_chunk
    ## TASK
    create_interop_dump = \
      PythonOperator(
        task_id='create_interop_dump_run_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'run_index_number':i,
                'seqrun_id_pull_key':'ongoing_seqruns',
                'seqrun_id_pull_task_ids':'generate_seqrun_list'},
        python_callable=run_interop_dump)
    ## PIPELINE
    wait_for_copy_chunk >> create_interop_dump
    ## TASK
    check_progress_for_run = \
      BranchPythonOperator(
        task_id='check_progress_for_run_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'run_index_number':i,
                'seqrun_id_pull_key':'ongoing_seqruns',
                'seqrun_id_pull_task_ids':'generate_seqrun_list',
                'samplesheet_validation_job_prefix':'samplesheet_validation',
                'tile_demult_job_prefix':'tile_demultiplexing',
                'no_job_prefix':'no_seqrun_checking',
                'next_job_prefix':'samplesheet_validation',
                'runParameters_xml_file_name':'runParameters.xml',
                'samplesheet_file_name':'SampleSheet.csv',
                'interop_dump_pull_task':'create_interop_dump_run_{0}'.format(i)},
        python_callable=check_progress_for_run_func)
    ## PIPELINE
    create_interop_dump >> check_progress_for_run
    ## TASK
    no_seqrun_checking = \
      DummyOperator(
        task_id='no_seqrun_checking_{0}'.format(i),
        dag=dag,
        queue='hpc_4G')
    ## PIPELINE
    check_progress_for_run >> no_seqrun_checking
    ## TASK
    samplesheet_validation_and_branch = \
      BranchPythonOperator(
        task_id='samplesheet_validation_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'run_index_number':i,
                'seqrun_id_pull_key':'ongoing_seqruns',
                'seqrun_id_pull_task_ids':'generate_seqrun_list',
                'samplesheet_file_name':'SampleSheet.csv',
                'runParameters_xml_file_name':'runParameters.xml',
                'no_job_prefix':'no_seqrun_checking',
                'next_job_prefix':'tile_demultiplexing',
                'next_job_range':[i for i in range(1,9)]},
        python_callable=samplesheet_validation_and_branch_func)
    ## PIPELINE
    check_progress_for_run >> samplesheet_validation_and_branch
    ## TASK
    run_tile_demult_list = list()
    for j in range(1,9):
      run_tile_demult_per_lane = \
        PythonOperator(
          task_id='tile_demultiplexing_{0}_{1}'.format(i,j),
          dag=dag,
        queue='hpc_4G',
        params={'run_index_number':i,
                'lane_id':j,
                'seqrun_id_pull_key':'ongoing_seqruns',
                'seqrun_id_pull_task_ids':'generate_seqrun_list',
                'samplesheet_file_name':'SampleSheet.csv',
                'runinfo_xml_file_name':'RunInfo.xml',
                'runParameters_xml_file_name':'runParameters.xml',
                'tile_list':[1101,],
                'threads':1},
        python_callable=run_tile_demult_list_func)
      run_tile_demult_list.\
        append(run_tile_demult_per_lane)
    ## PIPELINE
    samplesheet_validation_and_branch >> run_tile_demult_list
    samplesheet_validation_and_branch >> no_seqrun_checking

  ## PIPELINE
  generate_seqrun_list >> no_ongoing_seqrun

