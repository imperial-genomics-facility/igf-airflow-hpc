from datetime import timedelta
import os,json,logging,subprocess
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from igf_airflow.logging.upload_log_msg import send_log_to_channels,log_success,log_failure,log_sleep
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import get_ongoing_seqrun_list
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import copy_seqrun_manifest_file
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import reset_manifest_file
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import get_seqrun_chunks
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import copy_seqrun_chunk
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import run_interop_dump
from igf_airflow.utils.dag8_copy_ongoing_seqrun_utils import generate_interop_report_func
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
    remote_host=Variable.get('orwell_server_hostname'))


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
        trigger_rule='none_failed_min_one_success',
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
    generate_interop_report = \
      PythonOperator(
        task_id='generate_interop_report_run_{0}'.format(i),
        dag=dag,
        queue='hpc_4G',
        params={'run_index_number':i,
                'seqrun_id_pull_key':'ongoing_seqruns',
                'seqrun_id_pull_task_ids':'generate_seqrun_list',
                'runInfo_xml_file_name':'RunInfo.xml',
                'interop_dump_pull_task':'create_interop_dump_run_{0}'.format(i),
                'timeout':1200,
                'kernel_name':'python3',
                'output_notebook_key':'interop_notebook'},
        python_callable=generate_interop_report_func)
    ## PIPELINE
    create_interop_dump >> generate_interop_report
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

