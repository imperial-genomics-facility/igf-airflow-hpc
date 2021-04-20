from datetime import timedelta
import os,json,logging,subprocess
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import fetch_nextflow_analysis_info_and_branch_func
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import prep_nf_run_func
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import run_nf_command_func
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import copy_nf_data_to_box_func
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import copy_nf_data_to_irods_func
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import nf_analysis_copy_branch_func
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import change_pipeline_status
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import copy_nf_output_to_disk_func

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': days_ago(2),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 4,
  'max_active_runs':10,
  'catchup':True,
  'retry_delay': timedelta(minutes=5),
  'provide_context': True}

dag = \
  DAG(
    dag_id='dag10_nextflow_atacseq_pipeline',
    schedule_interval=None,
    tags=['hpc','analysis','nextflow','atacseq'],
    default_args=default_args,
    orientation='LR')

with dag:
  ## TASK
  fetch_nextflow_analysis_info_and_branch = \
    BranchPythonOperator(
      task_id='fetch_nextflow_analysis_info_and_branch',
      dag=dag,
      queue='hpc_4G',
      python_callable=fetch_nextflow_analysis_info_and_branch_func,
      params={'no_analysis_task':'no_analysis',
              'active_tasks':['prep_nf_atacseq_run'],
              'analysis_description_xcom_key':'analysis_description'})
  ## TASK
  prep_nf_atacseq_run = \
    PythonOperator(
      task_id='prep_nf_atacseq_run',
      dag=dag,
      queue='hpc_4G',
      python_callable=prep_nf_run_func,
      params={'analysis_description_xcom_key':'analysis_description',
              'analysis_description_xcom_task':'fetch_nextflow_analysis_info_and_branch',
              'nextflow_command_xcom_key':'nexflow_command',
              'nextflow_work_dir_xcom_key':'nextflow_work_dir'})
  ## TASK
  no_analysis = \
    DummyOperator(
      task_id='no_analysis',
      dag=dag)
  ## TASK
  update_analysis_and_status = \
    PythonOperator(
      task_id='update_analysis_and_status',
      dag=dag,
      queue='hpc_4G',
      python_callable=change_pipeline_status,
      trigger_rule='none_failed_or_skipped',
      params={'new_status':'FINISHED',
              'no_change_status':'SEEDED'})
  ## PIPELINE
  fetch_nextflow_analysis_info_and_branch >> prep_nf_atacseq_run
  fetch_nextflow_analysis_info_and_branch >> no_analysis
  no_analysis >> update_analysis_and_status
  ## TASK
  run_nf_atacseq = \
    PythonOperator(
      task_id='run_nf_atacseq',
      dag=dag,
      queue='hpc_4G_long',
      pool='nextflow_hpc',
      python_callable=run_nf_command_func,
      params={'nextflow_command_xcom_task':'prep_nf_atacseq_run',
              'nextflow_command_xcom_key':'nexflow_command',
              'nextflow_work_dir_xcom_task':'prep_nf_atacseq_run',
              'nextflow_work_dir_xcom_key':'nextflow_work_dir'})
  ## TASK
  copy_nf_output_to_disk = \
    PythonOperator(
      task_id='copy_nf_output_to_disk',
      dag=dag,
      queue='hpc_4G',
      python_callable=copy_nf_output_to_disk_func,
      params={'nextflow_work_dir_xcom_task':'prep_nf_atacseq_run',
              'nextflow_work_dir_xcom_key':'nextflow_work_dir',
              'result_dirname':'results',
              'data_dir_list':['bwa'],
              'report_file_dirs':['fastqc','igv','multiqc','pipeline_info','trim_galore'],
              'dag_file_name':'dag.html'})
  ## TASK
  nf_analysis_copy_branch = \
    BranchPythonOperator(
      task_id='nf_analysis_copy_branch',
      dag=dag,
      queue='hpc_4G',
      python_callable=nf_analysis_copy_branch_func,
      params={'nextflow_work_dir_xcom_task':'prep_nf_atacseq_run',
              'nextflow_work_dir_xcom_key':'nextflow_work_dir',
              'result_dirname':'results',
              'data_dir_list':['bwa'],
              'data_dir_xcom_key':'data_dir',
              'report_file_dirs':['fastqc','igv','multiqc','pipeline_info','trim_galore'],
              'report_file_xcom_key':'report_dir',
              'dag_file_name':'dag.html',
              'dag_file_xcom_key':'dag_file',
              'data_file_copy_tasks':['copy_nf_data_to_irods'],
              'report_file_copy_tasks':['copy_nf_data_to_box']})
  ## TASK
  copy_nf_data_to_irods = \
    PythonOperator(
      task_id='copy_nf_data_to_irods',
      dag=dag,
      queue='hpc_4G',
      python_callable=copy_nf_data_to_irods_func,
      params={'nextflow_work_dir_xcom_task':'prep_nf_atacseq_run',
              'nextflow_work_dir_xcom_key':'nextflow_work_dir',
              'result_dirname':'results',
              'data_dir_xcom_key':'data_dir',
              'data_dir_xcom_task':'nf_analysis_copy_branch'})
  ## TASK
  copy_nf_data_to_box = \
    PythonOperator(
      task_id='copy_nf_data_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=copy_nf_data_to_box_func,
      params={'report_file_xcom_key':'report_dir',
              'report_file_xcom_task':'nf_analysis_copy_branch',
              'dag_file_xcom_key':'dag_file',
              'dag_file_xcom_task':'nf_analysis_copy_branch'})
  ## PIPELINE
  prep_nf_atacseq_run >> run_nf_atacseq >> nf_analysis_copy_branch
  run_nf_atacseq >> copy_nf_output_to_disk
  nf_analysis_copy_branch >> copy_nf_data_to_irods
  nf_analysis_copy_branch >> copy_nf_data_to_box
  copy_nf_output_to_disk >> update_analysis_and_status
  copy_nf_data_to_irods >> update_analysis_and_status
  copy_nf_data_to_box >> update_analysis_and_status