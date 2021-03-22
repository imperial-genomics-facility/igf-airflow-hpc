from datetime import timedelta
import os,json,logging,subprocess
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import fetch_analysis_info_and_branch_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import configure_cellranger_run_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import run_sc_read_trimmming_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import run_cellranger_tool
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import decide_analysis_branch_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import load_cellranger_result_to_db_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import ftp_files_upload_for_analysis
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import irods_files_upload_for_analysis
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import run_scanpy_for_sc_5p_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import run_singlecell_notebook_wrapper_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import load_analysis_files_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import task_branch_function
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import upload_analysis_file_to_box
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import convert_bam_to_cram_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import run_picard_for_cellranger
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import run_samtools_for_cellranger
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import run_multiqc_for_cellranger

## ARGS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'max_active_runs':10,
    'catchup':False,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

FEATURE_TYPE_LIST = Variable.get('tenx_single_cell_immune_profiling_feature_types').split(',')

## DAG
dag = \
  DAG(
    dag_id='dag9_tenx_single_cell_immune_profiling',
    schedule_interval=None,
    tags=['hpc','analysis','tenx','sc'],
    default_args=default_args,
    orientation='LR')

with dag:
  ## TASK
  fetch_analysis_info_and_branch = \
    BranchPythonOperator(
      task_id='fetch_analysis_info',
      dag=dag,
      queue='hpc_4G',
      params={'no_analysis_task':'no_analysis',
              'analysis_description_xcom_key':'analysis_description',
              'analysis_info_xcom_key':'analysis_info'},
      python_callable=fetch_analysis_info_and_branch_func)
  ## TASK
  configure_cellranger_run = \
    PythonOperator(
      task_id='configure_cellranger_run',
      dag=dag,
      queue='hpc_4G',
      trigger_rule='none_failed_or_skipped',
      params={'xcom_pull_task_id':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'analysis_info_xcom_key':'analysis_info',
              'library_csv_xcom_key':'cellranger_library_csv'},
      python_callable=configure_cellranger_run_func)
  for analysis_name in FEATURE_TYPE_LIST:
    ## TASK
    task_branch = \
      BranchPythonOperator(
        task_id=analysis_name,
        dag=dag,
        queue='hpc_4G',
        params={'xcom_pull_task_id':'fetch_analysis_info',
                'analysis_info_xcom_key':'analysis_info',
                'analysis_name':analysis_name,
                'task_prefix':'run_trim'},
        python_callable=task_branch_function)
    run_trim_list = list()
    for run_id in range(0,10):
      ## TASK
      t = \
        PythonOperator(
          task_id='run_trim_{0}_{1}'.format(analysis_name,run_id),
          dag=dag,
          queue='hpc_4G',
          params={'xcom_pull_task_id':'fetch_analysis_info',
                  'analysis_info_xcom_key':'analysis_info',
                  'analysis_description_xcom_key':'analysis_description',
                  'analysis_name':analysis_name,
                  'run_id':run_id,
                  'r1_length':0,
                  'r2_length':0,
                  'fastq_input_dir_tag':'fastq_dir',
                  'fastq_output_dir_tag':'output_path'},
          python_callable=run_sc_read_trimmming_func)
      run_trim_list.append(t)
    ## TASK
    collect_trimmed_files = \
      DummyOperator(
        task_id='collect_trimmed_files_{0}'.format(analysis_name),
        trigger_rule='none_failed_or_skipped',
        dag=dag)
    ## PIPELINE
    fetch_analysis_info_and_branch >> task_branch
    task_branch >> run_trim_list
    run_trim_list >> collect_trimmed_files
    collect_trimmed_files >> configure_cellranger_run
  ## TASK
  no_analysis = \
    DummyOperator(
      task_id='no_analysis',
      dag=dag)
  ## PIPELINE
  fetch_analysis_info_and_branch >> no_analysis
  ## TASK
  run_cellranger = \
    PythonOperator(
      task_id='run_cellranger',
      dag=dag,
      queue='hpc_64G16t24hr',
      params={'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'library_csv_xcom_key':'cellranger_library_csv',
              'library_csv_xcom_pull_task':'configure_cellranger_run',
              'cellranger_xcom_key':'cellranger_output',
              'cellranger_options':['--localcores 16','--localmem 64']},
      python_callable=run_cellranger_tool)
  ## PIPELINE
  configure_cellranger_run >> run_cellranger
  ## TASK
  decide_analysis_branch = \
    BranchPythonOperator(
      task_id='decide_analysis_branch',
      dag=dag,
      queue='hpc_4G',
      python_callable=decide_analysis_branch_func,
      params={'load_cellranger_result_to_db_task':'load_cellranger_result_to_db',
              'run_scanpy_for_sc_5p_task':'run_scanpy_for_sc_5p',
              'run_scirpy_for_vdj_task':'run_scirpy_for_vdj',
              'run_scirpy_for_vdj_b_task':'run_scirpy_for_vdj_b',
              'run_scirpy_vdj_t_task':'run_scirpy_for_vdj_t',
              'run_seurat_for_sc_5p_task':'run_seurat_for_sc_5p',
              'run_picard_alignment_summary_task':'run_picard_alignment_summary',
              'convert_bam_to_cram_task':'convert_bam_to_cram',
              'library_csv_xcom_key':'cellranger_library_csv',
              'library_csv_xcom_pull_task':'configure_cellranger_run'})
  ## PIPELINE
  run_cellranger >> decide_analysis_branch
  ## TASK
  load_cellranger_result_to_db = \
    PythonOperator(
      task_id='load_cellranger_result_to_db',
      dag=dag,
      queue='hpc_4G',
      python_callable=load_cellranger_result_to_db_func,
      params={'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'collection_type':'CELLRANGER_MULTI',
              'collection_table':'sample',
              'xcom_collection_name_key':'sample_igf_id',
              'genome_column':'genome_build',
              'analysis_name':'cellranger_multi',
              'output_xcom_key':'loaded_output_files',
              'html_xcom_key':'html_report_file',
              'html_report_file_name':'web_summary.html'})
  upload_cellranger_report_to_ftp = \
    PythonOperator(
      task_id='upload_cellranger_report_to_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_cellranger_result_to_db',
              'xcom_pull_files_key':'html_report_file',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_CELLRANGER_MULTI',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_cellranger_report_to_box = \
    PythonOperator(
      task_id='upload_cellranger_report_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=None,
      params={'xcom_pull_task':'load_cellranger_result_to_db',
              'xcom_pull_files_key':'html_report_file'})
  upload_cellranger_results_to_irods = \
    PythonOperator(
      task_id='upload_cellranger_results_to_irods',
      dag=dag,
      queue='hpc_4G',
      python_callable=irods_files_upload_for_analysis,
      params={'xcom_pull_task':'load_cellranger_result_to_db',
              'xcom_pull_files_key':'loaded_output_files',
              'collection_name_key':'sample_igf_id',
              'analysis_name':'cellranger_multi'})
  ## PIPELINE
  decide_analysis_branch >> load_cellranger_result_to_db
  load_cellranger_result_to_db >> upload_cellranger_report_to_ftp
  load_cellranger_result_to_db >> upload_cellranger_report_to_box
  load_cellranger_result_to_db >> upload_cellranger_results_to_irods
  ## TASK
  run_scanpy_for_sc_5p = \
    PythonOperator(
      task_id='run_scanpy_for_sc_5p',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'python3',
              'count_dir':'count',
              'analysis_name':'scanpy',
              'output_notebook_key':'scanpy_notebook',
              'output_cellbrowser_key':'cellbrowser_dirs',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description'})
  load_scanpy_report_for_sc_5p_to_db = \
    PythonOperator(
      task_id='load_scanpy_report_for_sc_5p_to_db',
      dag=dag,
      queue='hpc_4G',
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_scanpy_for_sc_5p',
              'file_name_key':'scanpy_notebook',
              'analysis_name':'scanpy_5p',
              'collection_type':'SCANPY_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_scanpy_report_for_sc_5p_to_ftp = \
    PythonOperator(
      task_id='upload_scanpy_report_for_sc_5p_to_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_sc_5p_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCANPY_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scanpy_report_for_sc_5p_to_box = \
    PythonOperator(
      task_id='upload_scanpy_report_for_sc_5p_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=upload_analysis_file_to_box,
      params={'xcom_pull_task':'load_scanpy_report_for_sc_5p_to_db',
              'xcom_pull_files_key':'output_db_files',
              'analysis_tag':'scanpy_single_sample_report'})
  upload_cellbrowser_for_sc_5p_to_ftp = \
    PythonOperator(
      task_id='upload_cellbrowser_for_sc_5p_to_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'run_scanpy_for_sc_5p',
              'xcom_pull_files_key':'cellbrowser_dirs',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_CELLBROWSER',
              'collection_table':'sample',
              'collect_remote_file':True})
  ## PIPELINE
  decide_analysis_branch >> run_scanpy_for_sc_5p
  run_scanpy_for_sc_5p >> load_scanpy_report_for_sc_5p_to_db
  load_scanpy_report_for_sc_5p_to_db >> upload_scanpy_report_for_sc_5p_to_ftp
  load_scanpy_report_for_sc_5p_to_db >> upload_scanpy_report_for_sc_5p_to_box
  run_scanpy_for_sc_5p >> upload_cellbrowser_for_sc_5p_to_ftp
  ## TASK
  run_scirpy_for_vdj = \
    PythonOperator(
      task_id='run_scirpy_for_vdj',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'python3',
              'analysis_name':'scirpy',
              'vdj_dir':'vdj',
              'count_dir':'count',
              'output_notebook_key':'scirpy_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description'})
  load_scirpy_report_for_vdj_to_db = \
    PythonOperator(
      task_id='load_scirpy_report_for_vdj_to_db',
      dag=dag,
      queue='hpc_4G',
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_scirpy_for_vdj',
              'file_name_key':'scirpy_notebook',
              'analysis_name':'scirpy_vdj',
              'collection_type':'SCIRPY_VDJ_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_scirpy_report_for_vdj_to_ftp = \
    PythonOperator(
      task_id='upload_scirpy_report_for_vdj_to_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCIRPY_VDJ_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scirpy_report_for_vdj_to_box = \
    PythonOperator(
      task_id='upload_scirpy_report_for_vdj_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=upload_analysis_file_to_box,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_to_db',
              'xcom_pull_files_key':'output_db_files',
              'analysis_tag':'scirpy_vdj_single_sample_report'})
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj
  run_scirpy_for_vdj >> load_scirpy_report_for_vdj_to_db
  load_scirpy_report_for_vdj_to_db >> upload_scirpy_report_for_vdj_to_ftp
  load_scirpy_report_for_vdj_to_db >> upload_scirpy_report_for_vdj_to_box
  ## TASK
  run_scirpy_for_vdj_b = \
    PythonOperator(
      task_id='run_scirpy_for_vdj_b',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'python3',
              'analysis_name':'scirpy',
              'vdj_dir':'vdj_b',
              'count_dir':'count',
              'output_notebook_key':'scirpy_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description'})
  load_scirpy_report_for_vdj_b_to_db = \
    PythonOperator(
      task_id='load_scirpy_report_for_vdj_b_to_db',
      dag=dag,
      queue='hpc_4G',
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_scirpy_for_vdj_b',
              'file_name_key':'scirpy_notebook',
              'analysis_name':'scirpy_vdj_b',
              'collection_type':'SCIRPY_VDJ_B_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_scirpy_report_for_vdj_b_to_ftp = \
    PythonOperator(
      task_id='upload_scirpy_report_for_vdj_b_to_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_b_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCIRPY_VDJ_B_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scirpy_report_for_vdj_b_to_box = \
    PythonOperator(
      task_id='upload_scanpy_report_for_vdj_b_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=upload_analysis_file_to_box,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_b_to_db',
              'xcom_pull_files_key':'output_db_files',
              'analysis_tag':'scirpy_vdj_b_single_sample_report'})
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj_b
  run_scirpy_for_vdj_b >> load_scirpy_report_for_vdj_b_to_db
  load_scirpy_report_for_vdj_b_to_db >> upload_scirpy_report_for_vdj_b_to_ftp
  load_scirpy_report_for_vdj_b_to_db >> upload_scirpy_report_for_vdj_b_to_box
  ## TASK
  run_scirpy_for_vdj_t = \
    PythonOperator(
      task_id='run_scirpy_for_vdj_t',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'python3',
              'analysis_name':'scirpy',
              'vdj_dir':'vdj_t',
              'count_dir':'count',
              'output_notebook_key':'scirpy_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description'})
  load_scirpy_report_for_vdj_t_to_db = \
    PythonOperator(
      task_id='load_scirpy_report_for_vdj_t_to_db',
      dag=dag,
      queue='hpc_4G',
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_scirpy_for_vdj_t',
              'file_name_key':'scirpy_notebook',
              'analysis_name':'scirpy_vdj_t',
              'collection_type':'SCIRPY_VDJ_T_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_scirpy_report_for_vdj_t_to_ftp = \
    PythonOperator(
      task_id='upload_scirpy_report_for_vdj_t_to_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_t_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCIRPY_VDJ_T_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scirpy_report_for_vdj_t_to_box = \
    PythonOperator(
      task_id='upload_scirpy_report_for_vdj_t_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=upload_analysis_file_to_box,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_t_to_db',
              'xcom_pull_files_key':'output_db_files',
              'analysis_tag':'scirpy_vdj_t_single_sample_report'})
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj_t
  run_scirpy_for_vdj_t >> load_scirpy_report_for_vdj_t_to_db
  load_scirpy_report_for_vdj_t_to_db >> upload_scirpy_report_for_vdj_t_to_ftp
  load_scirpy_report_for_vdj_t_to_db >> upload_scirpy_report_for_vdj_t_to_box
  ## TASK
  run_seurat_for_sc_5p = \
    PythonOperator(
      task_id='run_seurat_for_sc_5p',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'R',
              'analysis_name':'seurat',
              'vdj_dir':'vdj',
              'count_dir':'count',
              'output_notebook_key':'seurat_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description'})
  load_seurat_report_for_sc_5p_db = \
    PythonOperator(
      task_id='load_seurat_report_for_sc_5p_db',
      dag=dag,
      queue='hpc_4G',
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_seurat_for_sc_5p',
              'file_name_key':'seurat_notebook',
              'analysis_name':'seurat_5p',
              'collection_type':'SEURAT_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_seurat_report_for_sc_5p_ftp = \
    PythonOperator(
      task_id='upload_seurat_report_for_sc_5p_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_seurat_report_for_sc_5p_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SEURAT_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_seurat_report_for_sc_5p_to_box = \
    PythonOperator(
      task_id='upload_seurat_report_for_sc_5p_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=upload_analysis_file_to_box,
      params={'xcom_pull_task':'load_seurat_report_for_sc_5p_db',
              'xcom_pull_files_key':'output_db_files',
              'analysis_tag':'seurat_single_sample_report'})
  ## PIPELINE
  decide_analysis_branch >> run_seurat_for_sc_5p
  run_seurat_for_sc_5p >> load_seurat_report_for_sc_5p_db
  load_seurat_report_for_sc_5p_db >> upload_seurat_report_for_sc_5p_ftp
  load_seurat_report_for_sc_5p_db >> upload_seurat_report_for_sc_5p_to_box
  ## TASK
  convert_bam_to_cram = \
    PythonOperator(
      task_id='convert_cellranger_bam_to_cram',
      dag=dag,
      queue='hpc_4G4t',
      python_callable=convert_bam_to_cram_func,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'threads':4,
              'analysis_name':'cellranger',
              'collection_type':'ALIGNED_CRAM',
              'collection_table':'sample',
              'cram_files_xcom_key':'cram_files'})
  upload_cram_to_irods = \
    PythonOperator(
      task_id='upload_cram_to_irods',
      dag=dag,
      queue='hpc_4G',
      python_callable=irods_files_upload_for_analysis,
      params={'xcom_pull_task':'convert_cellranger_bam_to_cram',
              'xcom_pull_files_key':'cram_files',
              'collection_name_key':'sample_igf_id',
              'analysis_name':'cellranger_multi'})
  ## PIPELINE
  decide_analysis_branch >> convert_bam_to_cram
  convert_bam_to_cram >> upload_cram_to_irods
  ## TASK
  run_picard_alignment_summary = \
    PythonOperator(
      task_id='run_picard_alignment_summary',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_picard_for_cellranger,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'load_metrics_to_cram':True,
              'java_param':'-Xmx4g',
              'picard_command':'CollectAlignmentSummaryMetrics',
              'picard_option':{},
              'analysis_files_xcom_key':'picard_alignment_summary',
              'bam_files_xcom_key':None})
  ## PIPELINE
  decide_analysis_branch >> run_picard_alignment_summary
  ## TASK
  run_picard_qual_summary = \
    PythonOperator(
      task_id='run_picard_qual_summary',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_picard_for_cellranger,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'load_metrics_to_cram':True,
              'java_param':'-Xmx4g',
              'picard_command':'QualityScoreDistribution',
              'picard_option':{},
              'analysis_files_xcom_key':'picard_qual_summary',
              'bam_files_xcom_key':None})
  ## PIPELINE
  run_picard_alignment_summary >> run_picard_qual_summary
  ## TASK
  run_picard_rna_summary = \
    PythonOperator(
      task_id='run_picard_rna_summary',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_picard_for_cellranger,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'load_metrics_to_cram':True,
              'java_param':'-Xmx4g',
              'picard_command':'CollectRnaSeqMetrics',
              'picard_option':{},
              'analysis_files_xcom_key':'picard_rna_summary',
              'bam_files_xcom_key':None})
  ## PIPELINE
  run_picard_qual_summary >> run_picard_rna_summary
  ## TASK
  run_picard_gc_summary = \
    PythonOperator(
      task_id='run_picard_gc_summary',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_picard_for_cellranger,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'load_metrics_to_cram':True,
              'java_param':'-Xmx4g',
              'picard_command':'CollectGcBiasMetrics',
              'picard_option':{},
              'analysis_files_xcom_key':'picard_gc_summary',
              'bam_files_xcom_key':None})
  ## PIPELINE
  run_picard_rna_summary >> run_picard_gc_summary
  ## TASK
  run_picard_base_dist_summary = \
    PythonOperator(
      task_id='run_picard_base_dist_summary',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_picard_for_cellranger,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'load_metrics_to_cram':True,
              'java_param':'-Xmx4g',
              'picard_command':'CollectBaseDistributionByCycle',
              'picard_option':{},
              'analysis_files_xcom_key':'picard_base_summary',
              'bam_files_xcom_key':None})
  ## PIPELINE
  run_picard_gc_summary >> run_picard_base_dist_summary
  ## TASK
  run_samtools_stats = \
    PythonOperator(
      task_id='run_samtools_stats',
      dag=dag,
      queue='hpc_4G4t',
      python_callable=run_samtools_for_cellranger,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'load_metrics_to_cram':True,
              'samtools_command':'stats',
              'threads':4,
              'analysis_files_xcom_key':'samtools_stats'})
  ## PIPELINE
  run_picard_gc_summary >> run_samtools_stats
  ## TASK
  run_samtools_idxstats = \
    PythonOperator(
      task_id='run_samtools_idxstats',
      dag=dag,
      queue='hpc_4G4t',
      python_callable=run_samtools_for_cellranger,
      params={'xcom_pull_files_key':'cellranger_output',
              'xcom_pull_task':'run_cellranger',
              'analysis_description_xcom_pull_task':'fetch_analysis_info',
              'analysis_description_xcom_key':'analysis_description',
              'use_ephemeral_space':True,
              'load_metrics_to_cram':True,
              'samtools_command':'idxstats',
              'threads':4,
              'analysis_files_xcom_key':'samtools_idxstats'})
  ## PIPELINE
  run_samtools_stats >> run_samtools_idxstats
  ## TASK
  run_multiqc = \
    PythonOperator(
      task_id='run_multiqc',
      dag=dag,
      queue='hpc_4G',
      python_callable=run_multiqc_for_cellranger,
      params={
        'list_of_analysis_xcoms_and_tasks':{
          'run_cellranger':'cellranger_output',
          'run_picard_alignment_summary':'picard_alignment_summary',
          'run_picard_qual_summary':'picard_qual_summary',
          'run_picard_rna_summary':'picard_rna_summary',
          'run_picard_gc_summary':'picard_gc_summary',
          'run_picard_base_dist_summary':'picard_base_summary',
          'run_samtools_stats':'samtools_stats',
          'run_samtools_idxstats':'samtools_idxstats'},
        'analysis_description_xcom_pull_task':'fetch_analysis_info',
        'analysis_description_xcom_key':'analysis_description',
        'use_ephemeral_space':True,
        'multiqc_html_file_xcom_key':'multiqc_html',
        'multiqc_data_file_xcom_key':'multiqc_data',
        'tool_order_list':['picad','samtools']})
  ## PIPELINE
  run_samtools_idxstats >> run_multiqc
  ## TASK
  upload_multiqc_to_ftp = \
    PythonOperator(
      task_id='upload_multiqc_to_ftp',
      dag=dag,
      queue='hpc_4G',
      python_callable=None)
  ## PIPELINE
  run_multiqc >> upload_multiqc_to_ftp
  ## TASK
  upload_multiqc_to_box = \
    PythonOperator(
      task_id='upload_multiqc_to_box',
      dag=dag,
      queue='hpc_4G',
      python_callable=None)
  ## PIPELINE
  run_multiqc >> upload_multiqc_to_box
  ## TASK
  update_analysis_and_status = \
    PythonOperator(
      task_id='update_analysis_and_status',
      dag=dag,
      queue='hpc_4G',
      python_callable=None,
      trigger_rule='none_failed_or_skipped')
  ## PIPELINE
  upload_multiqc_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_sc_5p_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_sc_5p_to_box >> update_analysis_and_status
  upload_cellbrowser_for_sc_5p_to_ftp >> update_analysis_and_status
  upload_scirpy_report_for_vdj_to_ftp >> update_analysis_and_status
  upload_scirpy_report_for_vdj_to_box >> update_analysis_and_status
  upload_scirpy_report_for_vdj_b_to_ftp >> update_analysis_and_status
  upload_scirpy_report_for_vdj_b_to_box >> update_analysis_and_status
  upload_scirpy_report_for_vdj_t_to_ftp >> update_analysis_and_status
  upload_scirpy_report_for_vdj_t_to_box >> update_analysis_and_status
  upload_seurat_report_for_sc_5p_ftp >> update_analysis_and_status
  upload_seurat_report_for_sc_5p_to_box >> update_analysis_and_status
  upload_cellranger_results_to_irods >> update_analysis_and_status
  upload_cellranger_report_to_ftp >> update_analysis_and_status
  upload_cellranger_report_to_box >> update_analysis_and_status
  upload_cram_to_irods >> update_analysis_and_status