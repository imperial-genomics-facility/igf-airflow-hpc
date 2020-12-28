from datetime import timedelta
import os,json,logging,subprocess
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from igf_airflow.logging.upload_log_msg import send_log_to_channels,log_success,log_failure,log_sleep
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling import fetch_analysis_info_and_branch_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling import configure_cellranger_run_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling import run_sc_read_trimmming_func
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling import run_cellranger_tool

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

FEATURE_TYPE_LIST = Variable.get('tenx_single_cell_immune_profiling_feature_types')

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
      queue='hpc_4g',
      params={'no_analysis_task':'no_analysis',
              'analysis_description_xcom_key':'analysis_description',
              'analysis_info_xcom_key':'analysis_info'},
      python_callable=fetch_analysis_info_and_branch_func)
  ## TASK
  configure_cellranger_run = \
    PythonOperator(
      task_id='configure_cellranger_run',
      dag=dag,
      queue='hpc_4g',
      params={'xcom_pull_task_id':'fetch_analysis_info_and_branch',
              'analysis_description_xcom_key':'analysis_description',
              'analysis_info_xcom_key':'analysis_info',
              'library_csv_xcom_key':'cellranger_library_csv'},
      python_callable=configure_cellranger_run_func)
  for analysis_name in FEATURE_TYPE_LIST:
    ## TASK
    task_branch = \
      DummyOperator(
        task_id=analysis_name,
        dag=dag)
    run_trim_list = list()
    for run_id in range(0,32):
      ## TASK
      t = \
        PythonOperator(
          task_id='run_trim_{0}_{1}'.format(analysis_name,run_id),
          dag=dag,
          params={'xcom_pull_task_id':'fetch_analysis_info_and_branch',
                  'analysis_info_xcom_key':'analysis_info',
                  'analysis_name':analysis_name,
                  'run_id':run_id,
                  'r1_length':26,
                  'r2_length':0,
                  'fastq_input_dir_tag':'fastq_dir',
                  'fastq_output_dir_tag':'output_path'},
          python_callable=run_sc_read_trimmming_func)
      run_trim_list.append(t)
    ## TASK
    collect_trimmed_files = \
      DummyOperator(
        task_id='collect_trimmed_files_{0}'.format(analysis_name),
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
      params={'analysis_description_xcom_pull_task':'fetch_analysis_info_and_branch',
              'analysis_description_xcom_key':'analysis_description',
              'library_csv_xcom_key':'cellranger_library_csv',
              'library_csv_xcom_pull_task':'configure_cellranger_run',
              'cellranger_xcom_key':'cellranger_output',
              'cellranger_options':['--localcores 8','--localmem 64']},
      python_callable=run_cellranger_tool)
  ## PIPELINE
  configure_cellranger_run >> run_cellranger
  ## TASK
  decide_analysis_branch = \
    BranchPythonOperator(
      task_id='decide_analysis_branch',
      dag=dag,
      python_callable=decide_analysis_branch_func,
      params={'upload_report_to_ftp_task':'upload_report_to_ftp',
              'upload_report_to_box_task':'upload_report_to_box',
              'upload_results_to_irods_task':'upload_results_to_irods',
              'run_scanpy_for_sc_5p_task':'run_scanpy_for_sc_5p',
              'run_scirpy_for_vdj_task':'run_scirpy_for_vdj',
              'run_scirpy_for_vdj_b_task':'run_scirpy_for_vdj_b',
              'run_scirpy_vdj_t_task':'run_scirpy_vdj_t',
              'run_seurat_for_sc_5p_task':'run_seurat_for_sc_5p',
              'run_picard_alignment_summary_task':'run_picard_alignment_summary',
              'library_csv_xcom_key':'cellranger_library_csv',
              'library_csv_xcom_pull_task':'configure_cellranger_run'})
  ## PIPELINE
  run_cellranger >> decide_analysis_branch
  ## TASK
  upload_report_to_ftp = \
    DummyOperator(
      task_id='upload_report_to_ftp',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> upload_report_to_ftp
  ## TASK
  upload_report_to_box = \
    DummyOperator(
      task_id='upload_report_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> upload_report_to_box
  ## TASK
  upload_results_to_irods = \
    DummyOperator(
      task_id='upload_results_to_irods',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> upload_results_to_irods
  ## TASK
  run_scanpy_for_sc_5p = \
    DummyOperator(
      task_id='run_scanpy_for_sc_5p',
      dag=dag)
  upload_scanpy_report_for_sc_5p_to_ftp = \
    DummyOperator(
      task_id='upload_scanpy_report_for_sc_5p_to_ftp',
      dag=dag)
  upload_scanpy_report_for_sc_5p_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_sc_5p_to_box',
      dag=dag)
  run_cellbrowser_for_sc_5p = \
    DummyOperator(
      task_id='run_cellbrowser_for_sc_5p',
      dag=dag)
  upload_cellbrowser_for_sc_5p_to_ftp = \
    DummyOperator(
      task_id='upload_cellbrowser_for_sc_5p_to_ftp',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_scanpy_for_sc_5p
  run_scanpy_for_sc_5p >> upload_scanpy_report_for_sc_5p_to_ftp
  run_scanpy_for_sc_5p >> upload_scanpy_report_for_sc_5p_to_box
  run_scanpy_for_sc_5p >> run_cellbrowser_for_sc_5p
  run_cellbrowser_for_sc_5p >> upload_cellbrowser_for_sc_5p_to_ftp
  ## TASK
  run_scirpy_for_vdj = \
    DummyOperator(
      task_id='run_scirpy_for_vdj',
      dag=dag)
  upload_scanpy_report_for_vdj_to_ftp = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_to_ftp',
      dag=dag)
  upload_scanpy_report_for_vdj_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj
  run_scirpy_for_vdj >> upload_scanpy_report_for_vdj_to_ftp
  run_scirpy_for_vdj >> upload_scanpy_report_for_vdj_to_box
  ## TASK
  run_scirpy_for_vdj_b = \
    DummyOperator(
      task_id='run_scirpy_for_vdj_b',
      dag=dag)
  upload_scanpy_report_for_vdj_b_to_ftp = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_b_to_ftp',
      dag=dag)
  upload_scanpy_report_for_vdj_b_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_b_to_box',
      dag=dag)
  run_cellbrowser_for_vdj_b = \
    DummyOperator(
      task_id='run_cellbrowser_for_vdj_b',
      dag=dag)
  upload_cellbrowser_for_vdj_b_to_ftp = \
    DummyOperator(
      task_id='upload_cellbrowser_for_vdj_b_to_ftp',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj_b
  run_scirpy_for_vdj_b >> upload_scanpy_report_for_vdj_b_to_ftp
  run_scirpy_for_vdj_b >> upload_scanpy_report_for_vdj_b_to_box
  run_scirpy_for_vdj_b >> run_cellbrowser_for_vdj_b
  run_cellbrowser_for_vdj_b >> upload_cellbrowser_for_vdj_b_to_ftp
  ## TASK
  run_scirpy_for_vdj_t = \
    DummyOperator(
      task_id='run_scirpy_for_vdj_t',
      dag=dag)
  upload_scanpy_report_for_vdj_t_to_ftp = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_t_to_ftp',
      dag=dag)
  upload_scanpy_report_for_vdj_t_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_t_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj_t
  run_scirpy_for_vdj_t >> upload_scanpy_report_for_vdj_t_to_ftp
  run_scirpy_for_vdj_t >> upload_scanpy_report_for_vdj_t_to_box
  ## TASK
  run_seurat_for_sc_5p = \
    DummyOperator(
      task_id='run_seurat_for_sc_5p',
      dag=dag)
  upload_seurat_report_for_sc_5p_ftp = \
    DummyOperator(
      task_id='upload_seurat_report_for_sc_5p_ftp',
      dag=dag)
  upload_seurat_report_for_sc_5p_to_box = \
    DummyOperator(
      task_id='upload_seurat_report_for_sc_5p_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_seurat_for_sc_5p
  run_seurat_for_sc_5p >> upload_seurat_report_for_sc_5p_ftp
  run_seurat_for_sc_5p >> upload_seurat_report_for_sc_5p_to_box
  ## TASK
  run_picard_alignment_summary = \
    DummyOperator(
      task_id='run_picard_alignment_summary',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_picard_alignment_summary
  ## TASK
  run_picard_qual_summary = \
    DummyOperator(
      task_id='run_picard_qual_summary',
      dag=dag)
  ## PIPELINE
  run_picard_alignment_summary >> run_picard_qual_summary
  ## TASK
  run_picard_rna_summary = \
    DummyOperator(
      task_id='run_picard_rna_summary',
      dag=dag)
  ## PIPELINE
  run_picard_qual_summary >> run_picard_rna_summary
  ## TASK
  run_picard_gc_summary = \
    DummyOperator(
      task_id='run_picard_gc_summary',
      dag=dag)
  ## PIPELINE
  run_picard_rna_summary >> run_picard_gc_summary
  ## TASK
  run_samtools_stats = \
    DummyOperator(
      task_id='run_samtools_stats',
      dag=dag)
  ## PIPELINE
  run_picard_gc_summary >> run_samtools_stats
  ## TASK
  run_samtools_idxstats = \
    DummyOperator(
      task_id='run_samtools_idxstats',
      dag=dag)
  ## PIPELINE
  run_samtools_stats >> run_samtools_idxstats
  ## TASK
  run_multiqc = \
    DummyOperator(
      task_id='run_multiqc',
      dag=dag)
  ## PIPELINE
  run_samtools_idxstats >> run_multiqc
  ## TASK
  upload_multiqc_to_ftp = \
    DummyOperator(
      task_id='upload_multiqc_to_ftp',
      dag=dag)
  ## PIPELINE
  run_multiqc >> upload_multiqc_to_ftp
  ## TASK
  upload_multiqc_to_box = \
    DummyOperator(
      task_id='upload_multiqc_to_box',
      dag=dag)
  ## PIPELINE
  run_multiqc >> upload_multiqc_to_box
  ## TASK
  update_analysis_and_status = \
    DummyOperator(
      task_id='update_analysis_and_status',
      dag=dag)
  ## PIPELINE
  upload_multiqc_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_sc_5p_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_sc_5p_to_box >> update_analysis_and_status
  upload_cellbrowser_for_sc_5p_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_vdj_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_vdj_to_box >> update_analysis_and_status
  upload_scanpy_report_for_vdj_b_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_vdj_b_to_box >> update_analysis_and_status
  upload_scanpy_report_for_vdj_t_to_ftp >> update_analysis_and_status
  upload_scanpy_report_for_vdj_t_to_box >> update_analysis_and_status
  upload_seurat_report_for_sc_5p_ftp >> update_analysis_and_status
  upload_seurat_report_for_sc_5p_to_box >> update_analysis_and_status
  upload_results_to_irods >> update_analysis_and_status
  upload_report_to_ftp >> update_analysis_and_status
  upload_report_to_box >> update_analysis_and_status