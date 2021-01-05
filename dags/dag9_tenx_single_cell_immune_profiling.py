from datetime import timedelta
import os,json,logging,subprocess
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from igf_airflow.logging.upload_log_msg import send_log_to_channels,log_success,log_failure,log_sleep
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
      params={'load_cellranger_result_to_db_task':'load_cellranger_result_to_db',
              'run_scanpy_for_sc_5p_task':'run_scanpy_for_sc_5p',
              'run_scirpy_for_vdj_task':'run_scirpy_for_vdj',
              'run_scirpy_for_vdj_b_task':'run_scirpy_for_vdj_b',
              'run_scirpy_vdj_t_task':'run_scirpy_vdj_t',
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
      python_callable=load_cellranger_result_to_db_func,
      params={'analysis_description_xcom_pull_task':'fetch_analysis_info_and_branch',
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
  upload_report_to_ftp = \
    PythonOperator(
      task_id='upload_report_to_ftp',
      dag=dag,
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_cellranger_result_to_db',
              'xcom_pull_files_key':'html_report_file',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_CELLRANGER_MULTI',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_report_to_box = \
    DummyOperator(
      task_id='upload_report_to_box',
      dag=dag,
      params={'xcom_pull_task':'load_cellranger_result_to_db',
              'xcom_pull_files_key':'html_report_file'})
  upload_results_to_irods = \
    PythonOperator(
      task_id='upload_results_to_irods',
      dag=dag,
      python_callable=irods_files_upload_for_analysis,
      params={'xcom_pull_task':'load_cellranger_result_to_db',
              'xcom_pull_files_key':'loaded_output_files',
              'collection_name_key':'sample_igf_id',
              'analysis_name':'cellranger_multi'})
  ## PIPELINE
  decide_analysis_branch >> load_cellranger_result_to_db
  load_cellranger_result_to_db >> upload_report_to_ftp
  load_cellranger_result_to_db >> upload_report_to_box
  load_cellranger_result_to_db >> upload_results_to_irods
  ## TASK
  run_scanpy_for_sc_5p = \
    PythonOperator(
      task_id='run_scanpy_for_sc_5p',
      dag=dag,
      python_callable=run_scanpy_for_sc_5p_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'output_notebook_key':'scanpy_notebook',
              'output_cellbrowser_key':'cellbrowser_dirs',
              'analysis_description_xcom_pull_task':'fetch_analysis_info_and_branch',
              'analysis_description_xcom_key':'analysis_description'})
  load_scanpy_report_for_sc_5p_to_db = \
    PythonOperator(
      task_id='load_scanpy_report_for_sc_5p_to_db',
      dag=dag,
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
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_sc_5p_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCANPY_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scanpy_report_for_sc_5p_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_sc_5p_to_box',
      dag=dag)
  upload_cellbrowser_for_sc_5p_to_ftp = \
    DummyOperator(
      task_id='upload_cellbrowser_for_sc_5p_to_ftp',
      dag=dag,
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
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'python3',
              'vdj_dir':'vdj',
              'count_dir':'count',
              'cell_marker_list':Variable.get('all_cell_marker_list'),
              'output_notebook_key':'scirpy_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info_and_branch',
              'analysis_description_xcom_key':'analysis_description',
              'template_ipynb_path':Variable.get('scirpy_single_sample_template'),
              'singularity_image_path':Variable.get('scirpy_notebook_image')})
  load_scanpy_report_for_vdj_to_db = \
    PythonOperator(
      task_id='load_scanpy_report_for_vdj_to_db',
      dag=dag,
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_scirpy_for_vdj',
              'file_name_key':'scirpy_notebook',
              'analysis_name':'scirpy_vdj',
              'collection_type':'SCIRPY_VDJ_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_scanpy_report_for_vdj_to_ftp = \
    PythonOperator(
      task_id='upload_scanpy_report_for_vdj_to_ftp',
      dag=dag,
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCIRPY_VDJ_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scanpy_report_for_vdj_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj
  run_scirpy_for_vdj >> load_scanpy_report_for_vdj_to_db
  load_scanpy_report_for_vdj_to_db >> upload_scanpy_report_for_vdj_to_ftp
  load_scanpy_report_for_vdj_to_db >> upload_scanpy_report_for_vdj_to_box
  ## TASK
  run_scirpy_for_vdj_b = \
    DummyOperator(
      task_id='run_scirpy_for_vdj_b',
      dag=dag,
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'python3',
              'vdj_dir':'vdj_b',
              'count_dir':'count',
              'cell_marker_list':Variable.get('all_cell_marker_list'),
              'output_notebook_key':'scirpy_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info_and_branch',
              'analysis_description_xcom_key':'analysis_description',
              'template_ipynb_path':Variable.get('scirpy_single_sample_template'),
              'singularity_image_path':Variable.get('scirpy_notebook_image')})
  load_scanpy_report_for_vdj_b_to_db = \
    PythonOperator(
      task_id='load_scanpy_report_for_vdj_b_to_db',
      dag=dag,
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_scirpy_for_vdj_b',
              'file_name_key':'scirpy_notebook',
              'analysis_name':'scirpy_vdj_b',
              'collection_type':'SCIRPY_VDJ_B_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_scanpy_report_for_vdj_b_to_ftp = \
    PythonOperator(
      task_id='upload_scanpy_report_for_vdj_b_to_ftp',
      dag=dag,
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_b_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCIRPY_VDJ_B_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scanpy_report_for_vdj_b_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_b_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj_b
  run_scirpy_for_vdj_b >> load_scanpy_report_for_vdj_b_to_db
  load_scanpy_report_for_vdj_b_to_db >> upload_scanpy_report_for_vdj_b_to_ftp
  load_scanpy_report_for_vdj_b_to_db >> upload_scanpy_report_for_vdj_b_to_box
  ## TASK
  run_scirpy_for_vdj_t = \
    DummyOperator(
      task_id='run_scirpy_for_vdj_t',
      dag=dag,
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'python3',
              'vdj_dir':'vdj_t',
              'count_dir':'count',
              'cell_marker_list':Variable.get('all_cell_marker_list'),
              'output_notebook_key':'scirpy_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info_and_branch',
              'analysis_description_xcom_key':'analysis_description',
              'template_ipynb_path':Variable.get('scirpy_single_sample_template'),
              'singularity_image_path':Variable.get('scirpy_notebook_image')})
  load_scanpy_report_for_vdj_t_to_db = \
    PythonOperator(
      task_id='load_scanpy_report_for_vdj_t_to_db',
      dag=dag,
      python_callable=load_analysis_files_func,
      params={'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'file_name_task':'run_scirpy_for_vdj_t',
              'file_name_key':'scirpy_notebook',
              'analysis_name':'scirpy_vdj_t',
              'collection_type':'SCIRPY_VDJ_T_HTML',
              'collection_table':'sample',
              'output_files_key':'output_db_files'})
  upload_scanpy_report_for_vdj_t_to_ftp = \
    PythonOperator(
      task_id='upload_scanpy_report_for_vdj_t_to_ftp',
      dag=dag,
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_scanpy_report_for_vdj_t_to_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SCIRPY_VDJ_T_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_scanpy_report_for_vdj_t_to_box = \
    DummyOperator(
      task_id='upload_scanpy_report_for_vdj_t_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_scirpy_for_vdj_t
  run_scirpy_for_vdj_t >> load_scanpy_report_for_vdj_t_to_db
  load_scanpy_report_for_vdj_t_to_db >> upload_scanpy_report_for_vdj_t_to_ftp
  load_scanpy_report_for_vdj_t_to_db >> upload_scanpy_report_for_vdj_t_to_box
  ## TASK
  run_seurat_for_sc_5p = \
    PythonOperator(
      task_id='run_seurat_for_sc_5p',
      dag=dag,
      python_callable=run_singlecell_notebook_wrapper_func,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger',
              'scanpy_timeout':1200,
              'allow_errors':False,
              'kernel_name':'R',
              'vdj_dir':'vdj',
              'count_dir':'count',
              'cell_marker_list':Variable.get('all_cell_marker_list'),
              'output_notebook_key':'seurat_notebook',
              'analysis_description_xcom_pull_task':'fetch_analysis_info_and_branch',
              'analysis_description_xcom_key':'analysis_description',
              'template_ipynb_path':Variable.get('seurat_single_sample_template'),
              'singularity_image_path':Variable.get('seurat_notebook_image')})
  load_seurat_report_for_sc_5p_db = \
    PythonOperator(
      task_id='load_seurat_report_for_sc_5p_db',
      dag=dag,
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
    DummyOperator(
      task_id='upload_seurat_report_for_sc_5p_ftp',
      dag=dag,
      python_callable=ftp_files_upload_for_analysis,
      params={'xcom_pull_task':'load_seurat_report_for_sc_5p_db',
              'xcom_pull_files_key':'output_db_files',
              'collection_name_task':'load_cellranger_result_to_db',
              'collection_name_key':'sample_igf_id',
              'collection_type':'FTP_SEURAT_HTML',
              'collection_table':'sample',
              'collect_remote_file':True})
  upload_seurat_report_for_sc_5p_to_box = \
    DummyOperator(
      task_id='upload_seurat_report_for_sc_5p_to_box',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> run_seurat_for_sc_5p
  run_seurat_for_sc_5p >> load_seurat_report_for_sc_5p_db
  load_seurat_report_for_sc_5p_db >> upload_seurat_report_for_sc_5p_ftp
  load_seurat_report_for_sc_5p_db >> upload_seurat_report_for_sc_5p_to_box
  ## TASK
  convert_bam_to_cram = \
    DummyOperator(
      task_id='convert_bam_to_cram',
      dag=dag,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger'})
  upload_cram_to_irods = \
    DummyOperator(
      task_id='upload_cram_to_irods',
      dag=dag)
  ## PIPELINE
  decide_analysis_branch >> convert_bam_to_cram
  convert_bam_to_cram >> upload_cram_to_irods
  ## TASK
  run_picard_alignment_summary = \
    DummyOperator(
      task_id='run_picard_alignment_summary',
      dag=dag,
      params={'cellranger_xcom_key':'cellranger_output',
              'cellranger_xcom_pull_task':'run_cellranger'})
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
  upload_cram_to_irods >> update_analysis_and_status