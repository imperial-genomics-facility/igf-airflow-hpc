import os
from datetime import timedelta
from itertools import chain
import queue
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from igf_airflow.utils.dag22_bclconvert_demult_utils import find_seqrun_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import format_and_split_samplesheet_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import trigger_lane_jobs
from igf_airflow.utils.dag22_bclconvert_demult_utils import trigger_ig_jobs
from igf_airflow.utils.dag22_bclconvert_demult_utils import run_bclconvert_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import bclconvert_report_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import sample_known_qc_factory_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import calculate_fastq_md5_checksum_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import load_fastq_and_qc_to_db_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import fastqscreen_run_wrapper_for_known_samples_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import fastqc_run_wrapper_for_known_samples_func

## DEFAULTS
MAX_PROJECTS = 4
MAX_LANES = 4
MAX_INDEX_GROUPS = 2
MAX_SAMPLES = 20 # divide samples to 30 groups

## ARGS
args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 10}

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        default_args=args,
        default_view='graph',
        orientation='TB',
        tags=['hpc'])


with dag:
	## TASK
	find_seqrun = \
		BranchPythonOperator(
			task_id="find_seqrun",
			dag=dag,
			queue="hpc_4G",
			python_callable=find_seqrun_func)
	## TASK
	format_and_split_samplesheet = \
		BranchPythonOperator(
			task_id="format_and_split_samplesheet",
			dag=dag,
			queue="hpc_4G",
			params={
				'xcom_key': 'formatted_samplesheets',
				'project_task_prefix': 'dummy_demult_start_project_',
				'max_projects': MAX_PROJECTS},
			python_callable=format_and_split_samplesheet_func)
	## TASK
	mark_run_finished = \
		DummyOperator(
			task_id='mark_run_finished')
	## PIPELINE
	find_seqrun >> format_and_split_samplesheet
	find_seqrun >> mark_run_finished
	format_and_split_samplesheet >> mark_run_finished
	for project_id in range(1, MAX_PROJECTS+1):
		## TASK
		dummy_project_task = \
			DummyOperator(
				task_id='dummy_demult_start_project_{}'.format(project_id),
				dag=dag,
				queue="hpc_4G")
		## TASKGROUP PROJECT_SECTION
		with TaskGroup(
			"demultiplexing_of_project_{0}".\
			format(project_id),
			tooltip="Demultiplexing run for project {0}".\
					format(project_id)) \
			as project_section_demultiplexing:
			## TASK
			demult_pj_start = \
				BranchPythonOperator(
					task_id="demult_start_project_{0}".\
							format(project_id),
					dag=dag,
					queue="hpc_4G",
					params={
						"xcom_key": "formatted_samplesheets",
						"xcaom_task": "format_and_split_samplesheet",
						"project_index": project_id,
						"project_index_column": "project",
						"lane_index_column": "lane",
						"max_lanes": MAX_LANES,
						"lane_task_prefix": "demultiplexing_of_project_{0}_lane".\
											format(project_id)},
					python_callable=trigger_lane_jobs)
			## TASK
			demult_pj_finish = \
				DummyOperator(
					task_id="demult_finish_project_{0}".\
							format(project_id))
			for lane_id in range(1, MAX_LANES+1):
				## TASKGROUP POJECT_LANE_SECTION
				with TaskGroup(
					"demultiplexing_of_project_{0}_lane_{1}".\
					format(project_id, lane_id),
					tooltip="Demultiplexing run for project {0}, lane {1}".\
							format(project_id, lane_id)) \
					as lane_project_section_demultiplexing:
					## TASK
					demult_ln_start = \
						PythonOperator(
							task_id="demult_start_project_{0}_lane_{1}".\
									format(project_id, lane_id),
							dag=dag,
							queue="hpc_4G",
							params={
								"xcom_key": "formatted_samplesheets",
								"xcaom_task": "format_and_split_samplesheet",
								"project_index": project_id,
								"project_index_column": "project",
								"lane_index": lane_id,
								"lane_index_column": "lane",
								"ig_index_column": "index_group_index",
								"max_index_groups": MAX_INDEX_GROUPS,
								"ig_task_prefix": "bclconvert_project_{0}_lane_{1}_ig".\
													format(project_id, lane_id),
							},
							python_callable=trigger_ig_jobs)
					## TASK
					demult_ln_finish = \
						DummyOperator(
							task_id="demult_finish_project_{0}_lane_{1}".\
									format(project_id, lane_id))
					for ig_id in range(1, MAX_INDEX_GROUPS+1):
						## TASKGROUP INDEX_GROUP_POJECT_LANE_SECTION
						with TaskGroup(
							"demultiplexing_of_project_{0}_lane_{1}_ig_{2}".\
							format(project_id, lane_id, ig_id),
							tooltip="Demultiplexing run for project {0}, lane {1}, IG {2}".\
									format(project_id, lane_id, ig_id)) \
							as ig_project_lane_section_demultiplexing:
							## TASK
							bclconvert_ig = \
								PythonOperator(
									task_id="bclconvert_project_{0}_lane_{1}_ig_{2}".\
											format(project_id, lane_id, ig_id),
									dag=dag,
									queue="hpc_64G16t",
									params={
										'xcom_key': 'formatted_samplesheets',
										'xcom_task': 'format_and_split_samplesheet',
										'xcom_key_for_reports': 'bclconvert_reports',
										'xcom_key_for_output': 'bclconvert_output',
										'project_index_column': 'project_index',
										'lane_index_column': 'lane_index',
										'ig_index_column': 'index_group_index',
										'project_column': 'project',
										'lane_column': 'lane',
										'index_group_column': 'index_group',
										'project_index': project_id,
										'lane_index': lane_id,
										'ig_index': ig_id,
										'samplesheet_column': 'samplesheet_file',
										'bcl_num_conversion_threads': 4,
										'bcl_num_compression_threads': 2,
										'bcl_num_decompression_threads': 2,
										'bcl_num_parallel_tiles': 4
									},
									python_callable=run_bclconvert_func)
							## TASK
							demult_report = \
								PythonOperator(
									task_id="demult_report_project_{0}_lane_{1}_ig_{2}".\
											format(project_id, lane_id, ig_id),
									dag=dag,
									queue="hpc_4G",
									params={
										'xcom_key_for_reports': "bclconvert_reports",
										'xcom_task_for_reports': "bclconvert_project_{0}_lane_{1}_ig_{2}".\
																	format(project_id, lane_id, ig_id)
									},
									python_callable=bclconvert_report_func)
							## TASK
							multiqc_report = \
								DummyOperator(
									task_id="multiqc_project_{0}_lane_{1}_ig_{2}".\
											format(project_id, lane_id, ig_id))
							## TASK
							sample_qc_page = \
								DummyOperator(
									task_id="sample_qc_page_project_{0}_lane_{1}_ig_{2}".\
											format(project_id, lane_id, ig_id))
							## TASKGROUP - QC KNOWN
							with TaskGroup(
								"qc_ig_{0}_lane_{1}_project_{2}".\
								format(ig_id, lane_id, project_id),
								tooltip="QC of project {0}, lane {1}, IG {2}".\
										format(project_id, lane_id, ig_id)) \
								as qc_known:
                                ## TASK
								sample_groups_ig = \
									PythonOperator(
										task_id="sample_groups_project_{0}_lane_{1}_ig_{2}".\
												format(project_id, lane_id, ig_id),
										dag=dag,
										queue="hpc_4G",
										params={
											'xcom_key_for_bclconvert_output': 'bclconvert_output',
											'xcom_task_for_bclconvert_output': "bclconvert_project_{0}_lane_{1}_ig_{2}".\
																				format(project_id, lane_id, ig_id),
											'xcom_key_for_sample_group': 'sample_group',
											'samplesheet_file_suffix': "Reports/SampleSheet.csv",
											'max_samples': MAX_SAMPLES,
											'project_index': project_id,
											'lane_index': lane_id,
											'ig_index': ig_id,
											'next_task_prefix': "md5_known_ig_{0}_lane_{1}_project_{2}_sample_".\
																	format(ig_id, lane_id, project_id)
										},
										python_callable=sample_known_qc_factory_func)
								## TASK
								sample_groups_finished_ig = \
									DummyOperator(
										task_id="sample_groups_finished_project_{0}_lane_{1}_ig_{2}".\
												format(project_id, lane_id, ig_id))
								for sample_id in range(1, MAX_SAMPLES+1):
									## TASK
									md5_calc = \
										PythonOperator(
											task_id="md5_known_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
													format(ig_id, lane_id, project_id, sample_id),
											dag=dag,
											queue="hpc_4G",
											params={
												'xcom_key_for_bclconvert_output': 'bclconvert_output',
												'xcom_task_for_bclconvert_output': "bclconvert_project_{0}_lane_{1}_ig_{2}".\
																					format(project_id, lane_id, ig_id),
												'xcom_key_for_sample_group': 'sample_group',
												'xcom_task_for_sample_group': "sample_groups_project_{0}_lane_{1}_ig_{2}".\
																			format(project_id, lane_id, ig_id),
												"xcom_key_for_checksum_sample_group": "checksum_sample_group",
												'samplesheet_file_suffix': "Reports/SampleSheet.csv",
												'project_index': project_id,
												'lane_index': lane_id,
												'ig_index': ig_id,
												'sample_group_id': sample_id
											},
											python_callable=calculate_fastq_md5_checksum_func)
									## TASK
									load_fastq_and_qc_to_db = \
										PythonOperator(
											task_id="load_fastq_and_qc_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
													format(ig_id, lane_id, project_id, sample_id),
											dag=dag,
											queue="hpc_4G",
											params={
												"xcom_key_for_checksum_sample_group": "checksum_sample_group",
												"xcom_task_for_checksum_sample_group": "md5_known_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
																						format(ig_id, lane_id, project_id, sample_id),
												"xcom_key_for_collection_group": "collection_group",
												"project_index_column": "project_index",
												"lane_index_column": "lane_index",
												"lane_index": lane_id,
												'ig_index_column': 'index_group_index',
												'ig_index': ig_id,
												'index_group_column': 'index_group',
											},
											python_callable=load_fastq_and_qc_to_db_func)
									## TASK
									fq_known = \
										PythonOperator(
											task_id="fastqc_known_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
													format(ig_id, lane_id, project_id, sample_id),
											dag=dag,
											queue="hpc_4G",
											params={
												'xcom_key_for_bclconvert_output': 'bclconvert_output',
												'xcom_task_for_bclconvert_output': "bclconvert_project_{0}_lane_{1}_ig_{2}".\
																				   format(project_id, lane_id, ig_id),
												"xcom_key_for_collection_group": "collection_group",
												"xcom_task_for_collection_group": "load_fastq_and_qc_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
																				  format(ig_id, lane_id, project_id, sample_id),
											},
											python_callable=fastqc_run_wrapper_for_known_samples_func)
									## TASK
									fqs_known = \
										PythonOperator(
											task_id="fastqscreen_known_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
													format(ig_id, lane_id, project_id, sample_id),
											dag=dag,
											queue="hpc_4G",
											params={
												'xcom_key_for_bclconvert_output': 'bclconvert_output',
												'xcom_task_for_bclconvert_output': "bclconvert_project_{0}_lane_{1}_ig_{2}".\
																				   format(project_id, lane_id, ig_id),
												"xcom_key_for_collection_group": "collection_group",
												"xcom_task_for_collection_group": "load_fastq_and_qc_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
																				  format(ig_id, lane_id, project_id, sample_id),
											},
											python_callable=fastqscreen_run_wrapper_for_known_samples_func)
									## TASK
									upload_fastq_to_irods = \
										DummyOperator(
											task_id="upload_fastq_to_irods_ig_{0}_lane_{1}_project_{2}_sample_{3}".\
													format(ig_id, lane_id, project_id, sample_id))
									##PIPELINE
									sample_groups_ig >> md5_calc >> load_fastq_and_qc_to_db
									load_fastq_and_qc_to_db >> fq_known >> sample_groups_finished_ig
									load_fastq_and_qc_to_db >> fqs_known >> sample_groups_finished_ig
									load_fastq_and_qc_to_db >> upload_fastq_to_irods >> sample_groups_finished_ig
							## TASKGROUP - QC UNKNOWN
							with TaskGroup(
								"qc_unknown_ig_{0}_lane_{1}_project_{2}".\
								format(ig_id, lane_id, project_id),
								tooltip="QC of project {0}, lane {1}, IG {2}".\
										format(project_id, lane_id, ig_id)) \
								as qc_unknown:
								## TASK
								fastqc_unknown = \
									DummyOperator(
										task_id="fastqc_unknown_ig_{0}_lane_{1}_project_{2}".\
												format(ig_id, lane_id, project_id))
								## TASK
								fastqscreen_unknown = \
									DummyOperator(
										task_id="fastqscreen_unknown_ig_{0}_lane_{1}_project_{2}".\
												format(ig_id, lane_id, project_id))
								## TASK
								multiqc_unknown = \
									DummyOperator(
										task_id="multiqc_unknown_ig_{0}_lane_{1}_project_{2}".\
												format(ig_id, lane_id, project_id))
								##PIPELINE
								fastqc_unknown >> multiqc_unknown
								fastqscreen_unknown >> multiqc_unknown
							## PIPELINE
							bclconvert_ig >> demult_report >> sample_qc_page
							bclconvert_ig >> qc_known >> multiqc_report >> sample_qc_page
							bclconvert_ig >> qc_unknown >> sample_qc_page
						## PIPELINE
						demult_ln_start >> ig_project_lane_section_demultiplexing >> demult_ln_finish
					## PIPELINE
					demult_pj_start >> demult_ln_start
					demult_ln_finish >> demult_pj_finish	
			## TASK
			setup_qc_page = \
				DummyOperator(
					task_id="setup_qc_page_for_{0}".\
							format(project_id))
			## TASK
			send_email = \
				DummyOperator(
					task_id="send_email_for_{0}".\
							format(project_id))
			## PIPELINE
			demult_pj_finish >> setup_qc_page >> send_email
		## PIPELINE
		format_and_split_samplesheet >> dummy_project_task >> project_section_demultiplexing >> mark_run_finished
