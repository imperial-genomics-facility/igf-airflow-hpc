import os
from datetime import timedelta
from itertools import chain
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

## DEFAULTS
MAX_PROJECTS = 4
MAX_LANES = 4
MAX_INDEX_GROUPS = 2
MAX_SAMPLES = 30 # divide samples to 30 groups

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
		DummyOperator(
			task_id="find_seqrun")
	## TASK
	format_and_split_samplesheet = \
		DummyOperator(
			task_id="format_and_split_samplesheet")
	## TASK
	mark_run_finished = \
		DummyOperator(
			task_id='mark_run_finished')
	## PIPELINE
	find_seqrun >> format_and_split_samplesheet
	for project_id in range(1, MAX_PROJECTS+1):
		## TASKGROUP PROJECT_SECTION
		with TaskGroup(
			"demultiplexing_of_project_{0}".format(project_id),
			tooltip="Demultiplexing run for project {0}".format(project_id)) \
			as project_section_demultiplexing:
			## TASK
			demult_pj_start = \
				DummyOperator(
					task_id="demult_start_project_{0}".format(project_id))
			## TASK
			demult_pj_finish = \
				DummyOperator(
					task_id="demult_finish_project_{0}".format(project_id))
			for lane_id in range(1, MAX_LANES+1):
				## TASKGROUP POJECT_LANE_SECTION
				with TaskGroup(
					"demultiplexing_of_project_{0}_lane_{1}".format(project_id, lane_id),
					tooltip="Demultiplexing run for project {0}, lane {1}".format(project_id, lane_id)) \
					as lane_project_section_demultiplexing:
					## TASK
					demult_ln_start = \
						DummyOperator(
							task_id="demult_start_project_{0}_lane_{1}".format(project_id, lane_id))
					## TASK
					demult_ln_finish = \
						DummyOperator(
							task_id="demult_finish_project_{0}_lane_{1}".format(project_id, lane_id))
					for ig_id in range(1, MAX_INDEX_GROUPS+1):
						## TASKGROUP INDEX_GROUP_POJECT_LANE_SECTION
						with TaskGroup(
							"demultiplexing_of_project_{0}_lane_{1}_ig_{2}".format(project_id, lane_id, ig_id),
							tooltip="Demultiplexing run for project {0}, lane {1}, IG {2}".format(project_id, lane_id, ig_id)) \
							as ig_project_lane_section_demultiplexing:
							## TASK
							bclconvert_ig = \
								DummyOperator(
									task_id="bclconvert_project_{0}_lane_{1}_ig_{2}".format(project_id, lane_id, ig_id))
							## TASK
							demult_report = \
								DummyOperator(
									task_id="demult_report_project_{0}_lane_{1}_ig_{2}".format(project_id, lane_id, ig_id))
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
								"qc_ig_{0}_lane_{1}_project_{2}".format(ig_id, lane_id, project_id),
								tooltip="QC of project {0}, lane {1}, IG {2}".format(project_id, lane_id, ig_id)) \
								as qc_known:
								for sample_id in range(1, MAX_SAMPLES+1):
									## TASK
									fq_known = \
										DummyOperator(
											task_id="fastqc_known_sample_{0}_ig_{1}_lane_{2}_project_{3}".\
													format(sample_id, ig_id, lane_id, project_id))
									## TASK
									fqs_known = \
										DummyOperator(
											task_id="fastqscreen_known_sample_{0}_ig_{1}_lane_{2}_project_{3}".\
													format(sample_id, ig_id, lane_id, project_id))
									## TASK
									md5_calc = \
										DummyOperator(
											task_id="md5_known_sample_{0}_ig_{1}_lane_{2}_project_{3}".\
													format(sample_id, ig_id, lane_id, project_id))
									## TASK
									load_fastq_and_qc_to_db = \
										DummyOperator(
											task_id="load_fastq_and_qc_sample_{0}_ig_{1}_lane_{2}_project_{3}".\
													format(sample_id, ig_id, lane_id, project_id))
									## TASK
									upload_fastq_to_irods = \
										DummyOperator(
											task_id="upload_fastq_to_irods_{0}_ig_{1}_lane_{2}_project_{3}".\
													format(sample_id, ig_id, lane_id, project_id))
									##PIPELINE
									fq_known >> fqs_known >> md5_calc >> load_fastq_and_qc_to_db >> upload_fastq_to_irods
							## TASKGROUP - QC UNKNOWN
							with TaskGroup(
								"qc_unknown_ig_{0}_lane_{1}_project_{2}".format(ig_id, lane_id, project_id),
								tooltip="QC of project {0}, lane {1}, IG {2}".format(project_id, lane_id, ig_id)) \
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
					task_id="setup_qc_page_for_{0}".format(project_id))
			## TASK
			send_email = \
				DummyOperator(
					task_id="send_email_for_{0}".format(project_id))
			## PIPELINE
			demult_pj_finish >> setup_qc_page >> send_email
		## PIPELINE
		format_and_split_samplesheet >> project_section_demultiplexing >> mark_run_finished
