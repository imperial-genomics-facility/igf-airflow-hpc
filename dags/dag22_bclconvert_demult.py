import os
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from igf_airflow.utils.dag22_bclconvert_demult_utils import find_seqrun_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import mark_seqrun_status_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_samplesheet_from_portal_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import format_and_split_samplesheet_func

sample_groups = {
    1: { 			# project 1 index
        1: { 		# lane 1 index
            1: 10,	# ig group 1 index with 100 samples
        },
        2: {		# lane 2 index
            1: 10,	# ig group 1 index with 100 samples
        }
    }
}

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
    'max_active_runs': 1}

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
        tags=['hpc', 'dynamic'])

with dag:
	## TASK
    find_seqrun = \
        BranchPythonOperator(
            task_id="find_seqrun",
            dag=dag,
            queue="hpc_4G",
            params={
                'next_task_id': 'mark_seqrun_as_running',
                'no_work_task': 'no_work'
            },
            python_callable=find_seqrun_func)
    ## TASK
    mark_seqrun_as_running = \
        BranchPythonOperator(
            task_id="mark_seqrun_as_running",
            dag=dag,
            queue="hpc_4G",
            params={
                'next_task': 'fetch_samplesheet_for_run',
                'last_task': 'no_work',
                'seed_status': 'RUNNING',
                'no_change_status': 'RUNNING',
                'seed_table': 'seqrun',
                'check_all_pipelines_for_seed_id': True
            },
            python_callable=mark_seqrun_status_func)
    ## TASK
    fetch_samplesheet_for_run = \
        PythonOperator(
            task_id="fetch_samplesheet_for_run",
            dag=dag,
            queue='hpc_4G',
            params={
                'samplesheet_xcom_key': 'samplesheet_data',
            },
            python_callable=get_samplesheet_from_portal_func)
    ## TASK
    format_and_split_samplesheet = \
        BranchPythonOperator(
			task_id="format_and_split_samplesheet",
			dag=dag,
			queue="hpc_4G",
			params={
				'xcom_key': 'formatted_samplesheets',
				'project_task_prefix': 'setup_qc_page_for_project_',
				'max_projects': len(sample_groups)},
			python_callable=format_and_split_samplesheet_func)
    ## TASK
    no_work = \
        DummyOperator(
            task_id="no_work")
    ## PIPELINE
    find_seqrun >> mark_seqrun_as_running
    find_seqrun >> no_work
    mark_seqrun_as_running >> fetch_samplesheet_for_run
    mark_seqrun_as_running >> no_work
    fetch_samplesheet_for_run >> format_and_split_samplesheet
    ## TASK
    mark_seqrun_as_finished = \
        PythonOperator(
            task_id="mark_seqrun_as_finished",
            dag=dag,
            queue="hpc_4G",
            params={
                'seed_status': 'FINISHED',
                'no_change_status': 'SEEDED',
                'seed_table': 'seqrun'
            },
            python_callable=mark_seqrun_status_func
    ## LOOP - PROJECT
    for project_id in sample_groups:
        ## TASK - PROJECT
        setup_qc_page_for_project = \
            DummyOperator(
                task_id=f"setup_qc_page_for_project_{project_id}")
        ## TASK - PROJECT
        setup_globus_transfer_for_project = \
            DummyOperator(
                task_id=f"setup_globus_transfer_for_project_{project_id}")
        ## TASK - PROJECT
        get_lanes_for_project = \
            DummyOperator(
                task_id=f"get_lanes_for_project_{project_id}")
        ## TASK - PROJECT
        build_qc_page_for_project = \
            DummyOperator(
                task_id=f"build_qc_page_for_project_{project_id}")
        ## TASK - PROJECT
        send_email_to_user_for_project = \
            DummyOperator(
                task_id=f"send_email_to_user_for_project_{project_id}")
        ## PIPELINE - PROJECT
        format_and_split_samplesheet >> setup_qc_page_for_project
        setup_qc_page_for_project >> setup_globus_transfer_for_project
        setup_globus_transfer_for_project >> get_lanes_for_project
        build_qc_page_for_project >> send_email_to_user_for_project
        send_email_to_user_for_project >> mark_seqrun_as_finished
        ## LOOP - LANE
        for lane_id in sample_groups.get(project_id):
            ## TASK - LANE
            get_igs_for_project_lane = \
                DummyOperator(
                    task_id=f"get_igs_for_project_{project_id}_lane_{lane_id}")
            ## TASK - LANE
            build_qc_page_for_project_lane = \
                DummyOperator(
                    task_id=f"build_qc_page_for_project_{project_id}_lane_{lane_id}")
            ## PIPELINE - LANE
            get_lanes_for_project >> get_igs_for_project_lane
            build_qc_page_for_project_lane >> build_qc_page_for_project
             ## LOOP - INDEXGROUP
            for index_id in sample_groups.get(project_id).get(lane_id):
                ## TASK - INDEXGROUP
                bclconvert_for_project_lane_index_group = \
                    DummyOperator(
                        task_id=f"bclconvert_for_project_{project_id}_lane_{lane_id}_index_group_{index_id}")
                ## TASK - INDEXGROUP
                generate_demult_report_for_project_lane_index_group = \
                    DummyOperator(
                        task_id=f"generate_demult_report_for_project_{project_id}_lane_{lane_id}_index_group_{index_id}")
                ## TASK - INDEXGROUP
                check_output_for_project_lane_index_group = \
                    DummyOperator(
                        task_id=f"check_output_for_project_{project_id}_lane_{lane_id}_index_group_{index_id}")
                ## TASK - INDEXGROUP
                multiqc_for_project_lane_index_group = \
                    DummyOperator(
                        task_id=f"multiqc_for_project_{project_id}_lane_{lane_id}_index_group_{index_id}")
                ## PIPELINE - INDEXGROUP
                get_igs_for_project_lane >> bclconvert_for_project_lane_index_group
                bclconvert_for_project_lane_index_group >> generate_demult_report_for_project_lane_index_group
                generate_demult_report_for_project_lane_index_group >> check_output_for_project_lane_index_group
                multiqc_for_project_lane_index_group >> build_qc_page_for_project_lane
                ## TASKGROUP - SAMPLE
                with TaskGroup(group_id=f'sample_group_{project_id}_{lane_id}_{index_id}') as sample_group:
                    ## LOOP - SAMPLE
                    for sample_id in range(1, sample_groups.get(project_id).get(lane_id).get(index_id) + 1):
                        ## TASK - SAMPLE
                        load_fastq_to_db = \
                            DummyOperator(
                                task_id=f"load_fastq_to_db_project_{project_id}_lane_{lane_id}_index_group_{index_id}_sample_{sample_id}")
                        ## TASK - SAMPLE
                        copy_fastq_to_irods = \
                            DummyOperator(
                                task_id=f"copy_fastq_to_irods_project_{project_id}_lane_{lane_id}_index_group_{index_id}_sample_{sample_id}")
                        ## TASK - SAMPLE
                        copy_fastq_to_globus = \
                            DummyOperator(
                                task_id=f"copy_fastq_to_globus_project_{project_id}_lane_{lane_id}_index_group_{index_id}_sample_{sample_id}")
                        ## TASK - SAMPLE
                        fastqc = \
                            DummyOperator(
                                task_id=f"fastqc_project_{project_id}_lane_{lane_id}_index_group_{index_id}_sample_{sample_id}")
                        ## TASK - SAMPLE
                        load_fastqc = \
                            DummyOperator(
                                task_id=f"load_fastqc_project_{project_id}_lane_{lane_id}_index_group_{index_id}_sample_{sample_id}")
                        ## TASK - SAMPLE
                        fastq_screen = \
                            DummyOperator(
                                task_id=f"fastq_screen_project_{project_id}_lane_{lane_id}_index_group_{index_id}_sample_{sample_id}")
                        # TASK - SAMPLE
                        load_fastq_screen = \
                            DummyOperator(
                                task_id=f"load_fastq_screen_project_{project_id}_lane_{lane_id}_index_group_{index_id}_sample_{sample_id}")
                        ## PIPELINE - SAMPLE
                        load_fastq_to_db >> copy_fastq_to_irods
                        load_fastq_to_db >> copy_fastq_to_globus
                        load_fastq_to_db >> fastqc
                        fastqc >> load_fastqc
                        load_fastqc >> fastq_screen
                        fastq_screen >> load_fastq_screen
                    check_output_for_project_lane_index_group >> sample_group
                    sample_group >> multiqc_for_project_lane_index_group