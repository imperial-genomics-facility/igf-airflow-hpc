import os
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

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
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        default_args=args,
        orientation='LR',
        tags=['hpc'])
## DAG
with dag:
    ## TASK
    check_finish_status_for_seqrun = \
        DummyOperator(
            task_id="check_finish_status_for_seqrun",
            dag=dag)
    ## TASK
    lane_level_start_tasks = list()
    lane_level_end_tasks = list()
    for lane_id in range(1,9):
        ## TASK - lane level
        fetch_projects_for_lane = \
            DummyOperator(
                task_id="fetch_projects_for_lane_{0}".format(lane_id),
                dag=dag)
        lane_level_start_tasks.\
            append(fetch_projects_for_lane)
        project_level_start_tasks = list()
        project_level_end_tasks = list()
        for project_id in range(1,6):
            ## TASK - project level
            fetch_samples_for_lane_project = \
                DummyOperator(
                    task_id="fetch_fastqs_for_lane_{0}_project_{1}".format(lane_id, project_id),
                    dag=dag)
            project_level_start_tasks.\
                append(fetch_samples_for_lane_project)
            sample_level_tasks = list()
            for sample_count in range(1,201):
                ## TASK - sample level
                run_fastq_screen_for_sample = \
                    DummyOperator(
                        task_id="run_fastq_screen_for_lane_{0}_project_{1}_sample_{2}".format(lane_id, project_id, sample_count),
                        dag=dag)
                sample_level_tasks.\
                    append(run_fastq_screen_for_sample)
            ## TASK - collect all samples for project
            collect_all_samples_for_project = \
                DummyOperator(
                    task_id="collect_output_for_lane_{0}_project_{1}".format(lane_id, project_id),
                    dag=dag)
            project_level_end_tasks.\
                append(collect_all_samples_for_project)
            ## PIPELINE
            fetch_samples_for_lane_project >> sample_level_tasks
            sample_level_tasks >> collect_all_samples_for_project
        ## TASK - collect all project for lane
        collect_output_for_all_project_for_lane = \
            DummyOperator(
                task_id="collect_output_for_lane_{0}".format(lane_id),
                dag=dag)
        lane_level_end_tasks.\
            append(collect_output_for_all_project_for_lane)
        ## PIPELINE
        fetch_projects_for_lane >> project_level_start_tasks
        project_level_end_tasks >> collect_output_for_all_project_for_lane
    ## TASK - collect all lane for flowcell
    collect_all_output_for_flowcell = \
        DummyOperator(
            task_id="collect_all_output_for_flowcell",
            dag=dag)
    ## PIPELINE
    check_finish_status_for_seqrun >> lane_level_start_tasks
    lane_level_end_tasks >> collect_all_output_for_flowcell
    ## TASK
    merge_all_output_and_create_report = \
        DummyOperator(
            task_id="merge_all_output_and_create_report",
            dag=dag)
    ## TASK
    upload_report_to_box = \
        DummyOperator(
            task_id="upload_report_to_box",
            dag=dag)
    ## PIPELINE
    collect_all_output_for_flowcell >> merge_all_output_and_create_report
    merge_all_output_and_create_report >> upload_report_to_box