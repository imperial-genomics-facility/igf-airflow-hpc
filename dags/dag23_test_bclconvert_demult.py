import os
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import get_samplesheet_from_portal_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import mark_seqrun_status_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import get_formatted_samplesheets_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import bcl_convert_run_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import generate_report_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import upload_report_to_box_func

## DEFAULTS
MAX_SAMPLESHEETS = 20

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
    get_samplesheet_from_portal = \
        DummyOperator(
            task_id='get_samplesheet_from_portal')
    # TASK
    mark_seqrun_running = \
        DummyOperator(
            task_id='mark_seqrun_running')
    # TASK
    mark_seqrun_finished = \
        DummyOperator(
            task_id='mark_seqrun_finished')
    # TASK
    get_formatted_samplesheets = \
        DummyOperator(
            task_id='get_formatted_samplesheets')
    for samplesheet_id in range(0, MAX_SAMPLESHEETS):
        # TASK
        bcl_convert_run = \
            DummyOperator(
                task_id=f'bcl_convert_run_{samplesheet_id}')
        # TASK
        generate_report = \
            DummyOperator(
                task_id=f'generate_report_{samplesheet_id}')
        # TASK
        upload_report_to_box = \
            DummyOperator(
                task_id=f'upload_report_to_box{samplesheet_id}')
        # PIPELINE
        get_samplesheet_from_portal >> mark_seqrun_running
        mark_seqrun_running >> get_formatted_samplesheets
        get_formatted_samplesheets >> bcl_convert_run
        bcl_convert_run >> generate_report
        generate_report >> upload_report_to_box
        upload_report_to_box >> mark_seqrun_finished
