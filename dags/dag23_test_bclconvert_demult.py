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
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import calculate_override_bases_mask_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import bcl_convert_run_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import generate_report_func
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import upload_report_to_box_func


## DEFAULTS
MAX_SAMPLESHEETS = 30

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
        PythonOperator(
            task_id='get_samplesheet_from_portal',
            dag=dag,
            queue='hpc_4G',
            params={
                'samplesheet_xcom_key': 'samplesheet_data',
            },
            python_callable=get_samplesheet_from_portal_func)
    ## TASK
    mark_seqrun_running = \
        PythonOperator(
            task_id='mark_seqrun_running',
            dag=dag,
            queue='hpc_4G',
            params={
                'next_task': 'get_formatted_samplesheets',
                'last_task': 'no_work',
                'seed_table': 'seqrun',
                'seed_status': 'RUNNING',
                'no_change_status': 'RUNNING'},
            python_callable=mark_seqrun_status_func)
    ## TASK
    no_work = \
        DummyOperator(
            task_id='no_work',
            dag=dag,
            queue='hpc_4G')
    ## TASK
    generate_merged_report = \
        DummyOperator(
            task_id='generate_merged_report',
            trigger_rule='none_failed',)
    ## TASK
    upload_merged_report_to_portal = \
        DummyOperator(
            task_id='upload_merged_report_to_portal')
    ## TASK
    mark_seqrun_finished = \
        DummyOperator(
            task_id='mark_seqrun_finished')
    ## TASK
    get_formatted_samplesheets = \
        BranchPythonOperator(
            task_id='get_formatted_samplesheets',
            dag=dag,
            queue='hpc_4G',
            params={
                'samplesheet_xcom_key': 'samplesheet_data',
                'samplesheet_xcom_task': 'get_samplesheet_from_portal',
                'formatted_samplesheet_xcom_key': 'formatted_samplesheet_data',
                'samplesheet_tag': 'samplesheet_tag',
                'samplesheet_file': 'samplesheet_file',
                'next_task_prefix': 'bcl_convert_run_'
            },
            python_callable=get_formatted_samplesheets_func)
    ## PIPELINE
    get_samplesheet_from_portal >> mark_seqrun_running
    mark_seqrun_running >> get_formatted_samplesheets

    ## SAMPLE GROUP LOOP
    for samplesheet_id in range(1, MAX_SAMPLESHEETS):
        ## TASK
        calculate_override_bases_mask = \
            PythonOperator(
                task_id=f'calculate_override_bases_mask_{samplesheet_id}',
                dag=dag,
                queue='hpc_4G',
                params={
                    'samplesheet_index': samplesheet_id,
                    'formatted_samplesheet_xcom_task': 'get_formatted_samplesheets',
                    'formatted_samplesheet_xcom_key': 'formatted_samplesheet_data',
                    'mod_samplesheet_xcom_key': 'mod_samplesheet'
                },
                python_callable=calculate_override_bases_mask_func)
        ## TASK
        bcl_convert_run = \
            PythonOperator(
                task_id=f'bcl_convert_run_{samplesheet_id}',
                dag=dag,
                queue='hpc_4G',
                params={
                    'mod_samplesheet_xcom_key': 'mod_samplesheet',
                    'mod_samplesheet_xcom_task': f'calculate_override_bases_mask_{samplesheet_id}',
                    'demult_dir_key': 'demult_dir'
                },
                python_callable=bcl_convert_run_func)
        ## TASK
        generate_report = \
            PythonOperator(
                task_id=f'generate_report_{samplesheet_id}',
                dag=dag,
                queue='hpc_4G',
                params={
                    'demult_dir_key': 'demult_dir',
                    'demult_dir_task': f'bcl_convert_run_{samplesheet_id}',
                    'demult_report_key': 'demult_report'
                },
                python_callable=generate_report_func)
        ## TASK
        upload_report_to_box = \
            PythonOperator(
                task_id=f'upload_report_to_box{samplesheet_id}',
                dag=dag,
                queue='hpc_4G',
                pool='box_task_pool',
                params={
                    'index_column': 'index',
                    'lane_column': 'lane',
                    'tag_column': 'tag',
                    'samplesheet_index': samplesheet_id,
                    'demult_report_key': 'demult_report',
                    'demult_report_task': f'generate_report_{samplesheet_id}',
                    'formatted_samplesheet_xcom_task': 'get_formatted_samplesheets',
                    'formatted_samplesheet_xcom_key': 'formatted_samplesheet_data',
                },
                python_callable=upload_report_to_box_func)
        ## PIPELINE
        get_formatted_samplesheets >> calculate_override_bases_mask
        calculate_override_bases_mask >> bcl_convert_run
        bcl_convert_run >> generate_report
        generate_report >> upload_report_to_box
        upload_report_to_box >> generate_merged_report
    ## PIPELINE
    generate_merged_report >> upload_merged_report_to_portal
    upload_merged_report_to_portal >> mark_seqrun_finished
    mark_seqrun_running >> no_work
