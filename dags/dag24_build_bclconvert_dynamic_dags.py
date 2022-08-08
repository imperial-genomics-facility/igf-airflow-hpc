import os
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import fetch_seqrun_data_from_portal_func
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import format_samplesheet_func

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
        default_view='tree',
        orientation='TB',
        tags=['hpc'])

with dag:
	## TASK
    fetch_seqrun_data_from_portal = \
        PythonOperator(
            task_id='fetch_seqrun_data_from_portal',
            dag=dag,
            queue='hpc_4G',
            params={
                'samplesheet_info_key': 'samplesheet_info'
            },
            python_callable=fetch_seqrun_data_from_portal_func,
        )
    ## TASK
    format_samplesheet = \
        PythonOperator(
            task_id='format_samplesheet',
            dag=dag,
            queue='hpc_4G',
            params={
                'samplesheet_info_key': 'samplesheet_info',
                'samplesheet_info_task': 'fetch_seqrun_data_from_portal',
                'samplesheet_file_key': 'samplesheet_file',
                'override_cycles_key': 'override_cycles',
                'tenx_sc_tag': '10X',
                'run_info_filname': 'RunInfo.xml',
                'formatted_samplesheets_key': 'formatted_samplesheets',
                'sample_groups_key': 'sample_groups'},
            python_callable=format_samplesheet_func)
    ## TASK
    generate_dynamic_dag = \
        DummyOperator(
            task_id='generate_dynamic_dag',
            dag=dag,
            queue='hpc_4G'
        )
    ## TASK
    copy_dag_to_hpc = \
        DummyOperator(
            task_id='copy_dag_to_hpc',
            dag=dag,
            queue='hpc_4G'
        )
    ## TASK
    copy_dag_to_igf_lims = \
        DummyOperator(
            task_id='copy_dag_to_igf_lims',
            dag=dag,
            queue='hpc_4G'
        )
    ## TASK
    copy_dag_to_wells = \
        DummyOperator(
            task_id='copy_dag_to_wells',
            dag=dag,
            queue='hpc_4G'
        )
    ## TASK
    register_pipeline = \
        DummyOperator(
            task_id='register_pipeline',
            dag=dag,
            queue='hpc_4G'
        )