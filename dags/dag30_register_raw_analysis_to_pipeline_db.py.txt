import os
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag30_register_raw_analysis_to_pipeline_db_utils import (
    fetch_raw_analysis_queue_func,
    process_raw_analysis_queue_func)

## ARGS
args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 10,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False}

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")

dag = \
    DAG(
        dag_id=DAG_ID,
        schedule_interval='0 */1 * * *', ## every 1hrs,
        default_args=args,
        default_view='tree',
        orientation='TB',
        max_active_runs=1,
        catchup=False,
        tags=['hpc', 'analysis', 'portal'])

with dag:
	## TASK
    fetch_raw_analysis_queue = \
        PythonOperator(
            task_id="fetch_raw_analysis_queue",
            dag=dag,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                'new_raw_analysis_list_key': 'new_raw_analysis_list'
            },
            python_callable=fetch_raw_analysis_queue_func
        )
    ## TASK
    process_raw_analysis_queue = \
        PythonOperator(
            task_id="process_raw_analysis_queue",
            dag=dag,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                "new_raw_analysis_list_key": "new_raw_analysis_list",
                "new_raw_analysis_list_task": "fetch_raw_analysis_queue"
            },
            python_callable=process_raw_analysis_queue_func
        )
    ## PIPELINE
    fetch_raw_analysis_queue >> process_raw_analysis_queue