import os
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from igf_airflow.utils.dag20_portal_tasks_utils import get_metadata_dump_from_pipeline_db_func

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
        tags=['hpc'])
with dag:
    ## TASK
    get_metadata_dump_from_pipeline_db = \
        PythonOperator(
            task_id="get_metadata_dump_from_pipeline_db",
            dag=dag,
            queue='hpc_4G',
            params={
                'json_dump_xcom_key': 'json_dump'
            },
            python_callable=get_metadata_dump_from_pipeline_db_func)