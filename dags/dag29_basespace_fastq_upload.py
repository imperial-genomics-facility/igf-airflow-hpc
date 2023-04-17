import os
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag29_basespace_fastq_upload_utils import upload_project_fastq_to_basespace_func

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
        schedule_interval=None,
        default_args=args,
        default_view='tree',
        orientation='TB',
        max_active_runs=1,
        catchup=False,
        tags=['hpc', 'basespace', 'upload'])

with dag:
	## TASK
    upload_project_fastq_to_basespace = \
        PythonOperator(
            task_id="upload_project_fastq_to_basespace",
            dag=dag,
            queue='hpc_4G',
            params={},
            python_callable=upload_project_fastq_to_basespace_func
        )