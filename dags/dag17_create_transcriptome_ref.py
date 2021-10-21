import os
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import download_gtf_file_func
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import create_star_index_func

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
    download_gtf_file = \
        PythonOperator(
            task_id="download_gtf_file",
            dag=dag,
            queue='hpc_4G',
            params={
                'gtf_xcom_key': 'gtf_file'
            },
            python_callable=download_gtf_file_func)
    ## TASK
    create_star_index = \
        PythonOperator(
            task_id="create_star_index",
            dag=dag,
            queue='hpc_32G8t',
            params={
                'gtf_xcom_task': 'download_gtf_file',
                'gtf_xcom_key': 'gtf_file',
                'star_ref_xcom_key': 'star_ref',
                'threads': 8,
                'star_options': [
                    '--sjdbOverhang', 149]
            },
            python_callable=create_star_index_func)