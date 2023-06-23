import os
import pendulum
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag27_cleanup_demultiplexing_output_utils import cleanup_fastq_for_seqrun_func

## ARGS
args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=2),
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
        schedule=None,
        default_args=args,
        default_view='tree',
        orientation='TB',
        max_active_runs=1,
        tags=['hpc', 'de-multiplexing', 'cleanup'])

with dag:
	## TASK
    cleanup_fastq_for_seqrun = \
        PythonOperator(
            task_id="mark_analysis_seed_as_running",
            dag=dag,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
            },
            python_callable=cleanup_fastq_for_seqrun_func
        )
    ## PIPELINE
    cleanup_fastq_for_seqrun

