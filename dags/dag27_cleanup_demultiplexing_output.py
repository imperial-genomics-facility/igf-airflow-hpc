import os
import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from igf_airflow.utils.dag27_cleanup_demultiplexing_output_utils import cleanup_fastq_for_seqrun_func

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule=None,
        catchup=False,
        start_date=pendulum.yesterday(),
        dagrun_timeout=timedelta(minutes=15),
        default_view='grid',
        orientation='TB',
        max_active_runs=1,
        tags=['hpc', 'de-multiplexing', 'cleanup'])

with dag:
	## TASK
    cleanup_fastq_for_seqrun = \
        PythonOperator(
            task_id="mark_analysis_seed_as_running",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
            },
            python_callable=cleanup_fastq_for_seqrun_func
        )
    ## PIPELINE
    cleanup_fastq_for_seqrun

