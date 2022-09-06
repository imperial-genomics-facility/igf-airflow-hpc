import os
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


## ARGS
args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
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
        tags=['hpc', 'snakemake', 'rnaseq'])

with dag:
	## TASK
    mark_analysis_seed_as_running = \
        DummyOperator(
            task_id="mark_analysis_seed_as_running",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    no_task = \
        DummyOperator(
            task_id="no_task",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    mark_analysis_seed_as_finished = \
        DummyOperator(
            task_id="mark_analysis_seed_as_finished",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    prepare_snakemake_inputs = \
        DummyOperator(
            task_id="prepare_snakemake_inputs",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    run_snakemake_pipeline = \
        DummyOperator(
            task_id="run_snakemake_pipeline",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    create_snakemake_report = \
        DummyOperator(
            task_id="create_snakemake_report",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    load_analysis_to_disk = \
        DummyOperator(
            task_id="load_analysis_to_disk",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    copy_analysis_to_globus_dir = \
        DummyOperator(
            task_id="copy_analysis_to_globus_dir",
            dag=dag,
            queue='hpc_4G',
        )
    ## PIPELINE
    mark_analysis_seed_as_running >> no_task
    mark_analysis_seed_as_running >> prepare_snakemake_inputs
    prepare_snakemake_inputs >> run_snakemake_pipeline
    run_snakemake_pipeline >> create_snakemake_report
    create_snakemake_report >> load_analysis_to_disk
    load_analysis_to_disk >> copy_analysis_to_globus_dir
    copy_analysis_to_globus_dir >> mark_analysis_seed_as_finished
