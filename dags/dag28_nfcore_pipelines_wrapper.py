import os
import pendulum
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import change_analysis_seed_status_func
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import load_analysis_to_disk_func
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import copy_analysis_to_globus_dir_func
from igf_airflow.utils.dag28_nfcore_pipelines_wrapper_utils import prepare_nfcore_pipeline_inputs

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
        schedule_interval=None,
        default_args=args,
        default_view='tree',
        orientation='TB',
        max_active_runs=10,
        tags=['hpc', 'nextflow', 'nfcore'])

with dag:
	## TASK
    mark_analysis_seed_as_running = \
        BranchPythonOperator(
            task_id="mark_analysis_seed_as_running",
            dag=dag,
            queue='hpc_4G',
            params={
                'new_status': 'RUNNING',
                'no_change_status': ['RUNNING', 'FAILED', 'FINISHED', 'UNKNOWN'],
                'next_task': 'prepare_nfcore_pipeline_inputs',
                'last_task': 'no_task',
                'seed_table': 'analysis'
            },
            python_callable=change_analysis_seed_status_func
        )
    ## TASK
    no_task = \
        EmptyOperator(
            task_id="no_task",
            dag=dag,
            queue='hpc_4G',
        )
    ## TASK
    mark_analysis_seed_as_finished = \
        PythonOperator(
            task_id="mark_analysis_seed_as_finished",
            dag=dag,
            queue='hpc_4G',
            params={
                'new_status': 'FINISHED',
                'no_change_status': ['SEEDED', 'FAILED'],
                'seed_table': 'analysis'
            },
            python_callable=change_analysis_seed_status_func
        )
    ## TASK
    mark_analysis_seed_as_failed = \
        PythonOperator(
            task_id="mark_analysis_seed_as_failed",
            dag=dag,
            queue='hpc_4G',
            params={
                'new_status': 'FAILED',
                'no_change_status': ['SEEDED', 'FINISHED'],
                'seed_table': 'analysis'
            },
            python_callable=change_analysis_seed_status_func
        )
    ## TASK
    prepare_nfcore_pipeline_inputs = \
        BranchPythonOperator(
            task_id="prepare_nfcore_pipeline_inputs",
            dag=dag,
            queue='hpc_4G',
            params={
                "nextflow_command_key": "nextflow_command",
                "nextflow_workdir_key": "nextflow_workdir",
                "next_task": "run_nfcore_pipeline",
                "last_task": "mark_analysis_seed_as_failed"
            },
            python_callable=prepare_nfcore_pipeline_inputs
        )
    ## TASK
    run_nfcore_pipeline = \
        BashOperator(
            task_id="run_nfcore_pipeline",
            dag=dag,
            queue='hpc_4G_long',
            pool='batch_job',
            do_xcom_push=False,
            params={
                "task_key": "nextflow_command",
                "task_id": "prepare_nfcore_pipeline_inputs"
            },
            bash_command="""
              bash {{ ti.xcom_pull(key=params.task_key, task_ids=params.task_id ) }}
            """
        )
    ## TASK
    create_md5sum_for_analysis = \
        BashOperator(
            task_id="create_md5sum_for_analysis",
            dag=dag,
            queue='hpc_4G',
            do_xcom_push=False,
            params={
                "task_key": "nextflow_workdir",
                "task_id": "prepare_nfcore_pipeline_inputs",
                "result_dir_name": "results",
            },
            bash_command="""
              cd {{ ti.xcom_pull(key=params.task_key, task_ids=params.task_id ) }}
              find {{ params.result_dir_name }} -type f -exec md5sum {} \; > file_manifest.md5
              mv file_manifest.md5 {{ params.result_dir_name }}/file_manifest.md5
            """
        )
    ## TASK
    load_analysis_to_disk = \
        PythonOperator(
            task_id="load_analysis_to_disk",
            dag=dag,
            queue='hpc_4G',
            params={
                "analysis_dir_key": "nextflow_workdir",
                "analysis_dir_task": "prepare_nfcore_pipeline_inputs",
                "result_dir_name": "results",
                "analysis_collection_dir_key": "analysis_collection_dir",
                "date_tag_key": "date_tag"
            },
            python_callable=load_analysis_to_disk_func
        )
    ## TASK
    copy_analysis_to_globus_dir = \
        PythonOperator(
            task_id="copy_analysis_to_globus_dir",
            dag=dag,
            queue='hpc_4G',
            params={
                "date_tag_key": "date_tag",
                "date_tag_task": "load_analysis_to_disk",
                "analysis_collection_dir_key": "analysis_collection_dir",
                "analysis_collection_dir_task": "load_analysis_to_disk"
            },
            python_callable=copy_analysis_to_globus_dir_func
        )
    ## PIPELINE
    mark_analysis_seed_as_running >> no_task
    mark_analysis_seed_as_running >> prepare_nfcore_pipeline_inputs
    prepare_nfcore_pipeline_inputs >> mark_analysis_seed_as_failed
    prepare_nfcore_pipeline_inputs >> run_nfcore_pipeline
    run_nfcore_pipeline >> create_md5sum_for_analysis
    create_md5sum_for_analysis >> load_analysis_to_disk
    load_analysis_to_disk >> copy_analysis_to_globus_dir
    copy_analysis_to_globus_dir >> mark_analysis_seed_as_finished