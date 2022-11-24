import os
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import change_analysis_seed_status_func
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import prepare_snakemake_inputs_func
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import load_analysis_to_disk_func
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import copy_analysis_to_globus_dir_func



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
        max_active_runs=10,
        tags=['hpc', 'snakemake', 'rnaseq'])

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
                'next_task': 'prepare_snakemake_inputs',
                'last_task': 'no_task',
                'seed_table': 'analysis'
            },
            python_callable=change_analysis_seed_status_func
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
    prepare_snakemake_inputs = \
        PythonOperator(
            task_id="prepare_snakemake_inputs",
            dag=dag,
            queue='hpc_4G',
            params={
                "snakemake_command_key": "snakemake_command",
                "snakemake_report_key": "snakemake_report",
                "snakemake_workdir_key": "snakemake_workdir"
            },
            python_callable=prepare_snakemake_inputs_func
        )
    ## TASK
    run_snakemake_pipeline = \
        BashOperator(
            task_id="run_snakemake_pipeline",
            dag=dag,
            queue='hpc_4G_long',
            pool='batch_job',
            do_xcom_push=False,
            params={
                "task_key": "snakemake_command",
                "task_id": "prepare_snakemake_inputs"
            },
            bash_command="""
              bash {{ ti.xcom_pull(key=params.task_key, task_ids=params.task_id ) }}
            """
        )
    ## TASK
    create_snakemake_report = \
        BashOperator(
            task_id="create_snakemake_report",
            dag=dag,
            queue='hpc_4G',
            do_xcom_push=False,
             params={
                "task_key": "snakemake_report",
                "task_id": "prepare_snakemake_inputs"
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
                "task_key": "snakemake_workdir_key",
                "task_id": "prepare_snakemake_inputs"
                "result_dir_name": "results"
            },
            bash_command="""
              cd {{ ti.xcom_pull(key=params.task_key, task_ids=params.task_id ) }}
              find {{ result_dir_name }} -type f -exec md5sum {} \; > file_manifest.md5
              mv file_manifest.md5 {{ result_dir_name }}/file_manifest.md5
            """
        )
    ## TASK
    load_analysis_to_disk = \
        PythonOperator(
            task_id="load_analysis_to_disk",
            dag=dag,
            queue='hpc_4G',
            params={
                "analysis_dir_key": "snakemake_workdir",
                "analysis_dir_task": "prepare_snakemake_inputs",
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
    mark_analysis_seed_as_running >> prepare_snakemake_inputs
    prepare_snakemake_inputs >> run_snakemake_pipeline
    run_snakemake_pipeline >> create_snakemake_report
    create_snakemake_report >> create_md5sum_for_analysis
    create_md5sum_for_analysis >> load_analysis_to_disk
    load_analysis_to_disk >> copy_analysis_to_globus_dir
    copy_analysis_to_globus_dir >> mark_analysis_seed_as_finished
