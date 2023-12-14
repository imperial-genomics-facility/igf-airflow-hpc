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
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
    change_analysis_seed_status_func,
    prepare_snakemake_inputs_func,
    load_analysis_to_disk_func,
    copy_analysis_to_globus_dir_func,
    send_email_to_user_func)



## ARGS
# args = {
#     'owner': 'airflow',
#     'start_date': pendulum.today('UTC').add(days=2),
#     'retries': 10,
#     'retry_delay': timedelta(minutes=5),
#     'provide_context': True,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'catchup': False}

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule=None,
        start_date=pendulum.yesterday(),
        default_view='grid',
        orientation='TB',
        max_active_runs=10,
        tags=['hpc', 'snakemake', 'rnaseq'])

with dag:
	## TASK
    mark_analysis_seed_as_running = \
        BranchPythonOperator(
            task_id="mark_analysis_seed_as_running",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
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
            retry_delay=timedelta(minutes=5),
            retries=4,
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
            retry_delay=timedelta(minutes=5),
            retries=4,
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
            retry_delay=timedelta(minutes=15),
            retries=10,
            queue='hpc_8G4t72hr',
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
            retry_delay=timedelta(minutes=15),
            retries=4,
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
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            do_xcom_push=False,
            params={
                "task_key": "snakemake_workdir",
                "task_id": "prepare_snakemake_inputs",
                "result_dir_name": "results"
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
            retry_delay=timedelta(minutes=5),
            retries=4,
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
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            params={
                "date_tag_key": "date_tag",
                "date_tag_task": "load_analysis_to_disk",
                "analysis_collection_dir_key": "analysis_collection_dir",
                "analysis_collection_dir_task": "load_analysis_to_disk"
            },
            python_callable=copy_analysis_to_globus_dir_func
        )
    ## TASK
    send_email = \
        PythonOperator(
            task_id="send_email_to_user",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            params={
                "send_email_to_user": True
            },
            python_callable=send_email_to_user_func
        )
    ## PIPELINE
    mark_analysis_seed_as_running >> no_task
    mark_analysis_seed_as_running >> prepare_snakemake_inputs
    prepare_snakemake_inputs >> run_snakemake_pipeline
    run_snakemake_pipeline >> create_snakemake_report
    create_snakemake_report >> create_md5sum_for_analysis
    create_md5sum_for_analysis >> load_analysis_to_disk
    load_analysis_to_disk >> copy_analysis_to_globus_dir
    copy_analysis_to_globus_dir >> send_email >> mark_analysis_seed_as_finished
