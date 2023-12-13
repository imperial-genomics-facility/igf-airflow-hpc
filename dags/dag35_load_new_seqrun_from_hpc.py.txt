import os
import pendulum
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.empty import EmptyOperator
from igf_airflow.utils.dag35_load_new_seqrun_from_hpc import (
    register_run_to_db_and_portal_func,
    generate_interop_report_and_upload_to_portal_func)

HPC_SEQRUN_PATH = Variable.get('hpc_seqrun_path', default_var=None)
SERVER_IN_USE = 'wells'

## quick check before running dag
if SERVER_IN_USE not in ['wells', 'orwell']:
    raise ValueError(f"{SERVER_IN_USE} is not valid")

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")

dag = \
    DAG(
        dag_id=DAG_ID,
        schedule=None, ## Never run
        catchup=False,
        start_date=pendulum.yesterday(),
        dagrun_timeout=timedelta(minutes=60),
        default_view='grid',
        orientation='TB',
        max_active_runs=1,
        tags=['hpc',])
with dag:
    ## TASK
    register_run_to_db_and_portal = \
        PythonOperator(
            task_id='register_run_to_db_and_portal',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            trigger_rule='none_failed_min_one_success',
            params={
                'server_in_use': SERVER_IN_USE,
                'wells_xcom_task': 'get_new_seqrun_id_from_wells',
                'orwell_xcom_task': 'get_new_seqrun_id_from_orwell',
                'xcom_key': 'seqrun_id'
            },
            python_callable=register_run_to_db_and_portal_func)
    ## TASK
    generate_interop_report_and_upload_to_portal = \
        PythonOperator(
            task_id='generate_interop_report_and_upload_to_portal',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'server_in_use': SERVER_IN_USE,
                'wells_xcom_task': 'get_new_seqrun_id_from_wells',
                'orwell_xcom_task': 'get_new_seqrun_id_from_orwell',
                'xcom_key': 'seqrun_id'
            },
            python_callable=generate_interop_report_and_upload_to_portal_func)