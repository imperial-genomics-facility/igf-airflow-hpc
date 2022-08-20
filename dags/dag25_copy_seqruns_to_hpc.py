import os
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

ORWELL_SERVER_HOSTNAME = Variable.get('orwell_server_hostname',  default_var="")
WELLS_SERVER_HOSTNAME = Variable.get('wells_server_hostname', default_var=None)
SERVER_IN_USE = 'wells'

## quick check before running dag
if SERVER_IN_USE not in ['wells', 'orwell']:
    raise ValueError(f"{SERVER_IN_USE} is not valid")

## SSH HOOK
orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file',  default_var=""),
    username=Variable.get('hpc_user',  default_var=""),
    remote_host=ORWELL_SERVER_HOSTNAME)

wells_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file',  default_var=""),
    username=Variable.get('hpc_user',  default_var=""),
    remote_host=WELLS_SERVER_HOSTNAME)

## ARGS
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

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule_interval=None, ## TO DO: run after 4 hrs
        default_args=args,
        default_view='tree',
        orientation='TB',
        tags=['hpc'])

with dag:
	## TASK
    decide_server = \
        BranchPythonOperator(
            task_id='decide_server',
            dag=dag,
            queue='hpc_4G',
            python_callable=lambda : 'get_all_runs_from_wells' if SERVER_IN_USE=='wells' else 'get_all_runs_from_orwell')
    ## TASK
    get_all_runs_from_wells = \
        SSHOperator(
            task_id='get_all_runs_from_wells',
            dag=dag,
            queue='hpc_4G',
            pool='wells_ssh_pool',
            ssh_hook=wells_ssh_hook,
            command="""
                bash get_all_runs_from_wells.sh
            """)
    ## TASK
    get_new_seqrun_id_from_wells = \
        PythonOperator(
            task_id='get_new_seqrun_id_from_wells',
            dag=dag,
            queue='hpc_4G',
            params={},
            python_callable=None
        )
    ## TASK
    copy_run_to_wells = \
        SSHOperator(
            task_id='copy_run_to_wells',
            dag=dag,
            queue='hpc_4G',
            pool='wells_ssh_pool',
            ssh_hook=wells_ssh_hook,
            command="""
                bash copy_new_run_to_wells.sh
            """)
    ## TASK
    copy_run_from_wells_to_hpc = \
       BashOperator(
            task_id='copy_run_from_wells_to_hpc',
            dag=dag,
            queue='hpc_4G',
            bash_command="""
                scp -r wells_path hpc_path
            """)
    ## TASK
    get_all_runs_from_orwell = \
        SSHOperator(
            task_id='get_all_runs_from_orwell',
            dag=dag,
            queue='hpc_4G',
            pool='orwell_ssh_pool',
            ssh_hook=orwell_ssh_hook,
            command="""
                bash get_all_runs_from_orwell.sh
            """)
    ## TASK
    get_new_seqrun_id_from_orwell = \
        PythonOperator(
            task_id='get_new_seqrun_id_from_orwell',
            dag=dag,
            queue='hpc_4G',
            params={},
            python_callable=None
        )
    ## TASK
    copy_run_to_orwell = \
        SSHOperator(
            task_id='copy_run_to_wells',
            dag=dag,
            queue='hpc_4G',
            pool='wells_ssh_pool',
            ssh_hook=wells_ssh_hook,
            command="""
                bash copy_new_run_to_wells.sh
            """)
    ## TASK
    copy_run_from_orwell_to_hpc = \
        BashOperator(
            task_id='copy_run_from_wells_to_hpc',
            dag=dag,
            queue='hpc_4G',
            bash_command="""
                scp -r wells_path hpc_path
            """)
    ## TASK
    register_run_to_db_and_portal = \
        PythonOperator(
            task_id='register_run_to_db_and_portal',
            dag=dag,
            queue='hpc_4G',
            trigger_rule='none_failed_min_one_success',
            params={},
            python_callable=None
        )
    ## PIPELINE
    decide_server >> get_all_runs_from_wells
    get_all_runs_from_wells >> get_new_seqrun_id_from_wells
    get_new_seqrun_id_from_wells >> copy_run_to_wells
    copy_run_to_wells >> copy_run_from_wells_to_hpc
    copy_run_from_wells_to_hpc >> register_run_to_db_and_portal
    decide_server >> get_all_runs_from_orwell
    get_all_runs_from_orwell >> get_new_seqrun_id_from_orwell
    get_new_seqrun_id_from_orwell>> copy_run_to_orwell
    copy_run_to_orwell >> copy_run_from_orwell_to_hpc
    copy_run_from_orwell_to_hpc >> register_run_to_db_and_portal