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
from igf_airflow.utils.dag25_copy_seqruns_to_hpc_utils import get_new_run_id_for_copy
from igf_airflow.utils.dag25_copy_seqruns_to_hpc_utils import register_run_to_db_and_portal_func

SEQRUN_SERVER_USER = Variable.get('seqrun_server_user', default_var=None)
ORWELL_SERVER_HOSTNAME = Variable.get('orwell_server_hostname',  default_var="")
ORWELL_BASE_PATH = Variable.get('seqrun_base_path', default_var=None)
WELLS_SERVER_HOSTNAME = Variable.get('wells_server_hostname', default_var=None)
WELLS_SEQRUN_BASE_PATH = Variable.get('wells_seqrun_base_path', default_var=None)
HPC_SEQRUN_PATH = Variable.get('hpc_seqrun_path', default_var=None)
HPC_SSH_KEY_FILE = Variable.get('hpc_ssh_key_file', default_var=None)
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
    'start_date': days_ago(1),
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
        schedule_interval='0 */4 * * *', ## every 4hrs
        default_args=args,
        default_view='tree',
        orientation='TB',
        tags=['hpc',])

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
                bash /home/igf/airflow_v2/seqrun_copy_scripts/check_new_runs.sh
            """)
    ## TASK
    get_new_seqrun_id_from_wells = \
        BranchPythonOperator(
            task_id='get_new_seqrun_id_from_wells',
            dag=dag,
            queue='hpc_4G',
            params={
                'xcom_task': 'get_all_runs_from_wells',
                'next_task': 'copy_run_to_wells',
                'no_work_task': 'no_work',
                'seqrun_id_xcom_key': 'seqrun_id'
            },
            python_callable=get_new_run_id_for_copy)
    ## TASK
    copy_run_to_wells = \
        SSHOperator(
            task_id='copy_run_to_wells',
            dag=dag,
            queue='hpc_4G',
            pool='wells_ssh_pool',
            ssh_hook=wells_ssh_hook,
             params={
                'xcom_task': 'get_new_seqrun_id_from_wells',
                'xcom_key': 'seqrun_id'
            },
            command="""
                bash /home/igf/airflow_v2/seqrun_copy_scripts/check_and_copy_new_seqrun.sh {{ ti.xcom_pull(task_ids=params.xcom_task, key=params.xcom_key) }}
            """)
    ## TASK
    copy_run_from_wells_to_hpc = \
       BashOperator(
            task_id='copy_run_from_wells_to_hpc',
            dag=dag,
            queue='hpc_4G',
            params={
                'server_hostname': WELLS_SERVER_HOSTNAME,
                'seqrun_path': WELLS_SEQRUN_BASE_PATH,
                'hpc_seqrun_path': HPC_SEQRUN_PATH,
                'remote_user': SEQRUN_SERVER_USER,
                'hpc_ssh_key_file': HPC_SSH_KEY_FILE,
                'xcom_task': 'get_new_seqrun_id_from_wells',
                'xcom_key': 'seqrun_id'
            },
            bash_command="""
                if [-d {{ params.hpc_seqrun_path }}/{{ ti.xcom_pull(task_ids=params.xcom_task, key=params.xcom_key) }} ];
                then
                  echo "{{ ti.xcom_pull(task_ids=params.xcom_task) }} already present on hpc"; exit 1;
                else
                  scp -i {{ params.hpc_ssh_key_file }} \
                      -r {{ params.remote_user }}@{{ params.server_hostname }}:{{ params.seqrun_path }}/{{ ti.xcom_pull(task_ids=params.xcom_task, key=params.xcom_key) }} \
                      {{ params.hpc_seqrun_path }}/
                fi
            """)
    # ## TASK
    # get_all_runs_from_orwell = \
    #     SSHOperator(
    #         task_id='get_all_runs_from_orwell',
    #         dag=dag,
    #         queue='hpc_4G',
    #         pool='orwell_ssh_pool',
    #         ssh_hook=orwell_ssh_hook,
    #         command="""
    #             bash check_new_runs.sh
    #         """)
    # ## TASK
    # get_new_seqrun_id_from_orwell = \
    #     BranchPythonOperator(
    #         task_id='get_new_seqrun_id_from_orwell',
    #         dag=dag,
    #         queue='hpc_4G',
    #         params={
    #             'xcom_task': 'get_all_runs_from_orwell',
    #             'next_task': 'copy_run_to_orwell',
    #             'no_work_task': 'no_work',
    #             'seqrun_id_xcom_key': 'seqrun_id'
    #         },
    #         python_callable=get_new_run_id_for_copy)
    # ## TASK
    # copy_run_to_orwell = \
    #     SSHOperator(
    #         task_id='copy_run_to_orwell',
    #         dag=dag,
    #         queue='hpc_4G',
    #         pool='wells_ssh_pool',
    #         ssh_hook=wells_ssh_hook,
    #         params={
    #             'xcom_task': 'get_new_seqrun_id_from_orwell',
    #             'xcom_key': 'seqrun_id'
    #         },
    #         command="""
    #             bash copy_new_run_to_orwell.sh {{ti.xcom_pull(task_ids=params.xcom_task, key=params.xcom_key)}}
    #         """)
    # ## TASK
    # copy_run_from_orwell_to_hpc = \
    #     BashOperator(
    #         task_id='copy_run_from_orwell_to_hpc',
    #         dag=dag,
    #         queue='hpc_4G',
    #         params={
    #             'server_hostname': ORWELL_SERVER_HOSTNAME,
    #             'seqrun_path': ORWELL_BASE_PATH,
    #             'hpc_seqrun_path': HPC_SEQRUN_PATH,
    #             'remote_user': SEQRUN_SERVER_USER,
    #             'hpc_ssh_key_file': HPC_SSH_KEY_FILE,
    #             'xcom_task': 'get_new_seqrun_id_from_wells'
    #         },
    #         bash_command="""
    #             if [-d {{ params.hpc_seqrun_path }}/{{ ti.xcom_pull(task_ids=params.xcom_task) }} ];
    #             then
    #               echo "{{ ti.xcom_pull(task_ids=params.xcom_task) }} already present on hpc"; exit 1;
    #             else
    #               scp -i {{ params.hpc_ssh_key_file }} \
    #                   -r {{ params.remote_user }}@{{ params.server_hostname }}:{{ params.seqrun_path }}/{{ ti.xcom_pull(task_ids=params.xcom_task) }} \
    #                   {{ params.hpc_seqrun_path }}/
    #             fi
    #         """)
    ## TASK
    register_run_to_db_and_portal = \
        PythonOperator(
            task_id='register_run_to_db_and_portal',
            dag=dag,
            queue='hpc_4G',
            trigger_rule='none_failed_min_one_success',
            params={
                'server_in_use': SERVER_IN_USE,
                'wells_xcom_task': 'get_all_runs_from_wells',
                'orwell_xcom_task': 'get_new_seqrun_id_from_orwell',
                'xcom_key': 'seqrun_id'
            },
            python_callable=register_run_to_db_and_portal_func)
    ## TASK
    no_work = \
        DummyOperator(
            task_id='no_work',
            dag=dag,
            queue='hpc_4G')
    ## PIPELINE
    decide_server >> get_all_runs_from_wells
    get_all_runs_from_wells >> get_new_seqrun_id_from_wells
    get_new_seqrun_id_from_wells >> no_work
    get_new_seqrun_id_from_wells >> copy_run_to_wells
    copy_run_to_wells >> copy_run_from_wells_to_hpc
    copy_run_from_wells_to_hpc >> register_run_to_db_and_portal
    # decide_server >> get_all_runs_from_orwell
    # get_all_runs_from_orwell >> get_new_seqrun_id_from_orwell
    # get_new_seqrun_id_from_orwell >> no_work
    # get_new_seqrun_id_from_orwell>> copy_run_to_orwell
    # copy_run_to_orwell >> copy_run_from_orwell_to_hpc
    # copy_run_from_orwell_to_hpc >> register_run_to_db_and_portal