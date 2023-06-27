import os
import pendulum
from datetime import timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import fetch_seqrun_data_from_portal_func
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import format_samplesheet_func
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import generate_dynamic_dag_func
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import copy_dag_to_hpc_func
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import copy_dag_to_remote_func
from igf_airflow.utils.dag24_build_bclconvert_dynamic_dags_utils import register_pipeline_func

WELLS_DYNAMIC_DAG_DIR = Variable.get('wells_dynamic_dag_dir', default_var=None)
WELLS_SERVER_HOSTNAME = Variable.get('wells_server_hostname', default_var=None)
IGF_LIMS_DYNAMIC_DAG_DIR  = Variable.get('igf_lims_dynamic_dag_dir', default_var=None)
IGF_LIMS_SERVER_HOSTNAME = Variable.get('igf_lims_server_hostname', default_var=None)


## ARGS
# args = {
#     'owner': 'airflow',
#     'start_date': pendulum.today('UTC').add(days=2),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'provide_context': True,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'catchup': False,
#     'max_active_runs': 10}

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
        tags=['hpc'])

with dag:
	## TASK
    fetch_seqrun_data_from_portal = \
        PythonOperator(
            task_id='fetch_seqrun_data_from_portal',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                'samplesheet_info_key': 'samplesheet_info'
            },
            python_callable=fetch_seqrun_data_from_portal_func)
    ## TASK
    format_samplesheet = \
        PythonOperator(
            task_id='format_samplesheet',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'samplesheet_info_key': 'samplesheet_info',
                'samplesheet_info_task': 'fetch_seqrun_data_from_portal',
                'samplesheet_file_key': 'samplesheet_file',
                'override_cycles_key': 'override_cycles',
                'tenx_sc_tag': '10X',
                'run_info_filname': 'RunInfo.xml',
                'formatted_samplesheets_key': 'formatted_samplesheets',
                'sample_groups_key': 'sample_groups'},
            python_callable=format_samplesheet_func)
    ## TASK
    generate_dynamic_dag = \
        PythonOperator(
            task_id='generate_dynamic_dag',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'formatted_samplesheets_key': 'formatted_samplesheets',
                'formatted_samplesheets_task': 'format_samplesheet',
                'sample_groups_key': 'sample_groups',
                'sample_groups_task': 'format_samplesheet',
                'samplesheet_info_key': 'samplesheet_info',
                'samplesheet_info_task': 'fetch_seqrun_data_from_portal',
                'samplesheet_id_key': 'samplesheet_id',
                'temp_dag_file_key': 'temp_dag_file',
                'dag_id_key': 'dag_id'
            },
            python_callable=generate_dynamic_dag_func)
    ## TASK
    copy_dag_to_hpc = \
        PythonOperator(
            task_id='copy_dag_to_hpc',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'temp_dag_file_key': 'temp_dag_file',
                'temp_dag_file_task': 'generate_dynamic_dag'
            },
            python_callable=copy_dag_to_hpc_func)
    ## TASK
    copy_dag_to_igf_lims = \
        PythonOperator(
            task_id='copy_dag_to_igf_lims',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'temp_dag_file_key': 'temp_dag_file',
                'temp_dag_file_task': 'generate_dynamic_dag',
                'remote_dynamic_dag_path': IGF_LIMS_DYNAMIC_DAG_DIR,
                'remote_server_hostname': IGF_LIMS_SERVER_HOSTNAME
            },
            python_callable=copy_dag_to_remote_func)
    ## TASK
    copy_dag_to_wells = \
        PythonOperator(
            task_id='copy_dag_to_wells',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'temp_dag_file_key': 'temp_dag_file',
                'temp_dag_file_task': 'generate_dynamic_dag',
                'remote_dynamic_dag_path': WELLS_DYNAMIC_DAG_DIR,
                'remote_server_hostname': WELLS_SERVER_HOSTNAME
            },
            python_callable=copy_dag_to_remote_func)
    ## TASK
    register_pipeline = \
        PythonOperator(
            task_id='register_pipeline',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'dag_id_key': 'dag_id',
                'dag_id_task': 'generate_dynamic_dag'
            },
            python_callable=register_pipeline_func)
    ## PIPELINE
    fetch_seqrun_data_from_portal >> format_samplesheet
    format_samplesheet >> generate_dynamic_dag
    generate_dynamic_dag >> copy_dag_to_hpc >> register_pipeline
    generate_dynamic_dag >> copy_dag_to_igf_lims >> register_pipeline
    generate_dynamic_dag >> copy_dag_to_wells >> register_pipeline