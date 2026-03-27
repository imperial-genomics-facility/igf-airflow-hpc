import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from igf_airflow.utils.dag20_portal_metadata_utils import (
    copy_remote_file_to_hpc_func,
    create_raw_metadata_for_new_projects_func,
    get_formatted_metadata_files_func,
    upload_raw_metadata_to_portal_func
)
from igf_airflow.utils.dag54_metadata_rehydrate_utils import (
    get_known_projects_func,
    get_current_metadata_files_func
)

HPC_RDS = '/rds/general/project/genomics-facility-archive-2019/live'
QUOTA_XLSX_FILE_PATH = f'{HPC_RDS}/orwell_access_lims/docs/igf/IGF operation/ADMIN/DB tables/Quotes.xlsx'
ACCESS_DB_PATH = f'{HPC_RDS}/orwell_access_lims/docs/igf/IGF operation/ADMIN/DB tables/Database2_be.accdb'


## DAG
DAG_ID = (
    os.path.basename(__file__)
    .replace(".pyc", "")
    .replace(".py", "")
)

@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=1,
    default_view='grid',
    orientation='TB',
    tags=["metadata", "hpc"]
)
def dag54_metadata_rehydrate():
    ## TASK
    copy_quota_xlsx = PythonOperator(
        task_id="copy_quota_xlsx",
        retry_delay=timedelta(minutes=5),
        retries=4,
        queue='hpc_4G',
        params={
            'xcom_key': 'quota_xlsx',
            'hpc_ssh_key_file': None,
            'source_address': None,
            'source_user': None,
            'source_path': QUOTA_XLSX_FILE_PATH
        },
        python_callable=copy_remote_file_to_hpc_func
    )
    ## TASK
    copy_access_db = PythonOperator(
        task_id="copy_access_db",
        retry_delay=timedelta(minutes=5),
        retries=4,
        queue='hpc_4G',
        params={
            'xcom_key': 'access_db',
            'hpc_ssh_key_file': None,
            'source_address': None,
            'source_user': None,
            'source_path': ACCESS_DB_PATH
        },
        python_callable=copy_remote_file_to_hpc_func
    )
    ## TASK
    get_known_projects = get_known_projects_func()
    ## TASK
    create_raw_metadata_for_new_projects = PythonOperator(
        task_id="create_raw_metadata_for_new_projects",
        retry_delay=timedelta(minutes=5),
        retries=4,
        queue='hpc_8G8t',
        params={
            'xcom_key': 'metadata_dir',
            'quota_xcom_task': 'copy_quota_xlsx',
            'quota_xcom_key': 'quota_xlsx',
            'access_db_xcom_task': 'copy_access_db',
            'access_db_xcom_key': 'access_db',
            'known_projects_xcom_task': 'get_known_projects',
            'known_projects_xcom_key': 'known_projects',
            'spark_threads': 8,
            'spark_py_file': '/home/vmuser/LimsMetadataParsing/dist/igfLimsParsing-0.0.1-py3.7.egg',
            'spark_script_path': '/home/vmuser/LimsMetadataParsing/scripts/parseAccessDbForMetadata.py',
            'ucanaccess_path': '/home/vmuser/UCanAccess-4.0.4-bin'
        },
        python_callable=create_raw_metadata_for_new_projects_func
    )
    ## TASK
    get_current_metadata_files = get_current_metadata_files_func(
        metadata_dir=create_raw_metadata_for_new_projects.output["metadata_dir"]
    )
    ## TASK
    get_formatted_metadata_files = PythonOperator(
        task_id="get_formatted_metadata_files",
        retry_delay=timedelta(minutes=5),
        retries=4,
        queue='hpc_4G',
        params={
            'xcom_key': 'formatted_metadata',
            'raw_metadata_xcom_key': 'metadata_dir',
            'raw_metadata_xcom_task': 'get_current_metadata_files'
        },
        python_callable=get_formatted_metadata_files_func
    )
    ## TASK
    upload_raw_metadata_to_portal = PythonOperator(
        task_id="upload_raw_metadata_to_portal",
        retry_delay=timedelta(minutes=5),
        retries=4,
        queue='hpc_4G',
        pool='igf_portal_pool',
        params={
            'formatted_metadata_xcom_key': 'formatted_metadata',
            'formatted_metadata_xcom_task': 'get_formatted_metadata_files'
        },
        python_callable=upload_raw_metadata_to_portal_func
    )
    ## PIPELINE
    copy_quota_xlsx >> create_raw_metadata_for_new_projects
    copy_access_db >> create_raw_metadata_for_new_projects
    get_known_projects >> create_raw_metadata_for_new_projects
    create_raw_metadata_for_new_projects >> get_current_metadata_files
    get_current_metadata_files >> get_formatted_metadata_files
    get_formatted_metadata_files >> upload_raw_metadata_to_portal


dag54_metadata_rehydrate()