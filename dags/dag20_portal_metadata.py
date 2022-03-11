import os
from datetime import timedelta
import queue
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from igf_airflow.utils.dag20_portal_metadata_utils import get_metadata_dump_from_pipeline_db_func
from igf_airflow.utils.dag20_portal_metadata_utils import upload_metadata_to_portal_db_func
from igf_airflow.utils.dag20_portal_metadata_utils import copy_remote_file_to_hpc_func
from igf_airflow.utils.dag20_portal_metadata_utils import get_known_projects_func
from igf_airflow.utils.dag20_portal_metadata_utils import create_raw_metadata_for_new_projects_func
from igf_airflow.utils.dag20_portal_metadata_utils import get_formatted_metadata_files_func
from igf_airflow.utils.dag20_portal_metadata_utils import upload_raw_metadata_to_portal_func


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

DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")

## SSH HOOK
orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file',  default_var=""),
    username=Variable.get('hpc_user',  default_var=""),
    remote_host=Variable.get('orwell_server_hostname',  default_var=""))


dag = \
    DAG(
        dag_id=DAG_ID,
        schedule_interval="@hourly",
        default_args=args,
        tags=['hpc'])
with dag:
    ## TASK
    copy_quota_xlsx = \
        PythonOperator(
            task_id="copy_quota_xlsx",
            dag=dag,
            queue='hpc_4G',
            pool="orwell_scp_pool",
            params={
                'xcom_key': 'quota_xlsx',
                'source_address': Variable.get("seqrun_server", default_var=""),
                'source_user': Variable.get("seqrun_server_user", default_var=""),
                'source_path': '/home/igf/docs/igf/IGF operation/ADMIN/DB tables/Quotes.xlsx'},
            python_callable=copy_remote_file_to_hpc_func)
    ## TASK
    copy_access_db = \
        PythonOperator(
            task_id="copy_access_db",
            dag=dag,
            queue='hpc_4G',
            pool="orwell_scp_pool",
            params={
                'xcom_key': 'access_db',
                'source_address': Variable.get("seqrun_server", default_var=""),
                'source_user': Variable.get("seqrun_server_user", default_var=""),
                'source_path': '/home/igf/docs/igf/IGF operation/ADMIN/DB tables/Database2_be.accdb'},
            python_callable=copy_remote_file_to_hpc_func)
    ## TASK
    get_known_projects = \
        PythonOperator(
            task_id="get_known_projects",
            dag=dag,
            queue='hpc_4G',
            params={
                'xcom_key': 'known_projects'},
            python_callable=get_known_projects_func)
    ## TASK
    create_raw_metadata_for_new_projects = \
        PythonOperator(
            task_id="create_raw_metadata_for_new_projects",
            dag=dag,
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
            python_callable=create_raw_metadata_for_new_projects_func)
    ## TASK
    get_formatted_metadata_files = \
        PythonOperator(
            task_id="get_formatted_metadata_files",
            dag=dag,
            queue='hpc_4G',
            params={
                'xcom_key': 'formatted_metadata',
                'raw_metadata_xcom_key': 'metadata_dir',
                'raw_metadata_xcom_task': 'create_raw_metadata_for_new_projects'
            },
            python_callable=get_formatted_metadata_files_func)
    ## TASK
    upload_raw_metadata_to_portal = \
        PythonOperator(
            task_id="upload_raw_metadata_to_portal",
            dag=dag,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                'formatted_metadata_xcom_key': 'formatted_metadata',
                'formatted_metadata_xcom_task': 'get_formatted_metadata_files'
            },
            python_callable=upload_raw_metadata_to_portal_func)
    ## TASK
    fetch_validated_metadata_from_portal_and_load = \
        SSHOperator(
            task_id="fetch_validated_metadata_from_portal_and_load",
            dag=dag,
            queue='hpc_4G',
            pool='igf_portal_pool',
            ssh_hook=orwell_ssh_hook,
            command="""
                source /home/igf/igf_code/IGF-cron-scripts/orwell/env.sh;
                python /home/igf/igf_code/data-management-python/scripts/seqrun_processing/find_and_register_project_metadata_from_portal_db.py \
                    --portal_db_conf_file $PORTAL_CONF_FILE \
                    --dbconfig $DBCONF \
                    --user_account_template /home/igf/igf_code/data-management-python/template/email_notification/send_new_account_info.txt \
                    --log_slack \
                    --slack_config $SLACKCONF \
                    --setup_irods \
                    --notify_user""")
    ## TASK
    get_metadata_dump_from_pipeline_db = \
        PythonOperator(
            task_id="get_metadata_dump_from_pipeline_db",
            dag=dag,
            queue='hpc_4G',
            params={
                'json_dump_xcom_key': 'json_dump'},
            python_callable=get_metadata_dump_from_pipeline_db_func)
    ## TASK
    upload_metadata_to_portal_db = \
        PythonOperator(
            task_id="upload_metadata_to_portal_db",
            dag=dag,
            queue="hpc_4G",
            pool='igf_portal_pool',
            params={
                "json_dump_xcom_key": "json_dump",
                "json_dump_xcom_task": "get_metadata_dump_from_pipeline_db",
                "data_load_url": "/api/v1/metadata/load_metadata"},
            python_callable=upload_metadata_to_portal_db_func)
    ## PIPELINE
    copy_quota_xlsx >> create_raw_metadata_for_new_projects
    copy_access_db >> create_raw_metadata_for_new_projects
    get_known_projects >> create_raw_metadata_for_new_projects
    create_raw_metadata_for_new_projects >> get_formatted_metadata_files
    get_formatted_metadata_files >> upload_raw_metadata_to_portal
    upload_raw_metadata_to_portal >> fetch_validated_metadata_from_portal_and_load
    fetch_validated_metadata_from_portal_and_load >> get_metadata_dump_from_pipeline_db
    get_metadata_dump_from_pipeline_db >> upload_metadata_to_portal_db