import os
import pendulum
from datetime import timedelta
import queue
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from igf_airflow.utils.dag20_portal_metadata_utils import (
    get_metadata_dump_from_pipeline_db_func,
    upload_metadata_to_portal_db_func,
    copy_remote_file_to_hpc_func,
    get_known_projects_func,
    create_raw_metadata_for_new_projects_func,
    get_formatted_metadata_files_func,
    upload_raw_metadata_to_portal_func,
    fetch_validated_metadata_from_portal_and_load_func)
from igf_airflow.utils.dag30_register_raw_analysis_to_pipeline_db_utils import (
    fetch_raw_analysis_queue_func,
    process_raw_analysis_queue_func)


# args = {
#     'owner': 'airflow',
#     'start_date': pendulum.today('UTC').add(days=2),
#     'retries': 4,
#     'retry_delay': timedelta(minutes=5),
#     'provide_context': True,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'catchup': False}

## Toggle for switching portal server
PORTAL_SERVER_MODE = 'PROD'
## change it to BACKUP when backup db in use
# PORTAL_SERVER_MODE = 'BACKUP'

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

igfportal_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfportal_server_hostname'))

igfdata_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfdata_server_hostname'))

igflims_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igf_lims_server_hostname'))


dag = \
    DAG(
        dag_id=DAG_ID,
        schedule="@hourly",
        start_date=pendulum.yesterday(),
        dagrun_timeout=timedelta(minutes=30),
        catchup=False,
        max_active_runs=1,
        default_view='grid',
        orientation='TB',
        tags=['hpc', 'portal', 'raw_metadata', 'raw_analysis', 'db', 'backup'])
with dag:
    ## TASK
    copy_quota_xlsx = \
        PythonOperator(
            task_id="copy_quota_xlsx",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            params={
                'xcom_key': 'quota_xlsx',
                'hpc_ssh_key_file': None,
                'source_address': None,
                'source_user': None,
                'source_path': '/rds/general/project/genomics-facility-archive-2019/live/orwell_access_lims/docs/igf/IGF operation/ADMIN/DB tables/Quotes.xlsx'},
            python_callable=copy_remote_file_to_hpc_func)
    ## TASK
    copy_access_db = \
        PythonOperator(
            task_id="copy_access_db",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            params={
                'xcom_key': 'access_db',
                'hpc_ssh_key_file': None,
                'source_address': None,
                'source_user': None,
                'source_path': '/rds/general/project/genomics-facility-archive-2019/live/orwell_access_lims/docs/igf/IGF operation/ADMIN/DB tables/Database2_be.accdb'},
            python_callable=copy_remote_file_to_hpc_func)
    ## TASK
    get_known_projects = \
        PythonOperator(
            task_id="get_known_projects",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            params={
                'xcom_key': 'known_projects'},
            python_callable=get_known_projects_func)
    ## TASK
    create_raw_metadata_for_new_projects = \
        PythonOperator(
            task_id="create_raw_metadata_for_new_projects",
            dag=dag,
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
            python_callable=create_raw_metadata_for_new_projects_func)
    ## TASK
    get_formatted_metadata_files = \
        PythonOperator(
            task_id="get_formatted_metadata_files",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
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
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                'formatted_metadata_xcom_key': 'formatted_metadata',
                'formatted_metadata_xcom_task': 'get_formatted_metadata_files'
            },
            python_callable=upload_raw_metadata_to_portal_func)
    ## TASK
    fetch_validated_metadata_from_portal_and_load = \
        PythonOperator(
            task_id="fetch_validated_metadata_from_portal_and_load",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            params={},
            python_callable=fetch_validated_metadata_from_portal_and_load_func
        )
    ## TASK
    fetch_raw_analysis_queue = \
        PythonOperator(
            task_id="fetch_raw_analysis_queue",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                'new_raw_analysis_list_key': 'new_raw_analysis_list'
            },
            python_callable=fetch_raw_analysis_queue_func
        )
    ## TASK
    process_raw_analysis_queue = \
        PythonOperator(
            task_id="process_raw_analysis_queue",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                "new_raw_analysis_list_key": "new_raw_analysis_list",
                "new_raw_analysis_list_task": "fetch_raw_analysis_queue"
            },
            python_callable=process_raw_analysis_queue_func
        )
    ## TASK
    backup_prod_db = \
        BashOperator(
            task_id='backup_prod_db',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command='bash /rds/general/user/igf/home/secret_keys/get_hourly_prod_db_dump.sh ')
    ## TASK
    backup_portal_db = \
        SSHOperator(
            task_id='backup_portal_db',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igfportal_ssh_hook,
            queue='hpc_4G',
            pool='igfportal_ssh_pool',
            command="bash /home/igf/dev/backup_cmd.sh ")
    ## TASK
    load_data_to_legacy_prod_db = \
        BashOperator(
            task_id="load_data_to_legacy_prod_db",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command='bash /rds/general/user/igf/home/secret_keys/update_legacy_prod_db.sh ')
    ## TASK
    copy_prod_db_to_igfdata = \
        BashOperator(
            task_id="copy_prod_db_to_igfdata",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command='bash /project/tgu/data2/airflow_v3/secrets/copy_prod_db_to_igfdata.sh ')
    ## TASK
    update_db_on_igfdata = \
        SSHOperator(
            task_id='update_db_on_igfdata',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igfdata_ssh_hook,
            queue='hpc_4G',
            pool='igfdata_ssh_pool',
            conn_timeout=300,
            cmd_timeout=300,
            command="bash /home/igf/superset/test1/db/db_update_script.sh ")
    ## TASK
    copy_portal_backup_to_hpc = \
        BashOperator(
            task_id="copy_portal_backup_to_hpc",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_portal_dump.sh ")
    ## TASK
    get_metadata_dump_from_pipeline_db = \
        PythonOperator(
            task_id="get_metadata_dump_from_pipeline_db",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            params={
                'json_dump_xcom_key': 'json_dump'},
            python_callable=get_metadata_dump_from_pipeline_db_func)
    ## TASK
    upload_metadata_to_portal_db = \
        PythonOperator(
            task_id="upload_metadata_to_portal_db",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue="hpc_4G",
            pool='igf_portal_pool',
            params={
                "json_dump_xcom_key": "json_dump",
                "json_dump_xcom_task": "get_metadata_dump_from_pipeline_db",
                "data_load_url": "/api/v1/metadata/load_metadata"},
            python_callable=upload_metadata_to_portal_db_func)
    ## TASK
    backup_nf_tower_db = \
        SSHOperator(
            task_id='backup_nf_tower_db',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igflims_ssh_hook,
            queue='hpc_4G',
            pool='igf_lims_ssh_pool',
            conn_timeout=300,
            cmd_timeout=300,
            command="bash /home/igf/nextflow_tower/tower_v20220627/db_backup.sh ")
    ## TASK
    copy_nf_tower_backup_to_hpc = \
        BashOperator(
            task_id="copy_nf_tower_backup_to_hpc",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_nf_dump.sh ")
    ## TASK
    copy_nf_tower_backup_to_igfdata = \
        BashOperator(
            task_id="copy_nf_tower_backup_to_igfdata",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_nf_dump_to_igfdata.sh ")
    ## TASK
    update_tower_db_on_igfdata = \
        SSHOperator(
            task_id='update_tower_db_on_igfdata',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igfdata_ssh_hook,
            queue='hpc_4G',
            pool='igfdata_ssh_pool',
            conn_timeout=300,
            cmd_timeout=300,
            command="bash /home/igf/superset/test1/db/tower_db_update_script.sh ")
    ## TASK
    backup_airflow_db = \
        SSHOperator(
            task_id='backup_airflow_db',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igflims_ssh_hook,
            queue='hpc_4G',
            pool='igf_lims_ssh_pool',
            conn_timeout=300,
            cmd_timeout=300,
            command="bash /home/igf/airflow_db_backup/backupdb.sh ")
    ## TASK
    copy_airflow_backup_to_hpc = \
        BashOperator(
            task_id="copy_airflow_backup_to_hpc",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_airflow_dump.sh ")
    ## TASK
    copy_airflow_backup_to_igfdata = \
        BashOperator(
            task_id="copy_airflow_backup_to_igfdata",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_airflow_dump_to_igfdata.sh ")
    ## TASK
    update_airflow_db_on_igfdata = \
        SSHOperator(
            task_id='update_airflow_db_on_igfdata',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igfdata_ssh_hook,
            queue='hpc_4G',
            pool='igfdata_ssh_pool',
            conn_timeout=300,
            cmd_timeout=300,
            command="bash /home/igf/superset/test1/db/airflow_db_update_script.sh ")
    ## TASK
    backup_superset_db_on_igfdata = \
        SSHOperator(
            task_id='backup_superset_db_on_igfdata',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igfdata_ssh_hook,
            queue='hpc_4G',
            pool='igfdata_ssh_pool',
            conn_timeout=300,
            cmd_timeout=300,
            command="bash /home/igf/superset/test1/db/superset_db_backup.sh ")
    ## TASK
    copy_superset_backup_to_hpc = \
        BashOperator(
            task_id="copy_superset_backup_to_hpc",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_superset_dump.sh ")
    ## TASK
    backup_portal_static_dir = \
        SSHOperator(
            task_id='backup_portal_static_dir',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            ssh_hook=igfportal_ssh_hook,
            queue='hpc_4G',
            pool='igfportal_ssh_pool',
            command="bash /home/igf/scripts/create_backup_for_static_dir.sh ")
    ## TASK
    copy_portal_static_backup_to_hpc = \
        BashOperator(
            task_id="copy_portal_static_backup_to_hpc",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_portal_static_dump.sh ")
    ## TASK
    copy_portal_static_backup_to_igfdata = \
        BashOperator(
            task_id="copy_portal_static_backup_to_igfdata",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_portal_static_dump_igfdata.sh ")
    ## TASK
    copy_portal_db_backup_to_igfdata = \
        BashOperator(
            task_id="copy_portal_db_backup_to_igfdata",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=4,
            queue='hpc_4G',
            bash_command="bash /rds/general/user/igf/home/secret_keys/copy_hourly_portal_dump_to_igfdata.sh ")
    ## PIPELINE
    copy_quota_xlsx >> create_raw_metadata_for_new_projects
    copy_access_db >> create_raw_metadata_for_new_projects
    get_known_projects >> create_raw_metadata_for_new_projects
    create_raw_metadata_for_new_projects >> get_formatted_metadata_files
    get_formatted_metadata_files >> upload_raw_metadata_to_portal
    upload_raw_metadata_to_portal >> fetch_validated_metadata_from_portal_and_load
    upload_raw_metadata_to_portal >> fetch_raw_analysis_queue
    fetch_raw_analysis_queue >> process_raw_analysis_queue
    fetch_validated_metadata_from_portal_and_load >> get_metadata_dump_from_pipeline_db
    process_raw_analysis_queue >> get_metadata_dump_from_pipeline_db
    fetch_validated_metadata_from_portal_and_load >> backup_prod_db
    process_raw_analysis_queue >> backup_prod_db
    get_metadata_dump_from_pipeline_db >> upload_metadata_to_portal_db
    backup_prod_db >> backup_portal_db
    backup_prod_db >> load_data_to_legacy_prod_db >> copy_prod_db_to_igfdata >> update_db_on_igfdata
    backup_portal_db >> copy_portal_backup_to_hpc
    backup_portal_db >> upload_metadata_to_portal_db
    backup_nf_tower_db >> copy_nf_tower_backup_to_hpc >> copy_nf_tower_backup_to_igfdata
    copy_nf_tower_backup_to_igfdata >> update_tower_db_on_igfdata
    backup_airflow_db >> copy_airflow_backup_to_hpc >> copy_airflow_backup_to_igfdata
    copy_airflow_backup_to_igfdata >> update_airflow_db_on_igfdata
    backup_superset_db_on_igfdata >> copy_superset_backup_to_hpc
    backup_portal_db >> backup_portal_static_dir
    backup_portal_static_dir >> copy_portal_static_backup_to_hpc
    copy_portal_static_backup_to_hpc >> copy_portal_static_backup_to_igfdata
    copy_portal_backup_to_hpc >> copy_portal_db_backup_to_igfdata
    update_tower_db_on_igfdata >> backup_superset_db_on_igfdata
    update_airflow_db_on_igfdata >> backup_superset_db_on_igfdata
    update_db_on_igfdata >> backup_superset_db_on_igfdata

