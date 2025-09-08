import os
import pendulum
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.bash import BashOperator
from igf_airflow.utils.dag21_portal_admin_view_utils import (
    get_seqrun_counts_func,
    prepare_storage_plot_generic,
    get_pipeline_stats_func,
    create_merged_json_and_upload_to_portal_func)

DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")

## SSH HOOK
orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('orwell_server_hostname'))
## SSH HOOK
woolf_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('woolf_server_hostname'))
woolf_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('woolf_server_hostname'))
## SSH HOOK
eliot_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('eliot_server_hostname'))
eliot_ssh_hook = \
  SSHHook(ssh_conn_id='eliot_conn')
## SSH HOOK
igf_lims_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igf_lims_server_hostname'))
## SSH HOOK
wells_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('wells_server_hostname'))
## SSH HOOK
igfportal_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfportal_server_hostname'))
## SSH HOOK
igfdata_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfdata_server_hostname'))
## DAG
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule="0 */2 * * 1-5",
        start_date=pendulum.yesterday(),
        catchup=False,
        max_active_runs=1,
        tags=['hpc'])
with dag:
    ## TASK
    get_seqrun_counts = \
        PythonOperator(
            task_id='get_seqrun_counts',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'json_dump_xcom_key': 'seqrun_json_dump'},
            python_callable=get_seqrun_counts_func)
    ## TASK
    wells_home = \
        SSHOperator(
            task_id='wells_home',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=wells_ssh_hook,
            queue='hpc_4G',
            pool='wells_ssh_pool',
            command="""
                df -Pk /home|grep ol-home|awk '{print $3 " " $4 " " $6 }'
                """) #cut -d " " -f 3,4,8')
    ## TASK
    nextseq1_root = \
        SSHOperator(
            task_id='nextseq1_root',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=wells_ssh_hook,
            queue='hpc_4G',
            pool='wells_ssh_pool',
            command='bash /home/igf/airflow_v4/seqrun_copy_scripts/check_nextseq1_disk.sh ')
    ## TASK
    wells_data = \
        SSHOperator(
            task_id='wells_data',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=wells_ssh_hook,
            queue='hpc_4G',
            pool='wells_ssh_pool',
            command="""
                df |grep wellsvg-datavol1|awk '{print $3 " " $4 " " $6 }'
            """
            )#'df /data|grep -w "/data"|cut -d " " -f 3,4,8')
    ## TASK
    igf_lims_root = \
        SSHOperator(
            task_id='igf_lims_root',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=igf_lims_ssh_hook,
            queue='hpc_4G',
            pool='igf_lims_ssh_pool',
            command="""
                df |grep vg_igflims-lv_root|awk '{print $3 " " $4 " " $6 }'
                #df /|grep -w "/"|sed 's|^[[:space:]]\+||'|cut -d " " -f 2,3,9
                """)
    ## TASK
    igfportal_root = \
        SSHOperator(
            task_id='igfportal_root',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=igfportal_ssh_hook,
            queue='hpc_4G',
            pool='igfportal_ssh_pool',
            command="""
                df -Pk|grep root|awk '{print $3 " " $4 " " $6 }'
                """)
    igfdata_root = \
        SSHOperator(
            task_id='igfdata_root',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=igfdata_ssh_hook,
            queue='hpc_4G',
            pool='igfdata_ssh_pool',
            command="""
                df -Pk|grep root|awk '{print $3 " " $4 " " $6 }'
                """)
    ## TASK
    hpc_rds = \
        BashOperator(
            task_id="hpc_rds",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            bash_command='cat /rds/general/sys-info/quotas/user/igf|grep -w "genomics-facility-archive-2019" -A1|grep Live|cut -d " " -f13,15')
    ## TASK
    prepare_storage_plot = \
        PythonOperator(
            task_id="prepare_storage_plot",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'hpc_rds': 'hpc_rds',
                'xcom_key': 'storage_stat_json'},
            python_callable=prepare_storage_plot_generic)  #prepare_storage_plot_func)
    ## PIPELINE
    wells_home >> prepare_storage_plot
    wells_data >> prepare_storage_plot
    nextseq1_root >> prepare_storage_plot
    igf_lims_root >> prepare_storage_plot
    igfportal_root >> prepare_storage_plot
    igfdata_root >> prepare_storage_plot
    hpc_rds >> prepare_storage_plot
    ## TASK
    get_pipeline_stats = \
        PythonOperator(
            task_id='get_pipeline_stats',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'xcom_key': 'pipeline_stats'
            },
            python_callable=get_pipeline_stats_func)
    ## TASK
    create_merged_json_and_upload_to_portal = \
        PythonOperator(
            task_id="create_merged_json_and_upload_to_portal",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            pool='igf_portal_pool',
            params={
                'seqrun_json_xcom_task': 'get_seqrun_counts',
                'seqrun_json_xcom_key': 'seqrun_json_dump',
                'storage_stat_xcom_key': 'storage_stat_json',
                'storage_stat_xcom_task': 'prepare_storage_plot',
                'pipeline_stats_xcom_task': 'get_pipeline_stats',
                'pipeline_stats_xcom_key': 'pipeline_stats'
            },
            python_callable=create_merged_json_and_upload_to_portal_func)
    ## PIPELINE
    get_seqrun_counts >> create_merged_json_and_upload_to_portal
    prepare_storage_plot >> create_merged_json_and_upload_to_portal
    get_pipeline_stats >> create_merged_json_and_upload_to_portal
