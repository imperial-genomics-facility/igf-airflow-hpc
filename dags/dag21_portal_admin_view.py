import os
import pendulum
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.bash import BashOperator
from igf_airflow.utils.dag21_portal_admin_view_utils import (
    get_seqrun_counts_func,
    prepare_storage_plot_func,
    prepare_storage_plot_generic,
    get_pipeline_stats_func,
    create_merged_json_and_upload_to_portal_func)

# args = {
#     'owner': 'airflow',
#     'start_date': days_ago(2),
#     'retries': 4,
#     'retry_delay': timedelta(minutes=5),
#     'provide_context': True,
#     'email_on_failure': pendulum.today('UTC').add(days=2),
#     'email_on_retry': False,
#     'catchup': False,
#     'max_active_runs': 1}

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
        schedule="@hourly",
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
    # orwell_home = \
    #     SSHOperator(
    #         task_id='orwell_home',
    #         dag=dag,
    #         retry_delay=timedelta(minutes=5),
    #         retries=1,
    #         ssh_hook=orwell_ssh_hook,
    #         queue='hpc_4G',
    #         pool='orwell_exe_pool',
    #         command="""
    #             df -Pk|grep rhel_wcma--mmuelle1--s1-home|awk '{print $3 " " $4 " " $6 }'
    #             """)  #'df /home|grep -w "/home"|cut -d " " -f 3,4,7')
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
    eliot_root = \
        SSHOperator(
            task_id='eliot_root',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=eliot_ssh_hook,
            queue='hpc_4G',
            pool='eliot_ssh_pool',
            command="""
                df -Pk |grep vg_eliot-lv_root|awk '{print $3 " " $4 " " $6 }'
                #df /|grep -w "/"|sed 's|^[[:space:]]\+||'|cut -d " " -f 2,4,7
                """)
    ## TASK
    eliot_data = \
        SSHOperator(
            task_id='eliot_data',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=eliot_ssh_hook,
            queue='hpc_4G',
            pool='eliot_ssh_pool',
            command="""
                df -Pk |grep eliotVG1-dataLV1|awk '{print $3 " " $4 " " $6 }'
                #df /data|grep -w "/data"|sed 's|^[[:space:]]\+||'|cut -d " " -f 2,3,6
                """)
    ## TASK
    eliot_data2 = \
        SSHOperator(
            task_id='eliot_data2',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            ssh_hook=eliot_ssh_hook,
            queue='hpc_4G',
            pool='eliot_ssh_pool',
            command="""
                df -Pk |grep eliotVG2-dataLV2|awk '{print $3 " " $4 " " $6 }'
                #df /data2|grep -w "/data2"|sed 's|^[[:space:]]\+||'|cut -d " " -f 2,3,7
                """)
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
    # ## TASK
    # woolf_root = \
    #    SSHOperator(
    #        task_id='woolf_root',
    #        dag=dag,
    #        retry_delay=timedelta(minutes=5),
    #        retries=1,
    #        ssh_hook=woolf_ssh_hook,
    #        queue='hpc_4G',
    #        pool='woolf_ssh_pool',
    #        command="""
    #            df -Pk|grep sda2|awk '{print $3 " " $4 " " $6 }'
    #            #df /|grep -w "/"|cut -d " " -f 8,9,12
    #            """)
    ## TASK
    # woolf_data1 = \
    #    SSHOperator(
    #        task_id='woolf_data1',
    #        dag=dag,
    #        retry_delay=timedelta(minutes=5),
    #        retries=1,
    #        ssh_hook=woolf_ssh_hook,
    #        queue='hpc_4G',
    #        pool='woolf_ssh_pool',
    #        command="""
    #            df -Pk|grep vg_woolf_data1-data1|awk '{print $3 " " $4 " " $6 }'
    #            #df /data1|grep -w "/data1"|sed 's|^[[:space:]]\+||'|cut -d " " -f 2,3,6
    #            """)
    ## TASK
    # woolf_data2 = \
    #    SSHOperator(
    #        task_id='woolf_data2',
    #        dag=dag,
    #        retry_delay=timedelta(minutes=5),
    #        retries=1,
    #        ssh_hook=woolf_ssh_hook,
    #        queue='hpc_4G',
    #        pool='woolf_ssh_pool',
    #        command="""
    #            df -Pk|grep vg_woolf_data2-data2|awk '{print $3 " " $4 " " $6 }'
    #            #df /data2|grep -w "/data2"|sed 's|^[[:space:]]\+||'|cut -d " " -f 2,3,6
    #            """)
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
            # params={
            #     'orwell_home': 'orwell_home_space',
            #     'wells_home': 'wells_home_space',
            #     'wells_data': 'wells_data_space',
            #     'nextseq1_data': 'nextseq1_root_space',
            #     'eliot_root': 'eliot_root_space',
            #     'eliot_data': 'eliot_data_space',
            #     'eliot_data2': 'eliot_data2_space',
            #     'igf_lims_root': 'igf_lims_root_space',
            #     'woolf_root': 'woolf_root_space',
            #     'woolf_data1': 'woolf_data1_space',
            #     'woolf_data2': 'woolf_data2_space',
            #     'igfportal_data': 'igfportal_root_space',
            #     'hpc_rds': 'hpc_rds_space',
            #     'xcom_key': 'storage_stat_json'},
            params={
                'hpc_rds': 'hpc_rds',
                'xcom_key': 'storage_stat_json'},
            python_callable=prepare_storage_plot_generic)  #prepare_storage_plot_func)
    ## PIPELINE
    # orwell_home >> prepare_storage_plot
    wells_home >> prepare_storage_plot
    wells_data >> prepare_storage_plot
    nextseq1_root >> prepare_storage_plot
    eliot_root >> prepare_storage_plot
    eliot_data >> prepare_storage_plot
    eliot_data2 >> prepare_storage_plot
    igf_lims_root >> prepare_storage_plot
    # woolf_root >> prepare_storage_plot
    # woolf_data1 >> prepare_storage_plot
    # woolf_data2 >> prepare_storage_plot
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
