import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
#from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.empty import EmptyOperator

## SSH HOOKS
igfportal_ssh_hook = SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfportal_server_hostname')
)
igfdata_ssh_hook = SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfdata_server_hostname')
)
## POOLS
IGFPORTAL_POOL =  Variable.get(
    'igfportal_ssh_pool_name',
    default_var='igfportal_ssh_pool'
)
IGFDATA_POOL =  Variable.get(
    'igfdata_ssh_pool_name',
    default_var='igfdata_ssh_pool'
)
## DAG
DAG_ID = (
    os.path.basename(__file__)
    .replace(".pyc", "")
    .replace(".py", "")
)

doc_md = """
### Description

* This DAG monitors the database replication status between igf-portal and igf-data servers
* The DAG is scheduled to run everyday at 23:00

"""
@dag(
    dag_id=DAG_ID,
    schedule="00 23 * * *", ## run everyday at 23:00
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_view='grid',
    orientation='TB',
    doc_md=doc_md,
    tags=["metadata", "igfportal", "igfdata"])
def dag48_monitor_db_replication():
    ## TASK - check igfdb on igf-portal server
    check_portal_igfdb = SSHOperator(
        task_id='check_portal_igfdb',
        retry_delay=timedelta(minutes=2),
        retries=1,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        do_xcom_push=True,
        get_pty=True,
        command="""
            set -e -o pipefail;
            output=$(docker exec igfdb bash -c \
                'export MYSQL_PWD=$MYSQL_ROOT_PASSWORD;
                 mysql -h igfdb -u root -e \
                    "SELECT
                     SERVICE_STATE
                     FROM
                     performance_schema.replication_applier_status;"');
            echo $output | grep ON;
            exit $?;
        """
    )
    ## TASK - check igfdb on igf-data server
    check_igfdata_igfdb = SSHOperator(
        task_id='check_igfdata_igfdb',
        retry_delay=timedelta(minutes=2),
        retries=1,
        ssh_hook=igfdata_ssh_hook,
        queue='hpc_4G',
        pool=IGFDATA_POOL,
        do_xcom_push=True,
        get_pty=True,
        command="""
            set -e -o pipefail;
            output=$(docker exec pipeline_db bash -c \
                'export MYSQL_PWD=$MYSQL_ROOT_PASSWORD;
                 mysql -h pipeline_db -u root -e \
                    "SELECT
                     SERVICE_STATE
                     FROM
                     performance_schema.replication_applier_status;"');
            echo $output | grep ON;
            exit $?;
        """
    )
    ## TASK - check portal_db on igf-data server
    check_igfdata_portaldb = SSHOperator(
        task_id='check_igfdata_portaldb',
        retry_delay=timedelta(minutes=2),
        retries=1,
        ssh_hook=igfdata_ssh_hook,
        queue='hpc_4G',
        pool=IGFDATA_POOL,
        do_xcom_push=True,
        get_pty=True,
        command="""
            set -e -o pipefail;
            output=$(docker exec portal_db bash -c \
                'export MYSQL_PWD=$MYSQL_ROOT_PASSWORD;
                 mysql -h portal_db -u root -e \
                    "SELECT
                     SERVICE_STATE
                     FROM
                     performance_schema.replication_applier_status;"');
            echo $output | grep ON;
            exit $?;
        """
    )
    ## TASK - empty
    last_task = EmptyOperator(task_id="last_task")
    ## PIPELINE
    check_portal_igfdb >> last_task
    check_igfdata_igfdb >> last_task
    check_igfdata_portaldb >> last_task


dag48_monitor_db_replication()