import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.airbyte.operators.airbyte import (
    AirbyteTriggerSyncOperator)

## SSH HOOK
igfportal_ssh_hook = SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('igfportal_server_hostname')
)
## CONNECTIONS
AIRBYTE_CONNECTION_ID = Variable.get(
    'airbyte_connection_id_for_portal_data_loading',
    default_var="airbyte_conn"
)
AIRBYTE_SYNC_ID = Variable.get(
    'airbyte_sync_id_for_portal_data_loading',
    default_var=None
)
## POOLS
IGFPORTAL_POOL =  Variable.get(
    'igfportal_ssh_pool_name',
    default_var='igfportal_ssh_pool'
)
## DAG
DAG_ID = (
    os.path.basename(__file__)
    .replace(".pyc", "")
    .replace(".py", "")
)

doc_md_DAG = """
### Description

* This DAG updates the IGFPortal metadata using Airbyte sync
* The DAG stops the IGFPortal server before loading data
* The DAG uses Airbyte to sync data from LIMS to IGFPortal database
* The DAG starts the IGFPortal server after data loading is complete
* The DAG is scheduled to run everyday at 23:40

"""

@dag(
    dag_id=DAG_ID,
    schedule="40 23 * * *", ## run everyday at 23:40
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=20,
    dagrun_timeout=timedelta(minutes=5),
    default_view='grid',
    orientation='TB',
    doc_md=doc_md_DAG,
    tags=["airbyte", "metadata", "igfportal"])
def dag47_airbyte_metadata_update_for_igfportal():
    ## TASK - stop IGFPortal
    stop_portal_server = SSHOperator(
        task_id='stop_portal_server',
        dag=dag,
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            cd $HOME/dev;
            docker compose \
                -f IGFPortal/docker-compose-igfportal-prod.yaml \
                -p igfportal \
                stop webserver celery_worker1 celery_flower;
            sleep 2
        """
    )
    ## load raw data
    load_raw_data_to_portal = AirbyteTriggerSyncOperator(
        task_id="load_raw_data_to_portal",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        connection_id=AIRBYTE_SYNC_ID
    )
    ## TASK - load new project data
    load_new_projects_to_portal = SSHOperator(
        task_id='load_new_projects_to_portal',
        dag=dag,
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            docker exec portal_db \
            bash -c '/usr/bin/mysql -h portal_db \
                     --user=$MYSQL_USER --password=$MYSQL_PASSWORD \
                     igfportaldb -e "
                        INSERT INTO raw_project (
                            project_id,
                            project_igf_id,
                            project_name,
                            start_timestamp,
                            description,
                            status,
                            deliverable)
                        SELECT
                            p.project_id,
                            p.project_igf_id,
                            p.project_name,
                            CAST(REPLACE(p.start_timestamp, \'.000000Z\', \'\') AS DATETIME) AS start_timestamp,
                            p.description,
                            p.status,
                            p.deliverable
                        FROM project p
                        WHERE p.project_id > (
                            SELECT COALESCE(MAX(project_id), 0) FROM raw_project)"'
        """
    )
    ## TASK - load new pipeline data
    load_new_pipelines_to_portal = SSHOperator(
        task_id='load_new_pipelines_to_portal',
        dag=dag,
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            docker exec portal_db \
            bash -c '/usr/bin/mysql -h portal_db \
                     --user=$MYSQL_USER --password=$MYSQL_PASSWORD \
                     igfportaldb -e "
                        INSERT INTO raw_pipeline (
                            pipeline_id,
                            pipeline_name,
                            pipeline_db,
                            pipeline_type,
                            is_active,
                            date_stamp)
                        SELECT 
                            ps.pipeline_id,
                            ps.pipeline_name,
                            ps.pipeline_db,
                            ps.pipeline_type,
                            ps.is_active,
                            CAST(REPLACE(ps.date_stamp, \'.000000Z\', \'\') AS DATETIME) AS date_stamp
                        FROM pipeline ps
                        WHERE ps.pipeline_id > (
                            SELECT COALESCE(MAX(pipeline_id), 0) FROM raw_pipeline)"'
        """
    )
    ## TASK - update existing project data
    update_existing_projects_to_portal = SSHOperator(
        task_id='update_existing_projects_to_portal',
        dag=dag,
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            docker exec portal_db \
            bash -c '/usr/bin/mysql -h portal_db \
                     --user=$MYSQL_USER --password=$MYSQL_PASSWORD \
                     igfportaldb -e "
                        UPDATE raw_project rp
                        JOIN project p ON p.project_id=rp.project_id
                        SET rp.status=p.status
                        WHERE rp.status != p.status"'
        """
    )
    ## TASK - update existing pipeline data
    update_existing_pipelines_to_portal = SSHOperator(
        task_id='update_existing_pipelines_to_portal',
        dag=dag,
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            docker exec portal_db \
            bash -c '/usr/bin/mysql -h portal_db \
                     --user=$MYSQL_USER --password=$MYSQL_PASSWORD \
                     igfportaldb -e "
                        UPDATE raw_pipeline rp
                        JOIN pipeline p ON p.pipeline_id=rp.pipeline_id
                        SET rp.is_active=p.is_active
                        WHERE rp.is_active != p.is_active"'
        """
    )
    ## TASK - start IGFPortal webserver
    start_portal_webserver = SSHOperator(
        task_id='start_portal_webserver',
        dag=dag,
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            cd $HOME/dev;
            docker compose \
                -f IGFPortal/docker-compose-igfportal-prod.yaml \
                -p igfportal \
                start webserver;
            sleep 2
        """
    )
    ## TASK - start IGFPortal celery worker
    start_portal_celery_worker = SSHOperator(
        task_id='start_portal_celery_worker',
        dag=dag,
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            cd $HOME/dev;
            docker compose \
                -f IGFPortal/docker-compose-igfportal-prod.yaml \
                -p igfportal \
                start celery_worker1 celery_flower;
            sleep 2
        """
    )
    ## PIPELINE
    stop_portal_server >> load_raw_data_to_portal
    load_raw_data_to_portal >> load_new_projects_to_portal
    load_new_projects_to_portal >> update_existing_projects_to_portal
    load_raw_data_to_portal >> load_new_pipelines_to_portal
    load_new_pipelines_to_portal >> update_existing_pipelines_to_portal
    update_existing_projects_to_portal >> start_portal_webserver
    update_existing_pipelines_to_portal >> start_portal_webserver
    start_portal_webserver >> start_portal_celery_worker


dag47_airbyte_metadata_update_for_igfportal()