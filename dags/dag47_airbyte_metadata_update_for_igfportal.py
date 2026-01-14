import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
# from airflow.providers.airbyte.operators.airbyte import (
#     AirbyteTriggerSyncOperator)
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator)

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
PORTALDB_CONNECTION_ID = Variable.get(
    'portaldb_connection_id_for_data_loading',
    default_var="portal_db_conn"
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

doc_md = """
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
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_view='grid',
    orientation='TB',
    doc_md=doc_md,
    tags=["airbyte", "metadata", "igfportal"])
def dag47_airbyte_metadata_update_for_igfportal():
    ## TASK - stop IGFPortal
    stop_portal_server = SSHOperator(
        task_id='stop_portal_server',
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
           #cd $HOME/dev;
           #docker compose \
           #    -f IGFPortal/docker-compose-igfportal-prod.yaml \
           #    -p igfportal \
           #    stop webserver nginx celery_worker1 celery_flower;
            sleep 1
        """
    )
    ## TASK - load project data
    load_project_data = SSHOperator(
        task_id='load_project_data',
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            cd $HOME/dev/data_loading;
            docker exec -i igfdb bash -c \
                'mysqldump -h igfdb -u root --password=$MYSQL_ROOT_PASSWORD \
                --add-drop-table $MYSQL_DATABASE project > /tmp/project.sql'
            docker cp igfdb:/tmp/project.sql .
            docker exec -i igfdb rm -f /tmp/project.sql
            docker cp project.sql portal_db:/tmp/project.sql
            docker exec -i portal_db bash -c \
                'mysql -h portal_db -u igfrw -p$MYSQL_PASSWORD $MYSQL_DATABASE \
                </tmp/project.sql'
            docker exec -i portal_db rm -f /tmp/project.sql
            rm -f project.sql
        """
    )
    ## TASK - load user data
    load_user_data = SSHOperator(
        task_id='load_user_data',
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            cd $HOME/dev/data_loading;
            docker exec -i igfdb bash -c \
                'mysqldump -h igfdb -u root --password=$MYSQL_ROOT_PASSWORD \
                --add-drop-table $MYSQL_DATABASE user > /tmp/user.sql'
            docker cp igfdb:/tmp/user.sql .
            docker exec -i igfdb rm -f /tmp/user.sql
            docker cp user.sql portal_db:/tmp/user.sql
            docker exec -i portal_db bash -c \
                'mysql -h portal_db -u igfrw -p$MYSQL_PASSWORD $MYSQL_DATABASE \
                </tmp/user.sql'
            docker exec -i portal_db rm -f /tmp/user.sql
            rm -f user.sql
        """
    )
    ## TASK - load pipeline data
    load_pipeline_data = SSHOperator(
        task_id='load_pipeline_data',
        retry_delay=timedelta(minutes=2),
        retries=2,
        ssh_hook=igfportal_ssh_hook,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        command="""
            cd $HOME/dev/data_loading;
            docker exec -i igfdb bash -c \
                'mysqldump -h igfdb -u root --password=$MYSQL_ROOT_PASSWORD \
                --add-drop-table $MYSQL_DATABASE pipeline > /tmp/pipeline.sql'
            docker cp igfdb:/tmp/pipeline.sql .
            docker exec -i igfdb rm -f /tmp/pipeline.sql
            docker cp pipeline.sql portal_db:/tmp/pipeline.sql
            docker exec -i portal_db bash -c \
                'mysql -h portal_db -u igfrw -p$MYSQL_PASSWORD $MYSQL_DATABASE \
                </tmp/pipeline.sql'
            docker exec -i portal_db rm -f /tmp/pipeline.sql
            rm -f pipeline.sql
        """
    )
    ## TASK - load raw data
    # load_raw_data_to_portal = AirbyteTriggerSyncOperator(
    #     task_id="load_raw_data_to_portal",
    #     airbyte_conn_id=AIRBYTE_CONNECTION_ID,
    #     connection_id=AIRBYTE_SYNC_ID,
    #     queue='hpc_4G',
    #     pool=IGFPORTAL_POOL,
    #     retries=2
    # )
    ## TASK - load new project data
    load_new_projects_to_portal = SQLExecuteQueryOperator(
        task_id='load_new_projects_to_portal',
        retry_delay=timedelta(minutes=2),
        retries=2,
        conn_id=PORTALDB_CONNECTION_ID,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        sql="""
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
                CAST(REPLACE(p.start_timestamp, '.000000Z', '') AS DATETIME) AS start_timestamp,
                p.description,
                p.status,
                p.deliverable
            FROM project p
            WHERE p.project_id > (
                SELECT COALESCE(MAX(project_id), 0) FROM raw_project)
        """,
        split_statements=False,
        return_last=False
    )
    ## TASK - load new pipeline data
    load_new_pipelines_to_portal = SQLExecuteQueryOperator(
        task_id='load_new_pipelines_to_portal',
        retry_delay=timedelta(minutes=2),
        retries=2,
        conn_id=PORTALDB_CONNECTION_ID,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        sql="""
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
                CAST(REPLACE(ps.date_stamp, '.000000Z', '') AS DATETIME) AS date_stamp
            FROM pipeline ps
            WHERE ps.pipeline_id > (
                SELECT COALESCE(MAX(pipeline_id), 0) FROM raw_pipeline)
        """,
        split_statements=False,
        return_last=False
    )
    ## TASK - load new user data
    load_new_users_to_portal = SQLExecuteQueryOperator(
        task_id='load_new_users_to_portal',
        retry_delay=timedelta(minutes=2),
        retries=2,
        conn_id=PORTALDB_CONNECTION_ID,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        sql="""
            INSERT INTO raw_user (
                user_id,
                user_igf_id,
                name,
                email_id,
                status)
            SELECT 
                u.user_id,
                u.user_igf_id,
                u.name,
                u.email_id,
                u.status
            FROM user u
            WHERE u.user_id > (
                SELECT COALESCE(MAX(user_id), 0) FROM raw_user)
        """,
        split_statements=False,
        return_last=False
    )
    ## TASK - update existing project data
    update_existing_projects_to_portal = SQLExecuteQueryOperator(
        task_id='update_existing_projects_to_portal',
        retry_delay=timedelta(minutes=2),
        retries=2,
        conn_id=PORTALDB_CONNECTION_ID,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        sql="""
            UPDATE raw_project rp
            JOIN project p ON p.project_id=rp.project_id
            SET rp.status=p.status
            WHERE rp.status != p.status
        """,
        split_statements=False,
        return_last=False
    )
    ## TASK - update existing pipeline data
    update_existing_pipelines_to_portal = SQLExecuteQueryOperator(
        task_id='update_existing_pipelines_to_portal',
        retry_delay=timedelta(minutes=2),
        retries=2,
        conn_id=PORTALDB_CONNECTION_ID,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        sql="""
            UPDATE raw_pipeline rp
            JOIN pipeline p ON p.pipeline_id=rp.pipeline_id
            SET rp.is_active=p.is_active
            WHERE rp.is_active != p.is_active
        """,
        split_statements=False,
        return_last=False
    )
    ## TASK - update existing user data
    update_existing_users_to_portal = SQLExecuteQueryOperator(
        task_id='update_existing_users_to_portal',
        retry_delay=timedelta(minutes=2),
        retries=2,
        conn_id=PORTALDB_CONNECTION_ID,
        queue='hpc_4G',
        pool=IGFPORTAL_POOL,
        sql="""
            UPDATE raw_user ru
            JOIN user u ON u.user_id=ru.user_id
            SET ru.status=u.status
            WHERE ru.status != u.status
        """,
        split_statements=False,
        return_last=False
    )
    ## TASK - start IGFPortal webserver
    # start_portal_webserver = SSHOperator(
    #     task_id='stop_docker_compose',
    #     retry_delay=timedelta(minutes=2),
    #     retries=4,
    #     ssh_hook=igfportal_ssh_hook,
    #     queue='hpc_4G',
    #     pool=IGFPORTAL_POOL,
    #     command="""
    #         cd $HOME/dev;
    #         docker compose \
    #             -f IGFPortal/docker-compose-igfportal-prod.yaml \
    #             -p igfportal \
    #             start webserver nginx;
    #         sleep 2
    #     """
    # )
    ## TASK - start IGFPortal celery worker
    # start_portal_celery_worker = SSHOperator(
    #     task_id='start_docker_compose',
    #     retry_delay=timedelta(minutes=2),
    #     retries=4,
    #     ssh_hook=igfportal_ssh_hook,
    #     queue='hpc_4G',
    #     pool=IGFPORTAL_POOL,
    #     command="""
    #         cd $HOME/dev;
    #         docker compose \
    #             -f IGFPortal/docker-compose-igfportal-prod.yaml \
    #             -p igfportal \
    #             start celery_worker1 celery_flower;
    #         sleep 2
    #     """
    # )
    ## PIPELINE
    stop_portal_server >> load_project_data
    load_project_data >> load_new_projects_to_portal
    load_new_projects_to_portal >> update_existing_projects_to_portal
    stop_portal_server >> load_pipeline_data
    load_pipeline_data >> load_new_pipelines_to_portal
    load_new_pipelines_to_portal >> update_existing_pipelines_to_portal
    stop_portal_server >> load_user_data
    load_user_data >> load_new_users_to_portal
    load_new_users_to_portal >> update_existing_users_to_portal
    # update_existing_projects_to_portal >> start_portal_webserver
    # update_existing_pipelines_to_portal >> start_portal_webserver
    # update_existing_users_to_portal >> start_portal_webserver
    # start_portal_webserver >> start_portal_celery_worker


dag47_airbyte_metadata_update_for_igfportal()