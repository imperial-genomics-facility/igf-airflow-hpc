import os, pendulum
from airflow.decorators import dag
from airflow.utils.edgemodifier import Label
from airflow.operators.empty import EmptyOperator
from igf_airflow.utils.dag37_project_cleanup_step1_utils import (
    find_project_data_for_cleanup,
    upload_project_cleanup_data_to_portal)

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
## schedule: at 00:30 on day-of-month 1
@dag(
    dag_id=DAG_ID,
	schedule="30 00 1 * *",
	start_date=pendulum.yesterday(),
	catchup=False,
	max_active_runs=1,
    default_view='grid',
    orientation='TB',
    tags=["project cleanup", "hpc"])
def project_cleanup_step1():
    no_task = EmptyOperator(task_id="no_task")
    find_project_instance = \
        find_project_data_for_cleanup(
            next_task='upload_project_cleanup_data_to_portal',
            no_task='no_task',
            xcom_key='json_file')
    ## no work branch
    find_project_instance >> Label('No task') >> no_task
    upload_project_cleanup_instance = \
        upload_project_cleanup_data_to_portal(
            xcom_key='json_file',
            xcom_task='find_projects_for_cleanup')
    ## upload file branch
    find_project_instance >> Label('Data found') >> upload_project_cleanup_instance

project_cleanup_step1()