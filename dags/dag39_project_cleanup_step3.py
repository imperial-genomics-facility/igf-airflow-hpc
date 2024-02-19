import os, pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag38_project_cleanup_step2_utils import fetch_project_cleanup_data
from igf_airflow.utils.dag39_project_cleanup_step3_utils import (
    cleanup_old_project_in_db,
    mark_project_deleted_on_portal,
    notify_user_about_project_cleanup_finished)

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
@dag(
    dag_id=DAG_ID,
	schedule=None,
	start_date=pendulum.yesterday(),
	catchup=False,
	max_active_runs=1,
    default_view='grid',
    orientation='TB',
    tags=["project cleanup", "hpc"])
def dag39_project_cleanup_step3():
    json_file = \
        fetch_project_cleanup_data()
    cleanup_task_context = \
        cleanup_old_project_in_db(
            project_cleanup_data_file=json_file)
    cleanup_task_context >> mark_project_deleted_on_portal()
    notify_task_context = \
        notify_user_about_project_cleanup_finished(
            project_cleanup_data_file=json_file)
dag39_project_cleanup_step3()