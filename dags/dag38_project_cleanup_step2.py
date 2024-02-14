import os, pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag38_project_cleanup_step2_utils import (
    fetch_project_cleanup_data,
    notify_user_about_project_cleanup,
    mark_user_notified_on_portal)

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
@dag(
    dag_id=DAG_ID,
	schedule=None,
	start_date=pendulum.yesterday(),
	catchup=True,
	max_active_runs=1,
    default_view='grid',
    orientation='TB',
    tags=["project cleanup", "hpc"])
def dag38_project_cleanup_step2():
    json_file = \
      fetch_project_cleanup_data()
    notify = \
      notify_user_about_project_cleanup(
        project_cleanup_data_file=json_file)
    notify >> mark_user_notified_on_portal()

dag38_project_cleanup_step2()