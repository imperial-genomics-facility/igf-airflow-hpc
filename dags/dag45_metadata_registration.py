import os, pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag45_metadata_registration_utils import register_metadata_from_portal

## DAG
DAG_ID = (
    os.path.basename(__file__)
    .replace(".pyc", "")
    .replace(".py", "")
)

@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=1,
    default_view='grid',
    orientation='TB',
    tags=["portal", "metadata", "registration"])
def dag45_metadata_registration():
    register_metadata_from_portal()


dag45_metadata_registration()