import os, pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag45_metadata_registration_utils import (
  find_raw_metadata_id,
  register_metadata_from_portal)

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
    raw_metadata_id = \
        find_raw_metadata_id()
    register_metadata_from_portal(
        raw_metadata_id=raw_metadata_id)


dag45_metadata_registration()