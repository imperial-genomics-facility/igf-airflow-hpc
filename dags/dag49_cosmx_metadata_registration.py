import os, pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag49_cosmx_metadata_registration_utils import (
    find_raw_metadata_id,
    register_cosmx_metadata
)

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
    tags=["portal", "metadata", "cosmx"]
)
def dag49_cosmx_metadata_registration():
    raw_cosmx_metadata_id = find_raw_metadata_id()
    register_cosmx_metadata(
        raw_cosmx_metadata_id=raw_cosmx_metadata_id
    )


dag49_cosmx_metadata_registration()