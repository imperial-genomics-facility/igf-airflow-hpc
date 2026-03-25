import os
import pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag54_metadata_rehydrate_utils import (

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
    tags=["metadata", "hpc"]
)
def dag54_metadata_rehydrate():
    pass


dag54_metadata_rehydrate()