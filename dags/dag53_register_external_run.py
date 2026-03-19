import os
import pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag53_register_external_run_utils import (
    create_qc_and_load_external_run
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
    tags=["external", "seqrun"]
)
def dag53_register_external_run():
    ## TASK1
    create_qc_and_load_external_run()


dag53_register_external_run()