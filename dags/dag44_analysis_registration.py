import os, pendulum
from airflow.decorators import dag
from igf_airflow.utils.dag44_analysis_registration_utils import (
    find_raw_metadata_id,
    fetch_raw_metadata_from_portal,
    check_raw_metadata_in_db,
    register_raw_analysis_metadata_in_db,
    mark_metadata_synced_on_portal)

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
    tags=["portal", "analysis", "registration"])
def dag44_analysis_registration():
    raw_metadata_info = \
        find_raw_metadata_id()
    raw_metadata_file_info = \
        fetch_raw_metadata_from_portal(
            raw_metadata_id=raw_metadata_info["raw_analysis_id"])
    valid_raw_metadata_file_info = \
        check_raw_metadata_in_db(
            raw_metadata_file=raw_metadata_file_info["raw_metadata_file"])
    registered_metadata = \
        register_raw_analysis_metadata_in_db(
            valid_raw_metadata_file=valid_raw_metadata_file_info["valid_raw_metadata_file"])
    _ = \
        mark_metadata_synced_on_portal(
            raw_analysis_id=raw_metadata_info["raw_analysis_id"],
            registration_status=registered_metadata["status"])


dag44_analysis_registration()