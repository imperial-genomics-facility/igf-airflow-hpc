import os, pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task_group, task

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")

## TASK - find raw metadata id in datrun.conf
@task(
    task_id="find_raw_metadata_id"
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def find_raw_metadata_id():
    return {"raw_metadata_id": 1}

## TASK - fetch raw analysis metadata from portal
@task(
    task_id="fetch_raw_metadata_from_portal"
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def fetch_raw_metadata_from_portal(raw_metadata_id):
    return {"raw_metadata_file": "raw_metadata.json"}

## TASK - check raw metadata in db
@task(
    task_id="check_raw_metadata_in_db"
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def check_raw_metadata_in_db(raw_metadata_file):
    return {"valid_raw_metadata_file": "raw_metadata.json"}

## TASK - register raw metadata in db
@task(
    task_id="register_raw_metadata_in_db"
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def register_raw_metadata_in_db(valid_raw_metadata_file):
    return {"status": True}

## TASK - mark raw metadata as synced on portal
@task(
    task_id="mark_metadata_synced_on_portal"
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G',
    multiple_outputs=False)
def mark_metadata_synced_on_portal(raw_metadata_id, registration_status):
    return {"status": True}

@dag(
    dag_id=DAG_ID,
	schedule=None,
	start_date=pendulum.yesterday(),
	catchup=True,
	max_active_runs=1,
    default_view='grid',
    orientation='TB',
    tags=["portal", "analysis", "registration"])
def dag44_analysis_registration():
    raw_metadata_info = \
        find_raw_metadata_id()
    raw_metadata_file_info = \
        fetch_raw_metadata_from_portal(
            raw_metadata_id=raw_metadata_info.raw_metadata_id)
    valid_raw_metadata_file_info = \
        check_raw_metadata_in_db(
            raw_metadata_file=raw_metadata_file_info.raw_metadata_file)
    registered_metadata = \
        register_raw_metadata_in_db(
            valid_raw_metadata_file=valid_raw_metadata_file_info.valid_raw_metadata_file)
    sync_info = \
        mark_metadata_synced_on_portal(
            raw_metadata_id=raw_metadata_info.raw_metadata_id,
            registration_status=registered_metadata.status)
    

dag44_analysis_registration()