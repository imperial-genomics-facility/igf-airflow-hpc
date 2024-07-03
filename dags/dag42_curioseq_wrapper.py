import json, time, os
import pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow import XComArg
from yaml import load, SafeLoader

## TASK
@task.branch(task_id="mark_analysis_running")
def mark_analysis_running() -> list:
    return ["fetch_analysis_design"]

## TASK
@task(task_id="mark_analysis_finished")
def mark_analysis_finished() -> None:
    pass

## TASK
@task(task_id="mark_analysis_failed", trigger_rule="all_failed")
def mark_analysis_failed() -> None:
    pass

# TASK 1: fetch design from db
@task(task_id="fetch_analysis_design")
def fetch_analysis_design() -> str:
    design = """sample_metadata:
  IGF1:
    beadBarcode: /path/barcode
  IGF2:
    beadBarcode: /path/barcode
analysis_metadata:
  curioseq_config:
    - "-config /path/curio_config"
    """
    return design

# Task 2: get analysis grouping
@task(task_id="get_analysis_groups")
def get_analysis_groups(design: str) -> list:
    json_data = load(design, Loader=SafeLoader)
    sample_metadata = json_data.get("sample_metadata")
    analysis_metadata = json_data.get("analysis_metadata")
    analysis_groups = list()
    for sample_id, sample_info in sample_metadata.items():
        analysis_groups.append({
            "sample_metadata": {
                sample_id: sample_info
            },
            "analysis_metadata": analysis_metadata
        })
    return analysis_groups

## TASK
@task(task_id="create_main_work_dir")
def create_main_work_dir() -> str:
    return "/path/work"

## TASK
@task(task_id="merge_fastqs_for_analysis")
def merge_fastqs_for_analysis(analysis_entry: dict) -> dict:
    sample_metadata = analysis_entry.get("sample_metadata")
    sample_id = list(sample_metadata.keys())
    analysis_metadata = analysis_entry.get("analysis_metadata")
    # if sample_id[0] == "IGF2":
    #     raise ValueError(f"I don't like {sample_id}")
    return {"sample_id": sample_id, "output": "/file/path/fastqs"}

## TASK
@task(task_id="prepare_analysis_scripts")
def prepare_analysis_scripts(analysis_entry: dict) -> dict:
    sample_metadata = analysis_entry.get("sample_metadata")
    sample_id = list(sample_metadata.keys())
    analysis_metadata = analysis_entry.get("analysis_metadata")
    # if sample_id[0] == "IGF2":
    #     raise ValueError(f"I don't like {sample_id}")
    return {"sample_id": sample_id, "output": "/file/path"}

# Task grpup1 task
@task(task_id="run_analysis")
def run_analysis(analysis_info: dict) -> dict:
    sample_metadata = analysis_info.get("sample_metadata")
    sample_id = analysis_info.get("sample_id")
    output = analysis_info.get("output")
    return {"sample_id": sample_id, "output": output}

# Task grpup1 task
@task(task_id="run_squidpy_qc")
def run_squidpy_qc(analysis_out: dict) -> dict:
    sample_id = analysis_out.get("sample_id")
    output = analysis_out.get("output")
    ## generate report and move it to visium output directory
    return {"sample_id": sample_id, "output": output}

# Task grpup1 task
@task(task_id="move_analysis")
def move_analysis(analysis_output: dict, work_dir: str) -> dict:
    sample_id = analysis_output.get("sample_id")
    output = analysis_output.get("output")
    final_output = f"{work_dir}/{sample_id}"
    return {"sample_id": sample_id, "output": final_output}

## TASK GROUP1: run script per analysis groups
@task_group
def prepare_and_run_analysis_for_each_groups(analysis_entry: dict, work_dir: str) -> dict:
    merged_fastqs = merge_fastqs_for_analysis(analysis_entry)
    analysis_info = prepare_analysis_scripts(analysis_entry=merged_fastqs)
    analysis_output = run_analysis(analysis_info=analysis_info)
    squidpy_out = run_squidpy_qc(analysis_out=analysis_output)
    final_output = move_analysis(analysis_output=squidpy_out, work_dir=work_dir)
    return final_output

# Task 3: collect all analysis outputs
@task(task_id="collect_analysis")
def collect_analysis(analysis_output_list: list) -> list:
    return analysis_output_list

# Task 6: move aggr to main work dir
@task(task_id="calculate_md5_for_work_dir", trigger_rule="none_failed")
def calculate_md5_for_work_dir() -> str:
    return "/path/output"

# Task: move aggr to main work dir
@task(task_id="load_analysis_to_db")
def load_analysis_to_db(output_dir: str) -> str:
    return "/path/loaded"

# Task: copy data to globus
@task(task_id="copy_data_to_globus")
def copy_data_to_globus(output_dir: str) -> str:
    return "/path/globus"

# Task: copy data to globus
@task(task_id="send_email_to_user")
def send_email_to_user() -> str:
    return None

## DAG
@dag(
	dag_id="curioseq_demo",
	schedule=None,
	start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
	catchup=False,
    default_view='grid',
    orientation='TB',
	tags=["example"],
)
def curioseq_demo():
    running_analysis = mark_analysis_running()
    finished_analysis = mark_analysis_finished()
    failed_analysis = mark_analysis_failed()
    running_analysis >> finished_analysis
    design = fetch_analysis_design()
    work_dir = create_main_work_dir()
    design >> work_dir
    running_analysis >> design
    analysis_groups = get_analysis_groups(design=design)
    analysis_outputs = \
        prepare_and_run_analysis_for_each_groups.\
        partial(work_dir=work_dir).\
        expand(analysis_entry=analysis_groups)
    analysis_output_list = \
      collect_analysis(analysis_outputs)
    md5_out = calculate_md5_for_work_dir()
    loaded_data = load_analysis_to_db(md5_out)
    globus_data = copy_data_to_globus(loaded_data)
    send_email = send_email_to_user()
    analysis_output_list >> md5_out
    globus_data >> send_email
    send_email >> failed_analysis
    send_email >> finished_analysis

curioseq_demo()