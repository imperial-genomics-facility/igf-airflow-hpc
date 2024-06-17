import json, time, os
import pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow import XComArg
from yaml import load, SafeLoader
from igf_airflow.utils.dag33_geomx_processing_util import (
	mark_analysis_running,
	no_work,
    fetch_analysis_design_from_db,
    copy_data_to_globus,
    send_email_to_user,
    mark_analysis_finished,
	mark_analysis_failed)
from igf_airflow.utils.dag34_cellranger_multi_scRNA_utils import (
    get_analysis_group_list,
    create_main_work_dir,
    run_cellranger_script,
    collect_and_branch,
    run_cellranger_aggr_script,
    move_single_sample_result_to_main_work_dir,
    move_aggr_result_to_main_work_dir,
    calculate_md5sum_for_main_work_dir,
    load_cellranger_results_to_db)
from igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils import (
    prepare_cellranger_arc_script,
    configure_cellranger_arc_aggr_run,
    run_single_sample_scanpy_for_arc,
    dummy_task_for_single_sample,
    merged_scanpy_report_for_arc as merged_scanpy_report)


## TASK: mark analysis as running
@task.branch(task_id="mark_analysis_running")
def mark_analysis_running() -> list:
    return ["fetch_analysis_design"]

## TASK: mark analysis as finished
@task(task_id="mark_analysis_finished")
def mark_analysis_finished() -> None:
    pass

## TASK: mark analysis as failed
@task(task_id="mark_analysis_failed", trigger_rule="all_failed")
def mark_analysis_failed() -> None:
    pass

# TASK: fetch design from db
@task(task_id="fetch_analysis_design")
def fetch_analysis_design() -> str:
    design = """sample_metadata:
  IGF1:
    image: null
    darkimage: null
    colorizedimage: null
    cytaimage: null
    slide: null
    area: null
    dapi-index: null
  IGF2:
    image: null
    darkimage: null
    colorizedimage: null
    cytaimage: null
    slide: null
    area: null
    dapi-index: null
analysis_metadata:
  spaceranger_config:
    - "--transcriptome=/path"
    - "--probe-set=/path"
    - "--filter-probes=true"
    - "--reorient-images=true"
    - "--create-bam=true"
  spaceranger_aggr_config:
    - "--normalize=mapped"
    """
    return design

# Task: get analysis groups
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

## TASK: create work directory
@task(task_id="create_main_work_dir")
def create_main_work_dir() -> str:
    return "/path/work"

## TG1 TASK: prepare spaceranger scripts for each samples
@task(task_id="prepare_analysis_scripts")
def prepare_analysis_scripts(analysis_entry: dict) -> dict:
    sample_metadata = analysis_entry.get("sample_metadata")
    sample_id = list(sample_metadata.keys())
    analysis_metadata = analysis_entry.get("analysis_metadata")
    # if sample_id[0] == "IGF2":
    #     raise ValueError(f"I don't like {sample_id}")
    return {"sample_id": sample_id, "output": "/file/path"}

## TG1 TASK: run analysis script
@task(task_id="run_analysis")
def run_analysis(analysis_info: dict) -> dict:
    sample_metadata = analysis_info.get("sample_metadata")
    sample_id = analysis_info.get("sample_id")
    output = analysis_info.get("output")
    return {"sample_id": sample_id, "output": output}

## TG1 TASK: run squidpy qc step
@task(task_id="run_squidpy_qc")
def run_squidpy_qc(analysis_out: dict) -> dict:
    sample_id = analysis_out.get("sample_id")
    output = analysis_out.get("output")
    ## generate report and move it to visium output directory
    return {"sample_id": sample_id, "output": output}

## TG1 TASK: move analysis to 
@task(task_id="move_analysis")
def move_analysis(analysis_output: dict, work_dir: str) -> dict:
    sample_id = analysis_output.get("sample_id")
    output = analysis_output.get("output")
    final_output = f"{work_dir}/{sample_id}"
    return {"sample_id": sample_id, "output": final_output}

## TASK GROUP 1: run script per analysis groups
@task_group
def prepare_and_run_analysis_for_each_groups(analysis_entry: dict, work_dir: str) -> dict:
    analysis_info = prepare_analysis_scripts(analysis_entry=analysis_entry)
    analysis_output = run_analysis(analysis_info=analysis_info)
    squidpy_out = run_squidpy_qc(analysis_out=analysis_output)
    final_output = move_analysis(analysis_output=squidpy_out, work_dir=work_dir)
    return final_output

## TASK: collect all analysis outputs
@task(task_id="collect_analysis")
def collect_analysis(analysis_output_list: list) -> list:
    return analysis_output_list

## TASK: switch to aggr if morethan one samples are present
@task.branch(task_id="decide_aggr")
def decide_aggr(
    analysis_output_list: list,
    aggr_task: str = "prepare_aggr_script",
    non_aggr_task: str = "calculate_md5_for_work_dir") -> list:
    if len(analysis_output_list) > 1:
        return [aggr_task]
    elif len(analysis_output_list) == 1:
        return [non_aggr_task]

## TASK: prep aggr run script
@task(task_id="prepare_aggr_script")
def prepare_aggr_script(analysis_output_list: list) -> str:
    return "/path/ALL"

## TASK: run aggr
@task(task_id="run_aggr")
def run_aggr(output_dir: str) -> str:
    return output_dir

## TASK: run aggr QC
@task(task_id="squidpy_qc_for_aggr")
def squidpy_qc_for_aggr(output_dir: str) -> str:
    ## generate squidpy qc and move it to visium directory
    return output_dir

## TASK: move aggr to main work dir
@task(task_id="move_aggr")
def move_aggr(aggr_path: str, work_dir: str) -> str:
    return f"{work_dir}/ALL"

## TASK: calculate md5sum for main work dir
@task(task_id="calculate_md5_for_work_dir", trigger_rule="none_failed")
def calculate_md5_for_work_dir() -> str:
    return "/path/output"

## TASK: load results to disk and db
@task(task_id="load_analysis_to_db")
def load_analysis_to_db(output_dir: str) -> str:
    return "/path/loaded"

## TASK: copy results to globus
@task(task_id="copy_data_to_globus")
def copy_data_to_globus(output_dir: str) -> str:
    return "/path/globus"

## TASK: endm email
@task(task_id="send_email_to_user")
def send_email_to_user() -> str:
    return None


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
	max_active_runs=10,
    default_view='grid',
    orientation='TB',
    tags=["spatial", "spaceranger", "hpc"])
def spaceranger_visium_wrapper_dag():
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
    aggr_or_not = decide_aggr(analysis_output_list)
    prep_aggr = prepare_aggr_script(analysis_output_list)
    aggr_run_dir = run_aggr(prep_aggr)
    aggr_qc_dir = squidpy_qc_for_aggr(aggr_run_dir)
    aggr_moved = move_aggr(aggr_path=aggr_qc_dir, work_dir=work_dir)
    md5_out = calculate_md5_for_work_dir()
    aggr_or_not >> prep_aggr
    aggr_or_not >> md5_out
    aggr_moved >> md5_out
    loaded_data = load_analysis_to_db(md5_out)
    globus_data = copy_data_to_globus(loaded_data)
    send_email = send_email_to_user()
    globus_data >> send_email
    send_email >> failed_analysis
    send_email >> finished_analysis

spaceranger_visium_wrapper_dag()