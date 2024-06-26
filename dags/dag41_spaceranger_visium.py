import json, time, os
import pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow import XComArg
from yaml import load, SafeLoader
from igf_airflow.utils.generic_airflow_tasks import (
	mark_analysis_running,
    fetch_analysis_design_from_db,
	no_work,
	send_email_to_user,
	copy_data_to_globus,
	mark_analysis_finished,
    create_main_work_dir,
    calculate_md5sum_for_main_work_dir,
	mark_analysis_failed)
from igf_airflow.utils.dag41_spaceranger_visium_utils import (
    get_spaceranger_analysis_group_list,
    prepare_spaceranger_count_run_dir_and_script_file,
    run_spaceranger_count_script,
    run_squidpy_qc,
    move_single_spaceranger_count_to_main_work_dir,
    collect_spaceranger_count_analysis,
    decide_aggr,
    prepare_spaceranger_aggr_script,
    run_spaceranger_aggr_script,
    squidpy_qc_for_aggr,
    move_spaceranger_aggr_to_main_work_dir,
    load_analysis_results_to_db

)
## TASK: mark analysis as running
# @task.branch(task_id="mark_analysis_running")
# def mark_analysis_running() -> list:
#     return ["fetch_analysis_design"]

## TASK: mark analysis as finished
# @task(task_id="mark_analysis_finished")
# def mark_analysis_finished() -> None:
#     pass

## TASK: mark analysis as failed
# @task(task_id="mark_analysis_failed", trigger_rule="all_failed")
# def mark_analysis_failed() -> None:
#     pass

# TASK: fetch design from db
# @task(task_id="fetch_analysis_design")
# def fetch_analysis_design() -> str:
#     design = """sample_metadata:
#   IGF1:
#     image: null
#     darkimage: null
#     colorizedimage: null
#     cytaimage: null
#     slide: null
#     area: null
#     dapi-index: null
#   IGF2:
#     image: null
#     darkimage: null
#     colorizedimage: null
#     cytaimage: null
#     slide: null
#     area: null
#     dapi-index: null
# analysis_metadata:
#   spaceranger_config:
#     - "--transcriptome=/path"
#     - "--probe-set=/path"
#     - "--filter-probes=true"
#     - "--reorient-images=true"
#     - "--create-bam=true"
#   spaceranger_aggr_config:
#     - "--normalize=mapped"
#     """
#     return design


## TASK: create work directory
# @task(task_id="create_main_work_dir")
# def create_main_work_dir() -> str:
#     return "/path/work"

## TG1 TASK: prepare spaceranger scripts for each samples
# @task(task_id="prepare_analysis_scripts")
# def prepare_analysis_scripts(analysis_entry: dict) -> dict:
#     sample_metadata = analysis_entry.get("sample_metadata")
#     sample_id = list(sample_metadata.keys())
#     analysis_metadata = analysis_entry.get("analysis_metadata")
#     # if sample_id[0] == "IGF2":
#     #     raise ValueError(f"I don't like {sample_id}")
#     return {"sample_id": sample_id, "output": "/file/path"}

## TG1 TASK: run analysis script
# @task(task_id="run_analysis")
# def run_analysis(analysis_info: dict) -> dict:
#     sample_metadata = analysis_info.get("sample_metadata")
#     sample_id = analysis_info.get("sample_id")
#     output = analysis_info.get("output")
#     return {"sample_id": sample_id, "output": output}

## TG1 TASK: run squidpy qc step
# @task(task_id="run_squidpy_qc")
# def run_squidpy_qc(analysis_out: dict) -> dict:
#     sample_id = analysis_out.get("sample_id")
#     output = analysis_out.get("output")
#     ## generate report and move it to visium output directory
#     return {"sample_id": sample_id, "output": output}

## TG1 TASK: move analysis to 
# @task(task_id="move_analysis")
# def move_analysis(analysis_output: dict, work_dir: str) -> dict:
#     sample_id = analysis_output.get("sample_id")
#     output = analysis_output.get("output")
#     final_output = f"{work_dir}/{sample_id}"
#     return {"sample_id": sample_id, "output": final_output}

## TASK GROUP 1: run script per analysis groups
@task_group
def prepare_and_run_analysis_for_each_groups(
        analysis_entry: dict,
        work_dir: str) -> dict:
    analysis_script_info = \
        prepare_spaceranger_count_run_dir_and_script_file(
            analysis_entry=analysis_entry)
    analysis_output = \
        run_spaceranger_count_script(
            analysis_script_info=analysis_script_info)
    squidpy_out = \
        run_squidpy_qc(
            analysis_output=analysis_output)
    final_output = \
        move_single_spaceranger_count_to_main_work_dir(
            analysis_output=squidpy_out,
            work_dir=work_dir)
    return final_output

# ## TASK: collect all analysis outputs
# @task(task_id="collect_analysis")
# def collect_analysis(analysis_output_list: list) -> list:
#     return analysis_output_list

# ## TASK: switch to aggr if morethan one samples are present
# @task.branch(task_id="decide_aggr")
# def decide_aggr(
#     analysis_output_list: list,
#     aggr_task: str = "prepare_aggr_script",
#     non_aggr_task: str = "calculate_md5_for_work_dir") -> list:
#     if len(analysis_output_list) > 1:
#         return [aggr_task]
#     elif len(analysis_output_list) == 1:
#         return [non_aggr_task]

# ## TASK: prep aggr run script
# @task(task_id="prepare_aggr_script")
# def prepare_aggr_script(analysis_output_list: list) -> str:
#     return "/path/ALL"

## TASK: run aggr
# @task(task_id="run_aggr")
# def run_aggr(output_dir: str) -> str:
#     return output_dir

## TASK: run aggr QC
# @task(task_id="squidpy_qc_for_aggr")
# def squidpy_qc_for_aggr(output_dir: str) -> str:
#     ## generate squidpy qc and move it to visium directory
#     return output_dir

## TASK: move aggr to main work dir
# @task(task_id="move_aggr")
# def move_aggr(aggr_path: str, work_dir: str) -> str:
#     return f"{work_dir}/ALL"

## TASK: calculate md5sum for main work dir
# @task(task_id="calculate_md5_for_work_dir", trigger_rule="none_failed")
# def calculate_md5_for_work_dir() -> str:
#     return "/path/output"

## TASK: load results to disk and db
@task(task_id="load_analysis_to_db")
def load_analysis_to_db(output_dir: str) -> str:
    return "/path/loaded"

## TASK: copy results to globus
@task(task_id="copy_data_to_globus")
def copy_data_to_globus(output_dir: str) -> str:
    return "/path/globus"

## TASK: send email
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
    ## TASK
    running_analysis = \
        mark_analysis_running(
            next_task="fetch_analysis_design",
            last_task="no_work")
    ## TASK
    finished_analysis = \
        mark_analysis_finished()
    ## TASK
    failed_analysis = \
        mark_analysis_failed()
    ## TASK
    design = \
        fetch_analysis_design_from_db()
    ## PIPELINE
    running_analysis >> \
        Label('Analysis Design found') >> \
            design
    running_analysis >> \
        Label('Analysis Design not found') >> \
            failed_analysis
    ## TASK
    work_dir = \
        create_main_work_dir(
            task_tag='spaceranger_output')
    ## PIPELINE
    design >> work_dir
    running_analysis >> design
    ## TASK
    analysis_groups = \
        get_spaceranger_analysis_group_list(
            design=design)
    ## TASK GROUP
    analysis_outputs = \
        prepare_and_run_analysis_for_each_groups.\
        partial(work_dir=work_dir).\
        expand(analysis_entry=analysis_groups)
    ## TASK
    analysis_output_list = \
      collect_spaceranger_count_analysis(
          analysis_outputs)
    ## TASK
    aggr_or_not = \
        decide_aggr(analysis_output_list)
    ## TASK
    aggr_script_info = \
        prepare_spaceranger_aggr_script(
            analysis_output_list)
    ## TASK
    aggr_run_dir = \
        run_spaceranger_aggr_script(
            aggr_script_info)
    ## TASK
    aggr_qc_dir = \
        squidpy_qc_for_aggr(
            aggr_run_dir)
    ## TASK
    aggr_moved = \
        move_spaceranger_aggr_to_main_work_dir(
            aggr_path=aggr_qc_dir,
            work_dir=work_dir)
    ## TASK
    work_dir_with_md5 = \
        calculate_md5sum_for_main_work_dir(
            work_dir)
    ## PIPELINE
    aggr_or_not >> aggr_script_info
    aggr_or_not >> work_dir_with_md5
    aggr_moved >> work_dir_with_md5
    ## TASK
    loaded_data_info = \
        load_analysis_results_to_db(
            work_dir_with_md5)
    ## TASK
    globus_data = \
        copy_data_to_globus(
            loaded_data_info)
    ## TASK
    send_email = \
        send_email_to_user()
    ## PIPELINE
    globus_data >> send_email
    send_email >> failed_analysis
    send_email >> finished_analysis

spaceranger_visium_wrapper_dag()