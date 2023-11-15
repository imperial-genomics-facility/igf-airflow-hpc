import json, time, os
from yaml import SafeLoader, load
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow import XComArg
import pandas as pd

design="""sample_metadata:
  IGF1:
    feature_types: Gene Expression
    cellranger_group: 1
  IGF2:
    feature_types: VDJ
    cellranger_group: 1
  IGF3:
    feature_types: Multiplexing Capture
    cellranger_group: 2
  IGF4:
    feature_types: Gene Expression
    cellranger_group: 2
  IGF5:
    feature_types: Gene Expression
    cellranger_group: 3
  IGF6:
    feature_types: VDJ
    cellranger_group: 3
  IGF7:
    feature_types: Gene Expression
    cellranger_group: 4
  IGF8:
    feature_types: Gene Expression
    cellranger_group: 5
  IGF9:
    feature_types: Gene Expression
    cellranger_group: 6
analysis_metadata:
  cellranger_multi_config:
    - [gene-expression]
    - reference,/REF
    - r1-length,28
    - r2-length,90
    - chemistry,auto
    - expect-cells,60000
    - force-cells,6000
    - include-introns,true
    - no-secondary,false
    - no-bam,false
    - check-library-compatibility,true
    - min-assignment-confidence,0.9
    - cmo-set,/path/custom/cmo.csv
    - [vdj]
    - reference,/path
    - r1-length,28
    - r2-length,90
    - [samples]
    - sample_id,cmo_ids
    - IGF3,CMO3"""

## TASK
@task(task_id="mark_analysis_running", queue='generic')
def mark_analysis_running() -> None:
	pass

## TASKS
@task(task_id="fetch_analysis_design", queue='generic')
def fetch_analysis_design_from_db() -> list:
	try:
		json_data = load(design, Loader=SafeLoader)
		sample_metadata = json_data.get("sample_metadata")
		analysis_metadata = json_data.get("analysis_metadata")
		return {"sample_metadata": sample_metadata, "analysis_metadata": analysis_metadata}
	except Exception as e:
		raise ValueError(e)

## TASK
@task(task_id="get_analysis_group_list", queue='generic')
def get_analysis_group_list(sample_group_info: dict) -> list:
  try:
    sample_metadata = sample_group_info["sample_metadata"]
    unique_sample_groups = set()
    for _, group in sample_metadata.items():
      unique_sample_groups.add(group['cellranger_group'])
    return list(unique_sample_groups)
  except Exception as e:
    raise

## TASK
@task(task_id="prepare_cellranger_script", queue='generic')
def prepare_cellranger_script(
      sample_group: str,
      sample_group_info: dict) -> str:
  try:
    sample_metadata = sample_group_info["sample_metadata"]
    analysis_metadata = sample_group_info["analysis_metadata"]
    sample_group_list = list()
    for sample_id, group in sample_metadata.items():
      cellranger_group = group['cellranger_group']
      if cellranger_group == sample_group:
        sample_group_list.append([
          sample_id,
          group['feature_types']])
    cellranger_multi_config = analysis_metadata["cellranger_multi_config"]
    group_level_data = list()
    group_level_data.extend(cellranger_multi_config)
    group_level_data.extend(sample_group_list)
    print(group_level_data)
    return {"run_script": f'run_script_{sample_group}.sh', "run_dir": f'run_dir_{sample_group}'}
  except Exception as e:
    raise ValueError(e)

## TASK
@task(task_id="run_cellranger_script", queue='generic')
def run_cellranger_script(run_info: dict) -> str:
  try:
    run_dir = run_info['run_dir']
    run_script = run_info['run_script']
    print(run_script)
    return {"cellranger_output": run_dir}
  except Exception as e:
    raise 

## TASK
@task(task_id="run_single_sample_scanpy", queue='generic')
def run_single_sample_scanpy(sample_group: str, cellranger_run_info: dict) -> str:
  try:
    cellranger_output = cellranger_run_info['cellranger_output']
    print(cellranger_output, sample_group)
    return {"sample_group": sample_group, "cellranger_output": cellranger_output, "scanpy_h5ad": f"{sample_group}.h5ad", "html_report": f"{sample_group}.html"}
  except Exception as e:
    raise 

## TASK
@task(task_id="merge_all_cellranger_output", queue='generic')
def merge_all_cellranger_output()-> list:
  context = get_current_context()
  ti = context.get('ti')
  all_lazy_task_ids = \
    context['task'].\
    get_direct_relative_ids(upstream=True)
  lazy_xcom = ti.xcom_pull(task_ids=all_lazy_task_ids)
  return lazy_xcom

## TASK
@task(task_id="merge_scanpy_report", queue='generic')
def merge_scanpy_report(analysis_groups)-> str:
  cellranger_output_dict = dict()
  for grp in analysis_groups:
    sample_group = grp['sample_group']
    cellranger_output = grp['cellranger_output']
    cellranger_output_dict.update({sample_group: cellranger_output})
  return cellranger_output_dict

## TASK GROUP
@task_group
def multiple_sample_task_group(
    sample_group: str,
    sample_group_info: dict) -> None:
  run_info = \
    prepare_cellranger_script(
      sample_group=sample_group,
      sample_group_info=sample_group_info)
  cellranger_run_info = run_cellranger_script(run_info)
  scanpy_run_info = \
    run_single_sample_scanpy(
      sample_group=sample_group,
      cellranger_run_info=cellranger_run_info)
## DAG
@dag(
    dag_id="cellranger_dag_combined",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def cellranger_dag():
  sample_group_info = fetch_analysis_design_from_db()
  mark_analysis_running() >> sample_group_info
  sample_groups = get_analysis_group_list(sample_group_info)
  grp = \
    multiple_sample_task_group.\
      partial(sample_group_info=sample_group_info).\
      expand(sample_group=sample_groups)
  analysis_groups = merge_all_cellranger_output()
  grp >> analysis_groups
  cellranger_output_dict = merge_scanpy_report(analysis_groups)
cellranger_dag()