import json, time, os
import pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task, task_group
from igf_airflow.utils.generic_airflow_tasks import (
	mark_analysis_running,
    fetch_analysis_design_from_db,
	send_email_to_user,
	copy_data_to_globus,
	mark_analysis_finished,
    create_main_work_dir,
    calculate_md5sum_for_main_work_dir,
    load_analysis_results_to_db,
	mark_analysis_failed,
    collect_all_analysis,
    move_per_sample_analysis_to_main_work_dir)
from igf_airflow.utils.dag42_curioseq_wrapper_utils import (
    merge_fastqs_for_analysis,
    prepare_curioseeker_analysis_scripts,
    run_curioseeker_nf_script,
    get_curioseeker_analysis_group_list,
    run_scanpy_qc,
    run_scanpy_qc_for_all_samples)


## TASK GROUP: run script per analysis groups
@task_group
def prepare_and_run_analysis_for_each_groups(
        analysis_entry: dict,
        work_dir: str) -> dict:
    ## TASK
    merged_fastqs = \
        merge_fastqs_for_analysis(
            analysis_entry)
    ## TASK
    analysis_script_info = \
        prepare_curioseeker_analysis_scripts(
            analysis_entry=analysis_entry,
            modified_sample_metadata=merged_fastqs)
    ## TASK
    analysis_output = \
        run_curioseeker_nf_script(
            analysis_script_info=analysis_script_info)
    ## TASK
    scanpy_out = \
        run_scanpy_qc(\
            analysis_out=analysis_output)
    ## TASK
    final_output = \
        move_per_sample_analysis_to_main_work_dir(
            analysis_output=scanpy_out,
            work_dir=work_dir)
    return final_output

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
@dag(
    dag_id=DAG_ID,
	schedule=None,
	start_date=pendulum.yesterday(),
	catchup=True,
	max_active_runs=10,
    default_view='grid',
    orientation='TB',
    tags=["spatial", "curioseeker", "hpc"])
def dag42_curioseq_wrapper():
    ## TASK
    running_analysis = \
        mark_analysis_running(
            next_task="fetch_analysis_design",
            last_task="mark_analysis_failed")
    ## TASK
    finished_analysis = \
        mark_analysis_finished()
    ## TASK
    failed_analysis = \
        mark_analysis_failed()
    ## PIPELINE
    running_analysis >> finished_analysis
    ## TASK
    design = \
        fetch_analysis_design_from_db()
    ## TASK
    work_dir = \
        create_main_work_dir(
            task_tag='curioseeker_output')
    ## PIPELINE
    running_analysis >> \
        Label('Analysis Design found') >> \
            design
    running_analysis >> \
        Label('Analysis Design not found') >> \
            failed_analysis
    design >> work_dir
    ## TASK
    analysis_groups = \
        get_curioseeker_analysis_group_list(
            design_dict=design)
    ## TASK GROUP EXPAND
    analysis_outputs = \
        prepare_and_run_analysis_for_each_groups.\
        partial(work_dir=work_dir).\
        expand(analysis_entry=analysis_groups)
    ## TASK
    analysis_output_list = \
      collect_all_analysis(
          analysis_outputs)
    ## TASK
    agg_report = \
        run_scanpy_qc_for_all_samples(
            analysis_output_list)
    ## TASK
    work_dir_with_md5 = \
        calculate_md5sum_for_main_work_dir(
            work_dir)
    ## TASK
    loaded_data = \
        load_analysis_results_to_db(
            work_dir_with_md5)
    ## TASK
    globus_data = \
        copy_data_to_globus(
            loaded_data)
    ## TASK
    send_email = \
        send_email_to_user()
    ## PIPELINE
    analysis_output_list >> work_dir_with_md5
    agg_report >> work_dir_with_md5
    globus_data >> send_email
    send_email >> failed_analysis
    send_email >> finished_analysis

## call dag
dag42_curioseq_wrapper()