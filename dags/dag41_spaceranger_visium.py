import os
import pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task_group
from igf_airflow.utils.generic_airflow_tasks import (
	mark_analysis_running,
    fetch_analysis_design_from_db,
	send_email_to_user,
	copy_data_to_globus,
	mark_analysis_finished,
    create_main_work_dir,
    calculate_md5sum_for_main_work_dir,
    load_analysis_results_to_db,
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
    move_spaceranger_aggr_to_main_work_dir)


## TASK GROUP
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
            design_dict=design)
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