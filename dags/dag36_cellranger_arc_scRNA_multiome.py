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
	mark_analysis_failed,
    collect_all_analysis)
from igf_airflow.utils.dag34_cellranger_multi_scRNA_utils import (
    get_analysis_group_list,
    run_cellranger_script,
    decide_aggr,
    run_cellranger_aggr_script,
    move_single_sample_result_to_main_work_dir,
    move_aggr_result_to_main_work_dir)
from igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils import (
    prepare_cellranger_arc_script,
    configure_cellranger_arc_aggr_run,
    run_single_sample_scanpy_for_arc,
    merged_scanpy_report_for_arc)

## TASK GROUP
@task_group
def prepare_and_run_analysis_for_each_groups(
        analysis_entry: dict,
        work_dir: str) -> dict:
    analysis_script_info = \
        prepare_cellranger_arc_script(
            analysis_entry=analysis_entry)
    analysis_output = \
        run_cellranger_script(
            analysis_script_info=analysis_script_info)
    scanpy_out = \
        run_single_sample_scanpy_for_arc(
            analysis_output=analysis_output)
    final_output = \
        move_single_sample_result_to_main_work_dir(
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
    tags=["scrna", "cellranger-arc", "hpc"])
def cellranger_arc_wrapper_dag():
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
            task_tag='cellranger_arc_output')
    ## PIPELINE
    design >> work_dir
    ## TASK
    analysis_groups = \
        get_analysis_group_list(
            design_dict=design,
            required_tag_name="library_types",
            required_tag_value="Gene Expression")
    ## TASK GROUP
    analysis_outputs = \
        prepare_and_run_analysis_for_each_groups.\
        partial(work_dir=work_dir).\
        expand(analysis_entry=analysis_groups)
    ## TASK
    analysis_output_list = \
      collect_all_analysis(
          analysis_outputs)
    ## TASK
    aggr_or_not = \
        decide_aggr(
            analysis_output_list,
            aggr_task="configure_cellranger_arc_aggr_run",
            non_aggr_task="calculate_md5_for_work_dir")
    ## TASK
    aggr_script_info = \
        configure_cellranger_arc_aggr_run(
            analysis_output_list,
            design)
    ## TASK
    aggr_run_dir = \
        run_cellranger_aggr_script(
            aggr_script_info)
    ## TASK
    aggr_qc_dir = \
        merged_scanpy_report_for_arc(
            aggr_run_dir,
            design)
    ## TASK
    aggr_moved = \
        move_aggr_result_to_main_work_dir(
            work_dir=work_dir,
            analysis_output_dir=aggr_qc_dir)
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

cellranger_arc_wrapper_dag()