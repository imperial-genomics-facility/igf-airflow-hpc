import os, pendulum
from airflow.decorators import dag, task_group
from airflow.utils.edgemodifier import Label
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
    prepare_cellranger_script,
    run_cellranger_script,
    run_single_sample_scanpy,
    collect_and_branch,
    configure_cellranger_aggr_run,
    run_cellranger_aggr_script,
    merged_scanpy_report,
    move_single_sample_result_to_main_work_dir,
    move_aggr_result_to_main_work_dir,
    calculate_md5sum_for_main_work_dir,
    load_cellranger_results_to_db)

# ## TASK GROUP
@task_group
def multiple_sample_task_group(
    main_work_dir: str,
    sample_group: str,
    sample_group_info: dict) -> None:
    run_info = \
        prepare_cellranger_script(
            sample_group=sample_group,
            design_dict=sample_group_info)
    cellranger_output_dir = \
        run_cellranger_script(run_info)
    scanpy_output_dict = \
        run_single_sample_scanpy(
            design_dict=sample_group_info,
            sample_group=sample_group,
            cellranger_output_dir=cellranger_output_dir)
    per_sample_info = \
        move_single_sample_result_to_main_work_dir(
            main_work_dir=main_work_dir,
            scanpy_output_dict=scanpy_output_dict)
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
    tags=["scrna", "cellranger", "hpc"])
def cellranger_wrapper_dag():
    analysis_running = \
        mark_analysis_running(
            next_task="fetch_analysis_design",
            last_task="no_work")
    sample_group_info = \
        fetch_analysis_design_from_db()
    main_work_dir = \
        create_main_work_dir()
    analysis_running >> sample_group_info
    analysis_running >> no_work()
    sample_groups = \
        get_analysis_group_list(
            design_dict=sample_group_info)
    grp = \
        multiple_sample_task_group.\
            partial(
                main_work_dir=main_work_dir,
                sample_group_info=sample_group_info).\
            expand(sample_group=sample_groups)
    aggr_script_dict = \
        configure_cellranger_aggr_run()
    aggr_branch = \
        collect_and_branch()
    grp >> aggr_branch
    aggr_branch >> Label('Multiple_samples') >> aggr_script_dict
    aggr_output_dict = \
        run_cellranger_aggr_script(
           script_dict=aggr_script_dict)
    scanpy_aggr_output_dict = \
        merged_scanpy_report(
            design_dict=sample_group_info,
            cellranger_aggr_output_dict=aggr_output_dict)
    final_work_dir = \
        move_aggr_result_to_main_work_dir(
            main_work_dir=main_work_dir,
            scanpy_aggr_output_dict=scanpy_aggr_output_dict)
    md5_file = \
        calculate_md5sum_for_main_work_dir(
            main_work_dir=final_work_dir)
    aggr_branch >> Label('Single_sample') >> md5_file
    loaded_files_info = \
        load_cellranger_results_to_db(
            main_work_dir=final_work_dir,
            md5_file=md5_file)
    copy_globus = \
		copy_data_to_globus(loaded_files_info)
    copy_globus >> send_email_to_user() >> mark_analysis_finished() >> mark_analysis_failed()

##  DAG
cellranger_wrapper_dag()