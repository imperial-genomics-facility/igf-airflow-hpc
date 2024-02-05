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
    run_cellranger_script,
    collect_and_branch,
    run_cellranger_aggr_script,
    merged_scanpy_report,
    move_single_sample_result_to_main_work_dir,
    move_aggr_result_to_main_work_dir,
    calculate_md5sum_for_main_work_dir,
    load_cellranger_results_to_db)
from igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils import (
    prepare_cellranger_arc_script,
    configure_cellranger_arc_aggr_run,
    run_single_sample_scanpy_for_arc)

# ## TASK GROUP
@task_group
def multiple_sample_task_group(
    main_work_dir: str,
    sample_group: str,
    sample_group_info: dict) -> None:
    run_info = \
        prepare_cellranger_arc_script(
            sample_group=sample_group,
            design_dict=sample_group_info)
    cellranger_output_dir = \
        run_cellranger_script(run_info)
    scanpy_output_dict = \
        run_single_sample_scanpy_for_arc(
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
    tags=["scrna", "cellranger-arc", "hpc"])
def cellranger_arc_wrapper_dag():
    analysis_running = \
        mark_analysis_running(
            next_task="fetch_analysis_design",
            last_task="no_work")
    sample_group_info = \
        fetch_analysis_design_from_db()
    main_work_dir = \
        create_main_work_dir()
    analysis_running >> Label('Analysis Design found') >> sample_group_info
    analysis_running >> Label('Analysis Design not found') >> no_work()
    sample_group_info >> main_work_dir
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
        configure_cellranger_arc_aggr_run(
            design_dict=sample_group_info)
    aggr_branch = \
        collect_and_branch()
    grp >> aggr_branch
    aggr_branch >> Label('Multiple samples') >> aggr_script_dict
    aggr_output_dir = \
        run_cellranger_aggr_script(
           script_dict=aggr_script_dict)
    scanpy_aggr_output_dict = \
        merged_scanpy_report(
            design_dict=sample_group_info,
            cellranger_aggr_output_dir=aggr_output_dir)
    final_work_dir = \
        move_aggr_result_to_main_work_dir(
            main_work_dir=main_work_dir,
            scanpy_aggr_output_dict=scanpy_aggr_output_dict)
    md5_file = \
        calculate_md5sum_for_main_work_dir(
            main_work_dir=final_work_dir)
    loaded_files_info = \
        load_cellranger_results_to_db(
            main_work_dir=final_work_dir,
            md5_file=md5_file)
    copy_globus = \
		copy_data_to_globus(loaded_files_info)
    ## single sample branch
    md5_file_single = \
        calculate_md5sum_for_main_work_dir(
            main_work_dir=main_work_dir)
    aggr_branch >> Label('Single sample') >> md5_file_single
    loaded_files_info_single = \
        load_cellranger_results_to_db(
            main_work_dir=main_work_dir,
            md5_file=md5_file_single)
    copy_globus_single = \
		copy_data_to_globus(loaded_files_info_single)
    ## merge branch
    send_mail = send_email_to_user()
    copy_globus >> send_mail
    copy_globus_single >> send_mail
    send_mail >> mark_analysis_finished() >> mark_analysis_failed()

##  DAG
cellranger_arc_wrapper_dag()