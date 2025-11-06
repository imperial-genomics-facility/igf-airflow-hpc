import os
import pendulum
from airflow.decorators import dag
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
from igf_airflow.utils.dag46_scRNA_10X_flex_utils import (
    prepare_cellranger_flex_script)
from igf_airflow.utils.dag34_cellranger_multi_scRNA_utils import (
    run_cellranger_script,
    run_single_sample_scanpy)

## DAG
DAG_ID = (
    os.path.basename(__file__)
    .replace(".pyc", "")
    .replace(".py", "")
)


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=1,
    default_view='grid',
    orientation='TB',
    tags=["scRNA", "analysis", "10XGenomics", "flex"])
def dag46_scRNA_10X_flex():
    ## TASK
    running_analysis = \
        mark_analysis_running(
            next_task="fetch_analysis_design",
            last_task="mark_analysis_failed")
    ## TASK
    design = \
        fetch_analysis_design_from_db()
    ## TASK
    work_dir = \
        create_main_work_dir(
            task_tag='scRNA_flex_output')
    ## TASK - Configure Flex Pipeline
    analysis_script_info = \
        prepare_cellranger_flex_script(
            design_dict=design,
            work_dir=work_dir)
    ## TASK - Execute Flex Pipeline
    cellranger_output_dir = \
        run_cellranger_script(
            script_dict=analysis_script_info)
    ## TASK - Generate Scanpy QC for all samples
    scanpy_out = \
        run_single_sample_scanpy(
            sample_group=analysis_script_info["sample_group"],
            cellranger_output_dir=cellranger_output_dir,
            design_dict=design)
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
    ## TASK
    finished_analysis = \
        mark_analysis_finished()
    ## TASK
    failed_analysis = \
        mark_analysis_failed()
    ## PIPELINE
    running_analysis >> design
    globus_data >> send_email
    send_email >> finished_analysis
    send_email >> failed_analysis


dag46_scRNA_10X_flex()