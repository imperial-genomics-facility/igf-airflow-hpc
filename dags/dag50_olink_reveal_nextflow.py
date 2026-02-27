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
    mark_analysis_failed
)
from igf_airflow.utils.dag50_olink_reveal_nextflow_utils import (
    prepare_olink_nextflow_script,
    run_olink_nextflow_script
)


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
    max_active_runs=10,
    default_view='grid',
    orientation='TB',
    tags=["olink", "reveal", "analysis"])
def dag50_olink_reveal_nextflow():
    ## TASK
    running_analysis = mark_analysis_running(
        next_task="fetch_analysis_design",
        last_task="mark_analysis_failed"
    )
    ## TASK
    design = fetch_analysis_design_from_db()
    ## TASK
    work_dir = create_main_work_dir(
        task_tag='olink_reveal_output'
    )
    ## TASK - Configure olink Pipeline
    analysis_script = prepare_olink_nextflow_script(
        design_dict=design,
        work_dir=work_dir
    )
    ## TASK - Execute Olink Pipeline
    olink_exe = run_olink_nextflow_script(
        script_dict=analysis_script
    )
    ## TASK
    work_dir_with_md5 = calculate_md5sum_for_main_work_dir(
        work_dir
    )
    ## TASK
    loaded_data = load_analysis_results_to_db(
        work_dir_with_md5
    )
    ## TASK
    globus_data = copy_data_to_globus(
        loaded_data
    )
    ## TASK
    send_email = send_email_to_user()
    ## TASK
    finished_analysis = mark_analysis_finished()
    ## TASK
    failed_analysis = mark_analysis_failed()
    ## PIPELINE
    running_analysis >> design
    olink_exe >> work_dir_with_md5
    globus_data >> send_email
    send_email >> finished_analysis
    send_email >> failed_analysis


dag50_olink_reveal_nextflow()