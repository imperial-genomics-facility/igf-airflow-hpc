import os, pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task, task_group
from igf_airflow.utils.generic_airflow_tasks import (
	mark_analysis_running,
    fetch_analysis_design_from_db,
	send_email_to_user,
	copy_data_to_globus,
	mark_analysis_finished,
    create_main_work_dir,
	mark_analysis_failed)
from igf_airflow.utils.dag43_cosmx_export_and_qc_utils import (
    run_ftp_export_factory,
    prepare_run_ftp_export,
    run_ftp_export,
    prep_extract_ftp_export,
    extract_ftp_export,
    collect_extracted_data,
    copy_export_dir_to_globus,
    collect_all_slides,
    match_slide_ids_with_project_id,
    prep_validate_export_md5,
    validate_export_md5,
    generate_count_qc_report,
    generate_fov_qc_report,
    copy_slide_reports_to_globus,
    register_db_data,
    collect_slide_metadata,
    generate_additional_qc_report1,
    generate_additional_qc_report2,
    upload_reports_to_portal,
    collect_all_processed_slides
)


@task_group
def run_export_task_group(run_entry, work_dir):
    ## TASK
    downloaded_data = \
        prepare_run_ftp_export(
            run_entry=run_entry,
            work_dir=work_dir)
    ftp_export = \
        run_ftp_export(
            cosmx_ftp_export_name=downloaded_data["cosmx_ftp_export_name"])
    extracted_data = \
        prep_extract_ftp_export(
            run_entry=downloaded_data["run_entry"],
            export_finished=ftp_export)
    extract_tar = \
        extract_ftp_export(
            export_dir=extracted_data["export_dir"],
            work_dir=work_dir)
    validated_data = \
        prep_validate_export_md5(
            run_entry=extracted_data["run_entry"],
            extract_finished=extract_tar)
    md5_validate = \
        validate_export_md5(
            export_dir=validated_data["export_dir"])
    globus_copy = \
        copy_export_dir_to_globus(
            export_dir=validated_data["export_dir"])
    colleced_run_entry = \
        collect_extracted_data(
            run_entry=validated_data["run_entry"],
            validation_finished=md5_validate,
            globus_copy_finished=globus_copy)
    return colleced_run_entry


@task_group
def slide_qc_task_group(
    slide_entry,
    matched_slide_ids,
    design_file):
    ## TASK
    slide_meatadata = \
        collect_slide_metadata(
            slide_entry=slide_entry,
            matched_slide_ids=matched_slide_ids)
    count_qc = \
        generate_count_qc_report(
            slide_entry=slide_meatadata)
    fov_qc = \
        generate_fov_qc_report(
            slide_entry=count_qc)
    additional_qc_1 = \
        generate_additional_qc_report1(
            slide_entry=count_qc)
    additional_qc_2 = \
        generate_additional_qc_report2(
            slide_entry=count_qc)
    db_entry = \
        register_db_data(
            slide_entry=count_qc,
            design_file=design_file)
    fov_qc >> db_entry
    additional_qc_1 >> db_entry
    additional_qc_2 >> db_entry
    globus_report_data = \
        copy_slide_reports_to_globus(
            slide_entry=db_entry)
    uploaded_reports = \
        upload_reports_to_portal(
            slide_entry=globus_report_data)
    return uploaded_reports


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
    tags=["spatial", "cosmx", "hpc"])
def dag43_cosmx_export_and_qc():
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
    design_file = \
        fetch_analysis_design_from_db()
    ## TASK
    work_dir = \
        create_main_work_dir(
            task_tag='cosmx_export_and_qc')
    ## PIPELINE
    running_analysis >> \
        Label('Analysis Design found') >> \
            design_file
    running_analysis >> \
        Label('Analysis Design not found') >> \
            failed_analysis
    design_file >> work_dir
    ## TO DO TASK
    design_data = \
        run_ftp_export_factory(
            design_file=design_file["analysis_design"],
            work_dir=work_dir)
    ## TO DO TASK GROUP EXPAND
    downloaded_data = \
        run_export_task_group.\
		    partial(work_dir=work_dir).\
		    expand(run_entry=design_data)
    ## TASK
    all_slides = \
        collect_all_slides(
            run_entry_list=downloaded_data)
    ## TASK
    matched_slides = \
        match_slide_ids_with_project_id(
            slide_data_list=all_slides)
    ## TASK GROUP
    all_processed_slides = \
        slide_qc_task_group.\
            partial(
                matched_slide_ids=matched_slides,
                design_file=design_file).\
            expand(slide_entry=all_slides)
    ## TASK
    all_processed_slides_list = \
        collect_all_processed_slides(
            slide_entry_list=all_processed_slides)
    ## TASK
    send_email = \
        send_email_to_user()
    all_processed_slides_list >> send_email
    send_email >> failed_analysis
    send_email >> finished_analysis

dag43_cosmx_export_and_qc()