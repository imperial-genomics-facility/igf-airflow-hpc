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
    run_ftp_export,
    extract_ftp_export,
    collect_all_slides,
    validate_export_md5,
    generate_count_qc_report,
    generate_fov_qc_report,
    generate_db_data,
    copy_slide_data_to_globus,
    register_db_data,
    collect_qc_reports_and_upload_to_portal
)

# @task(multiple_outputs=False)
# def run_ftp_export_factory(design_file, work_dir):
# 	design_data = [
# 		{'cosmx_run_id': 'A', 'export_directory_path': 'A_f'},
# 		{'cosmx_run_id': 'B', 'export_directory_path': 'B_f'}]
# 	return design_data

# @task(multiple_outputs=False)
# def run_ftp_export(run_entry, work_dir):
# 	run_entry.update({'download_path': work_dir + '/' + run_entry['export_directory_path'] + '/download'})
# 	return run_entry

# @task(multiple_outputs=False)
# def extract_ftp_export(run_entry):
#     run_entry.update({'extract': '/path'})
#     return run_entry

# @task(multiple_outputs=False)
# def collect_all_slides(run_entry):
#     slide_data = [
# 		{'run_name': 'A', 'slide_id': 'slide1'},
# 		{'run_name': 'A', 'slide_id': 'slide2'},
# 		{'run_name': 'B', 'slide_id': 'slide3'},
# 		{'run_name': 'B', 'slide_id': 'slide4'}]
#     return slide_data

# @task(multiple_outputs=False)
# def validate_export_md5(run_entry):
#     run_entry.update({'validation': True})
#     return run_entry

# @task(multiple_outputs=False)
# def generate_count_qc_report(run_entry):
#     run_entry.update({'count_qc_json': 'json', 'count_qc_html': 'html'})
#     return run_entry

# @task(multiple_outputs=False)
# def generate_fov_qc_report(run_entry):
#     run_entry.update({'fov_qc_json': 'json', 'fov_qc_html': 'html'})
#     return run_entry

# @task(multiple_outputs=False)
# def generate_db_data(qc_list):
#     run_entry = dict()
#     for qc in qc_list:
#         run_entry.update(**qc)
#     run_entry.update({'db_data': '/DB'})
#     return run_entry

# @task(multiple_outputs=False)
# def copy_slide_data_to_globus(run_entry):
#     run_entry.update({'Globus': '/globus_path'})
#     return run_entry

# @task(multiple_outputs=False)
# def register_db_data(run_entry):
#     run_entry.update({'db': True})
#     return run_entry

# @task(multiple_outputs=False)
# def collect_qc_reports_and_upload_to_portal(run_entrys):
#     print([r for r in run_entrys])
#     return True

@task_group
def run_export_task_group(run_entry, work_dir):
    downloaded_data = run_ftp_export(run_entry, work_dir)
    extracted_data = extract_ftp_export(run_entry=downloaded_data)
    return extracted_data

@task_group
def slide_qc_task_group(run_entry):
    validated_data = validate_export_md5(run_entry)
    count_qc = generate_count_qc_report(validated_data)
    fov_qc = generate_fov_qc_report(validated_data)
    ## add more here
    db_entry = generate_db_data(qc_list = [count_qc, fov_qc])
    registered_data = register_db_data(db_entry)
    globus_data = copy_slide_data_to_globus(registered_data)
    return globus_data

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
            task_tag='cosmx_exportand_qc')
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
        run_ftp_export_factory(design_file, work_dir)
    ## TO DO TASK GROUP EXPAND
    downloaded_data = \
        run_export_task_group.\
		    partial(work_dir=work_dir).\
		    expand(run_entry=design_data)
    ## TO DO TASK
    all_slides = \
        collect_all_slides(run_entry=downloaded_data)
    ## TO DO TASK GROUP EXPAND
    slide_qc_reports = \
        slide_qc_task_group.\
            expand(run_entry=all_slides)
    ## TO DO TASK
    collect_qc = \
        collect_qc_reports_and_upload_to_portal(slide_qc_reports)
    ## TASK
    send_email = \
        send_email_to_user()
    collect_qc >> send_email
    send_email >> failed_analysis
    send_email >> finished_analysis

dag43_cosmx_export_and_qc()