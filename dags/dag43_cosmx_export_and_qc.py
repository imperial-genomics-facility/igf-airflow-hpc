import os, pendulum
from airflow.utils.edgemodifier import Label
from airflow.decorators import dag, task, task_group


@task(multiple_outputs=False)
def mark_analysis_running():
	return {'status':'running'}

@task(multiple_outputs=False)
def mark_analysis_finished():
	return {'status':'finished'}

@task(multiple_outputs=False, trigger_rule="one_failed")
def mark_analysis_failed():
	return {'status':'failed'}

@task(multiple_outputs=False)
def fetch_analysis_design():
    return '/design.json'

@task(multiple_outputs=False)
def create_work_dir():
	return '/work_dir'

@task(multiple_outputs=False)
def export_factory(work_dir):
	design_data = [
		{'cosmx_run_id': 'A', 'export_directory_path': 'A_f'},
		{'cosmx_run_id': 'B', 'export_directory_path': 'B_f'}]
	return design_data

@task(multiple_outputs=False)
def run_ftp_export(run_entry, work_dir):
	run_entry.update({'download_path': work_dir + '/' + run_entry['export_directory_path'] + '/download'})
	return run_entry

@task(multiple_outputs=False)
def extract_ftp_export(run_entry):
    run_entry.update({'extract': '/path'})
    return run_entry

@task(multiple_outputs=False)
def validate_export_md5(run_entry):
    run_entry.update({'validation': True})
    return run_entry

@task_group
def run_export_task_group(run_entry, work_dir):
    downloaded_data = run_ftp_export(run_entry, work_dir)
    extracted_data = extract_ftp_export(run_entry=downloaded_data)
    return extracted_data

@task(multiple_outputs=False)
def collect_all_slides(run_entry):
    slide_data = [
		{'run_name': 'A', 'slide_id': 'slide1'},
		{'run_name': 'A', 'slide_id': 'slide2'},
		{'run_name': 'B', 'slide_id': 'slide3'},
		{'run_name': 'B', 'slide_id': 'slide4'}]
    return slide_data

@task(multiple_outputs=False)
def generate_count_qc_report(run_entry):
    run_entry.update({'count_qc_json': 'json', 'count_qc_html': 'html'})
    return run_entry

@task(multiple_outputs=False)
def generate_fov_qc_report(run_entry):
    run_entry.update({'fov_qc_json': 'json', 'fov_qc_html': 'html'})
    return run_entry

@task(multiple_outputs=False)
def generate_db_data(qc_list):
    run_entry = dict()
    for qc in qc_list:
        run_entry.update(**qc)
    run_entry.update({'db_data': '/DB'})
    return run_entry

@task(multiple_outputs=False)
def copy_data_to_globus(run_entry):
    run_entry.update({'Globus': '/globus_path'})
    return run_entry

@task(multiple_outputs=False)
def register_db_data(run_entry):
    run_entry.update({'db': True})
    return run_entry

@task(multiple_outputs=False)
def collect_qc_reports_and_upload_to_portal(run_entrys):
    print([r for r in run_entrys])
    return True

@task(multiple_outputs=False)
def send_email():
    return True

@task_group
def slide_qc_task_group(run_entry):
    validated_data = validate_export_md5(run_entry)
    count_qc = generate_count_qc_report(validated_data)
    fov_qc = generate_fov_qc_report(validated_data)
    ## add more here
    db_entry = generate_db_data(qc_list = [count_qc, fov_qc])
    registered_data = register_db_data(db_entry)
    globus_data = copy_data_to_globus(registered_data)
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
    running_analysis = \
        mark_analysis_running()
    finished_analysis = \
        mark_analysis_finished()
    failed_analysis = \
        mark_analysis_failed()
    design_file = fetch_analysis_design()
    running_analysis >> design_file
    work_dir = \
		create_work_dir()
    design_file >> work_dir
    design_data = \
        export_factory(work_dir)
    downloaded_data = \
        run_export_task_group.\
		    partial(work_dir=work_dir).\
		    expand(run_entry=design_data)
    all_slides = \
        collect_all_slides(run_entry=downloaded_data)
    slide_qc_reports = \
        slide_qc_task_group.\
            expand(run_entry=all_slides)
    collect_qc = collect_qc_reports_and_upload_to_portal(slide_qc_reports)
    email_user = send_email()
    collect_qc >> email_user
    email_user >> finished_analysis
    email_user >> failed_analysis

dag43_cosmx_export_and_qc()