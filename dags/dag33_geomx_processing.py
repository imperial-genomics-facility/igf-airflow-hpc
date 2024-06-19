import json, time, os
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow import XComArg
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from igf_airflow.utils.generic_airflow_tasks import (
	mark_analysis_running,
	fetch_analysis_design_from_db,
	no_work,
	send_email_to_user,
	copy_data_to_globus,
	mark_analysis_finished,
	mark_analysis_failed)
from igf_airflow.utils.dag33_geomx_processing_util import (
	check_and_process_config_file,
	fetch_fastq_file_path_from_db,
	create_temp_fastq_input_dir,
	prepare_geomx_dcc_run_script,
	generate_geomx_dcc_count,
	generate_geomx_qc_report,
	calculate_md5sum_for_dcc,
	copy_geomx_config_file_to_output,
	load_dcc_count_to_db)

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
@dag(
	dag_id=DAG_ID,
	schedule=None,
	start_date=pendulum.yesterday(),
	dagrun_timeout=timedelta(minutes=180),
	catchup=False,
	max_active_runs=10,
    default_view='grid',
    orientation='TB',
	tags=["geomx", "spatial", "hpc"],
)
def geomx_dag():
	analysis_running = \
		mark_analysis_running(
			next_task="fetch_analysis_design",
			last_task="no_work")
	analysis_design = \
		fetch_analysis_design_from_db()
	analysis_running >> analysis_design
	analysis_running >> no_work()
	config_file = \
		check_and_process_config_file(analysis_design)
	fastq_list = \
		fetch_fastq_file_path_from_db(analysis_design)
	temp_fastq_dir = \
		create_temp_fastq_input_dir(fastq_list)
	dcc_run_script = \
		prepare_geomx_dcc_run_script(
			design_dict=analysis_design,
			symlink_dir=temp_fastq_dir,
			config_file_dict=config_file)
	dcc_count = \
		generate_geomx_dcc_count(
			design_dict=analysis_design,
			dcc_script_dict=dcc_run_script)
	qc_report = \
		generate_geomx_qc_report(
			design_dict=analysis_design,
			dcc_count_path=dcc_count,
			config_file_dict=config_file)
	md5_sum = \
		calculate_md5sum_for_dcc(dcc_count)
	geomx_config_file = \
		copy_geomx_config_file_to_output(
			design_dict=analysis_design,
			dcc_count_path=dcc_count)
	load_dcc = \
		load_dcc_count_to_db(
			dcc_count_path=dcc_count,
			md5_file=md5_sum,
			geomx_config=geomx_config_file,
			report_file=qc_report)
	copy_globus = \
		copy_data_to_globus(load_dcc)
	copy_globus >> send_email_to_user() >> mark_analysis_finished() >> mark_analysis_failed()


## PIPELINE
geomx_dag()