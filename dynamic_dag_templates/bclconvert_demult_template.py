import os
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from igf_airflow.utils.dag22_bclconvert_demult_utils import find_seqrun_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import mark_seqrun_status_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_samplesheet_from_portal_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import format_and_split_samplesheet_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import setup_qc_page_for_project_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import setup_globus_transfer_for_project_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import trigger_lane_jobs
from igf_airflow.utils.dag22_bclconvert_demult_utils import trigger_ig_jobs
from igf_airflow.utils.dag22_bclconvert_demult_utils import run_bclconvert_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import bclconvert_report_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import sample_known_qc_factory_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import calculate_fastq_md5_checksum_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import load_fastq_and_qc_to_db_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import fastqc_run_wrapper_for_known_samples_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import fastqscreen_run_wrapper_for_known_samples_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import merge_single_cell_fastq_files_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import check_output_for_project_lane_index_group_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import collect_qc_reports_for_samples_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import multiqc_for_project_lane_index_group_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import copy_qc_to_ftp_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import build_qc_page_data_for_project_lane_index_group_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import build_qc_page_for_project_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import load_bclconvert_report_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import fastqc_for_undetermined_reads_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import fastq_screen_for_undetermined_reads_func
from igf_airflow.utils.dag22_bclconvert_demult_utils import multiqc_for_undetermined_reads_func


### DYNAMIC DAG DEFINITION
## INPUT - seqrun_igf_id
seqrun_igf_id = '{{ SEQRUN_IGF_ID }}'

## INPUT - sample group info
sample_groups = {{ SAMPLE_GROUPS|safe }}

## INPUT - formatted samplesheets
formatted_samplesheets = {{ FORMATTED_SAMPLESHEETS|safe }}

## ARGS
args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 1}

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")

dag = \
    DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        default_args=args,
        default_view='graph',
        orientation='TB',
        tags=['hpc', 'dynamic'])

with dag:
	## TASK
    find_seqrun = \
        BranchPythonOperator(
            task_id="find_seqrun",
            dag=dag,
            queue="hpc_4G",
            params={
                'seqrun_igf_id': seqrun_igf_id,
                'next_task_id': 'mark_seqrun_as_running',
                'no_work_task': 'no_work'
            },
            python_callable=find_seqrun_func)
    ## TASK
    mark_seqrun_as_running = \
        BranchPythonOperator(
            task_id="mark_seqrun_as_running",
            dag=dag,
            queue="hpc_4G",
            params={
                'seqrun_igf_id': seqrun_igf_id,
                'next_task': 'format_and_split_samplesheet',
                'last_task': 'no_work',
                'seed_status': 'RUNNING',
                'no_change_status': 'RUNNING',
                'seed_table': 'seqrun',
                'check_all_pipelines_for_seed_id': True
            },
            python_callable=mark_seqrun_status_func)
    ## TASK
    """ fetch_samplesheet_for_run = \
        PythonOperator(
            task_id="fetch_samplesheet_for_run",
            dag=dag,
            queue='hpc_4G',
            params={
                'seqrun_igf_id': seqrun_igf_id,
                'samplesheet_xcom_key': 'samplesheet_data',
            },
            python_callable=get_samplesheet_from_portal_func) """
    ## TASK
    format_and_split_samplesheet = \
        BranchPythonOperator(
			task_id="format_and_split_samplesheet",
			dag=dag,
			queue="hpc_4G",
			params={
                'seqrun_igf_id': seqrun_igf_id,
                'formatted_samplesheets': formatted_samplesheets,
                'sample_groups': sample_groups,
                'samplesheet_xcom_key': 'samplesheet_data',
                'samplesheet_xcom_task': 'fetch_samplesheet_for_run',
				'xcom_key': 'formatted_samplesheets',
				'project_task_prefix': 'setup_qc_page_for_project_',
				'max_projects': len(sample_groups)},
			python_callable=format_and_split_samplesheet_func)
    ## TASK
    no_work = \
        DummyOperator(
            task_id="no_work")
    ## PIPELINE
    find_seqrun >> mark_seqrun_as_running
    find_seqrun >> no_work
    # mark_seqrun_as_running >> fetch_samplesheet_for_run
    mark_seqrun_as_running >> no_work
    # fetch_samplesheet_for_run >> format_and_split_samplesheet
    mark_seqrun_as_running >> format_and_split_samplesheet
    ## TASK
    mark_seqrun_as_finished = \
        PythonOperator(
            task_id="mark_seqrun_as_finished",
            dag=dag,
            queue="hpc_4G",
            trigger_rule="all_done",
            params={
                'seqrun_igf_id': seqrun_igf_id,
                'seed_status': 'FINISHED',
                'no_change_status': 'SEEDED',
                'seed_table': 'seqrun'
            },
            python_callable=mark_seqrun_status_func)
    ## LOOP - PROJECT
    for project_id in sample_groups:
        ## TASK - PROJECT
        setup_qc_page_for_project = \
            PythonOperator(
                task_id=f"setup_qc_page_for_project_{project_id}",
                dag=dag,
                queue="hpc_4G",
                params={
                    'seqrun_igf_id': seqrun_igf_id,
                    'formatted_samplesheets': formatted_samplesheets,
                    'sample_groups': sample_groups,
                    'project_data_xcom_key': 'formatted_samplesheets',
                    'project_data_xcom_task': 'format_and_split_samplesheet',
                    'project_index_column': 'project_index',
                    'project_index': project_id,
                    'project_column': 'project'
                },
                python_callable=setup_qc_page_for_project_func)
        ## TASK - PROJECT
        setup_globus_transfer_for_project = \
            PythonOperator(
                task_id=f"setup_globus_transfer_for_project_{project_id}",
                dag=dag,
                queue="hpc_4G",
                params={
                    'seqrun_igf_id': seqrun_igf_id,
                    'formatted_samplesheets': formatted_samplesheets,
                    'sample_groups': sample_groups,
                    'project_data_xcom_key': 'formatted_samplesheets',
                    'project_data_xcom_task': 'format_and_split_samplesheet',
                    'project_index_column': 'project_index',
                    'project_index': project_id,
                    'project_column': 'project',
                    "globus_dir_xcom_key": "globus_root_dir"
                },
                python_callable=setup_globus_transfer_for_project_func)
        ## TASK - PROJECT
        get_lanes_for_project = \
            BranchPythonOperator(
                task_id=f"get_lanes_for_project_{project_id}",
                dag=dag,
				queue="hpc_4G",
                params={
                    'seqrun_igf_id': seqrun_igf_id,
                    'formatted_samplesheets': formatted_samplesheets,
                    'sample_groups': sample_groups,
					"xcom_key": "formatted_samplesheets",
					"xcaom_task": "format_and_split_samplesheet",
					"project_index": project_id,
					"project_index_column": "project_index",
					"lane_index_column": "lane_index",
					"max_lanes": len(sample_groups.get(project_id)),
					"lane_task_prefix": f"get_igs_for_project_{project_id}_lane_"},
					python_callable=trigger_lane_jobs)
        ## TASK - PROJECT
        build_qc_page_for_project = \
            PythonOperator(
                task_id=f"build_qc_page_for_project_{project_id}",
                dag=dag,
				queue="hpc_4G",
                params={
                    "seqrun_igf_id": seqrun_igf_id,
                    "formatted_samplesheets": formatted_samplesheets,
					"project_index": project_id,
                    'ftp_path_prefix': '/www/html/',
                    'ftp_url_prefix': 'http://eliot.med.ic.ac.uk/',
                    'run_qc_page_name': 'index.html',
                    'run_qc_template_name': 'project_info/run_level_qc.html',
                    'sample_qc_template_name': 'project_info/sample_level_qc.html',
                    'known_multiqc_name_suffix': 'known',
                    'undetermined_multiqc_name_suffix': 'undetermined',
                    "samplereadcountfile": 'samplereadcountfile.json',
                    "samplereadcountcsvfile": 'samplereadcountfile.csv',
                    "seqruninfofile": 'seqruninfofile.json',
                    "status_data_json": 'status_data.json',
                },
                python_callable=build_qc_page_for_project_func)
        ## TASK - PROJECT
        send_email_to_user_for_project = \
            DummyOperator(
                task_id=f"send_email_to_user_for_project_{project_id}")
        ## PIPELINE - PROJECT
        format_and_split_samplesheet >> setup_qc_page_for_project
        setup_qc_page_for_project >> setup_globus_transfer_for_project
        setup_globus_transfer_for_project >> get_lanes_for_project
        build_qc_page_for_project >> send_email_to_user_for_project
        send_email_to_user_for_project >> mark_seqrun_as_finished
        ## LOOP - LANE
        for lane_id in sample_groups.get(project_id):
            ## TASK - LANE
            get_igs_for_project_lane = \
                BranchPythonOperator(
                    task_id=f"get_igs_for_project_{project_id}_lane_{lane_id}",
                    dag=dag,
					queue="hpc_4G",
				    params={
                        'seqrun_igf_id': seqrun_igf_id,
                        'formatted_samplesheets': formatted_samplesheets,
                        'sample_groups': sample_groups,
						"xcom_key": "formatted_samplesheets",
						"xcaom_task": "format_and_split_samplesheet",
						"project_index": project_id,
						"project_index_column": "project_index",
						"lane_index": lane_id,
						"lane_index_column": "lane_index",
						"ig_index_column": "index_group_index",
						"max_index_groups": len(sample_groups.get(project_id).get(lane_id)),
						"ig_task_prefix": f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_"},
					python_callable=trigger_ig_jobs)
            ## TASK - LANE
            build_qc_page_for_project_lane = \
                DummyOperator(
                    task_id=f"build_qc_page_for_project_{project_id}_lane_{lane_id}")
            ## PIPELINE - LANE
            get_lanes_for_project >> get_igs_for_project_lane
            build_qc_page_for_project_lane >> build_qc_page_for_project
             ## LOOP - INDEXGROUP
            for index_id in sample_groups.get(project_id).get(lane_id):
                ## TASK - INDEXGROUP
                bclconvert_for_project_lane_index_group = \
                    PythonOperator(
                        task_id=f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_64G16t",
                        params={
                            'seqrun_igf_id': seqrun_igf_id,
                            'formatted_samplesheets': formatted_samplesheets,
                            'sample_groups': sample_groups,
							'xcom_key': 'formatted_samplesheets',
							'xcom_task': 'format_and_split_samplesheet',
							'xcom_key_for_reports': 'bclconvert_reports',
							'xcom_key_for_output': 'bclconvert_output',
							'project_index_column': 'project_index',
							'lane_index_column': 'lane_index',
							'ig_index_column': 'index_group_index',
							'project_column': 'project',
							'lane_column': 'lane',
							'index_group_column': 'index_group',
							'project_index': project_id,
							'lane_index': lane_id,
							'ig_index': index_id,
							'samplesheet_column': 'samplesheet_file',
							'bcl_num_conversion_threads': 4,
							'bcl_num_compression_threads': 2,
							'bcl_num_decompression_threads': 2,
							'bcl_num_parallel_tiles': 4},
						python_callable=run_bclconvert_func)
                ## TASK - INDEXGROUP
                generate_demult_report_for_project_lane_index_group = \
                    PythonOperator(
                        task_id=f"generate_demult_report_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_4G",
                        params={
                            'seqrun_igf_id': seqrun_igf_id,
                            'formatted_samplesheets': formatted_samplesheets,
                            'sample_groups': sample_groups,
							'xcom_key_for_reports': "bclconvert_reports",
							'xcom_task_for_reports': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            'xcom_key_for_html_reports': "bclconvert_html_reports"},
						python_callable=bclconvert_report_func)
                ## TASK - INDEXGROUP
                load_demult_report_for_project_lane_index_group = \
                    PythonOperator(
                        task_id=f"load_demult_report_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
                        queue="hpc_4G",
                        params={
                            'seqrun_igf_id': seqrun_igf_id,
                            'formatted_samplesheets': formatted_samplesheets,
                            'project_index': project_id,
							'lane_index': lane_id,
							'index_group_index': index_id,
                            'sample_groups': sample_groups,
                            'xcom_key_for_bclconvert_reports': "bclconvert_reports",
                            'xcom_task_for_bclconvert_reports': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            'xcom_key_for_html_reports': "bclconvert_html_reports",
                            'xcom_task_for_html_reports': f"generate_demult_report_for_project_{project_id}_lane_{lane_id}_ig_{index_id}"
                        },
                        python_callable=load_bclconvert_report_func)
                ## TASK - INDEXGROUP
                check_output_for_project_lane_index_group = \
                    PythonOperator(
                        task_id=f"check_output_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_4G",
                        params={
                            'seqrun_igf_id': seqrun_igf_id,
                            'xcom_key_bclconvert_reports': 'bclconvert_reports',
                            'xcom_task_bclconvert_reports': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            'demult_stats_file_name': "Demultiplex_Stats.csv"
                        },
                        python_callable=check_output_for_project_lane_index_group_func)
                ## TASK - INDEXGROUP
                merge_single_cell_fastq_files = \
                    PythonOperator(
                        task_id=f"merge_single_cell_fastq_files_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_4G",
                        params={
                            'project_index': project_id,
							'lane_index': lane_id,
							'index_group_index': index_id,
                            'seqrun_igf_id': seqrun_igf_id,
                            'formatted_samplesheets': formatted_samplesheets,
                            'xcom_key_bclconvert_output': 'bclconvert_output',
                            'xcom_task_bclconvert_output': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            'xcom_key_bclconvert_reports': 'bclconvert_reports',
                            'xcom_task_bclconvert_reports': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            'samplesheet_file_suffix': "SampleSheet.csv"
                        },
                        python_callable=merge_single_cell_fastq_files_func)
                ## TASK - INDEXGROUP
                collect_qc_reports_for_samples = \
                    PythonOperator(
                        task_id=f"collect_qc_reports_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_4G",
                        params={
                            "xcom_key_for_bclconvert_output": "bclconvert_output",
                            "bclconvert_task_prefix": "bclconvert_",
                            "fastqc_task_prefix": f"sample_group_{project_id}_{lane_id}_{index_id}.fastqc_",
                            "fastq_screen_task_prefix": f"sample_group_{project_id}_{lane_id}_{index_id}.fastq_screen_",
                            "xcom_key_for_fastqc_output": "fastqc_output",
                            "xcom_key_for_fastq_screen_output": "fastq_screen_output"
                        },
                        python_callable=collect_qc_reports_for_samples_func)
                ## TASK - INDEXGROUP
                multiqc_for_project_lane_index_group = \
                    PythonOperator(
                        task_id=f"multiqc_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_4G",
                        params={
                            'seqrun_igf_id': seqrun_igf_id,
                            'formatted_samplesheets': formatted_samplesheets,
                            'project_index_column': 'project_index',
							'lane_index_column': 'lane_index',
							'ig_index_column': 'index_group_index',
							'project_column': 'project',
							'lane_column': 'lane',
							'index_group_column': 'index_group',
							'project_index': project_id,
							'lane_index': lane_id,
							'index_group_index': index_id,
                            "xcom_key_for_qc_file_list": "qc_file_list",
                            "xcom_key_for_multiqc": "multiqc",
                            "tool_order_list": ["bclconvert", "fastqc", "fastqscreen"],
                            "multiqc_param_list": ["--zip-data-dir",],
                            "status_tag": "known"
                        },
                        python_callable=multiqc_for_project_lane_index_group_func)
                ##TASK - INDEXGROUP
                copy_known_multiqc_to_ftp = \
                    PythonOperator(
                        task_id=f"copy_known_multiqc_to_ftp_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_4G",
						params={
                            "remote_collection_type": "FTP_MULTIQC_HTML_REPORT",
                            "xcom_key_for_qc_collection": "multiqc",
                            "xcom_task_for_qc_collection": f"multiqc_for_project_{project_id}_lane_{lane_id}_ig_{index_id}"
                        },
                        python_callable=copy_qc_to_ftp_func)
                ##TASK - INDEXGROUP
                fastqc_for_undetermined_reads = \
                    PythonOperator(
                        task_id=f"fastqc_for_undetermined_reads_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
                        queue="hpc_4G",
                        params={
                            "xcom_key_for_undetermined_fastqc": "undetermined_fastqc",
                            'xcom_key_for_bclconvert_output': 'bclconvert_output',
							'xcom_task_for_bclconvert_output': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        },
                        python_callable=fastqc_for_undetermined_reads_func)
                ## TASK - INDEXGROUP
                fastq_screen_for_undetermined_reads = \
                    PythonOperator(
                        task_id=f"fastq_screen_for_undetermined_reads_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
                        queue="hpc_8G",
                        params={
                            "xcom_key_for_undetermined_fastq_screen": "undetermined_fastq_screen",
                            'xcom_key_for_bclconvert_output': 'bclconvert_output',
							'xcom_task_for_bclconvert_output': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        },
                        python_callable=fastq_screen_for_undetermined_reads_func
                    )
                ## TASK - INDEXGROUP
                multiqc_for_undetermined_reads = \
                    PythonOperator(
                        task_id=f"multiqc_for_undetermined_reads_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
                        queue="hpc_4G",
                        params={
                            'seqrun_igf_id': seqrun_igf_id,
                            'formatted_samplesheets': formatted_samplesheets,
                            'project_index': project_id,
							'lane_index': lane_id,
							'index_group_index': index_id,
                            'tool_order_list': ['bclconvert', 'fastqc', 'fastqscreen'],
                            'multiqc_param_list': ['--zip-data-dir'],
                            'status_tag': 'undetermined',
                            "xcom_key_for_undetermined_fastq_screen": "undetermined_fastq_screen",
                            "xcom_task_for_undetermined_fastq_screen": f"fastq_screen_for_undetermined_reads_{project_id}_lane_{lane_id}_ig_{index_id}",
                            "xcom_key_for_undetermined_fastqc": "undetermined_fastqc",
                            "xcom_task_for_undetermined_fastqc": f"fastqc_for_undetermined_reads_{project_id}_lane_{lane_id}_ig_{index_id}",
                            'xcom_key_for_bclconvert_reports': 'bclconvert_reports',
							'xcom_task_for_bclconvert_reports': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        },
                        python_callable=multiqc_for_undetermined_reads_func
                    )
                ## TASK - INDEXGROUP
                build_qc_page_data_for_project_lane_index_group = \
                    PythonOperator(
                        task_id=f"build_qc_page_data_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                        dag=dag,
						queue="hpc_4G",
                        params={
                            "seqrun_igf_id": seqrun_igf_id,
                            "formatted_samplesheets": formatted_samplesheets,
                            "project_index_column": "project_index",
							"lane_index_column": "lane_index",
							"ig_index_column": "index_group_index",
							"project_column": "project",
							"lane_column": "lane",
							"index_group_column": "index_group",
							"project_index": project_id,
							"lane_index": lane_id,
							"index_group_index": index_id,
                            "xcom_key_bclconvert_reports": "bclconvert_reports",
                            "xcom_task_bclconvert_reports": f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            "samplesheet_file_suffix": "SampleSheet.csv",
                            "ftp_path_prefix": "/www/html/",
                            "ftp_url_prefix": "http://eliot.med.ic.ac.uk/"
                        },
                        python_callable=build_qc_page_data_for_project_lane_index_group_func
                    )
                ## PIPELINE - INDEXGROUP
                get_igs_for_project_lane >> bclconvert_for_project_lane_index_group
                bclconvert_for_project_lane_index_group >> generate_demult_report_for_project_lane_index_group
                generate_demult_report_for_project_lane_index_group >> load_demult_report_for_project_lane_index_group
                load_demult_report_for_project_lane_index_group >> check_output_for_project_lane_index_group
                check_output_for_project_lane_index_group >> merge_single_cell_fastq_files
                multiqc_for_project_lane_index_group >> copy_known_multiqc_to_ftp
                copy_known_multiqc_to_ftp >> build_qc_page_data_for_project_lane_index_group
                merge_single_cell_fastq_files >> fastqc_for_undetermined_reads
                merge_single_cell_fastq_files >> fastq_screen_for_undetermined_reads
                fastqc_for_undetermined_reads >> multiqc_for_undetermined_reads
                fastq_screen_for_undetermined_reads >> multiqc_for_undetermined_reads
                bclconvert_for_project_lane_index_group >> multiqc_for_undetermined_reads
                multiqc_for_undetermined_reads >> build_qc_page_data_for_project_lane_index_group
                build_qc_page_data_for_project_lane_index_group >> build_qc_page_for_project_lane
                ## TASKGROUP - SAMPLE
                with TaskGroup(group_id=f'sample_group_{project_id}_{lane_id}_{index_id}') as sample_group:
                    ## TASK - INDEXGROUP
                    get_samples_for_project_lane_ig = \
                        PythonOperator(
                            task_id=f"get_samples_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            dag=dag,
							queue="hpc_4G",
                            params={
                                'seqrun_igf_id': seqrun_igf_id,
                                'formatted_samplesheets': formatted_samplesheets,
                                'sample_groups': sample_groups,
								'xcom_key_for_bclconvert_output': 'bclconvert_output',
								'xcom_task_for_bclconvert_output': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
								'xcom_key_for_sample_group': 'sample_group',
								'samplesheet_file_suffix': "Reports/SampleSheet.csv",
								'max_samples': int(sample_groups.get(project_id).get(lane_id).get(index_id)),
								'project_index': project_id,
								'lane_index': lane_id,
								'ig_index': index_id,
								'next_task_prefix': f"sample_group_{project_id}_{lane_id}_{index_id}.calculate_md5_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_"},
							python_callable=sample_known_qc_factory_func)
                    ## TASK - INDEXGROUP
                    prepare_globus_copy_for_project_lane_ig = \
                        DummyOperator(
                            task_id=f"prepare_globus_copy_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            dag=dag,
                            queue="hpc_4G")
                    ## TASK - INDEXGROUP
                    get_fastqs_and_copy_to_globus_for_project_lane_ig = \
                        DummyOperator(
                            task_id=f"get_fastqs_and_copy_to_globus_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            dag=dag,
                            queue="hpc_4G")
                    ## TASK - INDEXGROUP
                    dump_md5_for_project_lane_ig = \
                        DummyOperator(
                            task_id=f"dump_md5_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            dag=dag,
                            queue="hpc_4G")
                    ## TASK - INDEXGROUP
                    copy_demult_reports_to_globus_for_project_lane_ig = \
                        DummyOperator(
                            task_id=f"copy_demult_reports_to_globus_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
                            dag=dag,
                            queue="hpc_4G")
                    ## PIPELINE - INDEXGROUP
                    prepare_globus_copy_for_project_lane_ig >> get_fastqs_and_copy_to_globus_for_project_lane_ig
                    get_fastqs_and_copy_to_globus_for_project_lane_ig >> dump_md5_for_project_lane_ig
                    dump_md5_for_project_lane_ig >> copy_demult_reports_to_globus_for_project_lane_ig
                    copy_demult_reports_to_globus_for_project_lane_ig >> collect_qc_reports_for_samples
                    ## LOOP - SAMPLE
                    for sample_id in range(1, sample_groups.get(project_id).get(lane_id).get(index_id) + 1):
                        ## TASK -SAMPLE
                        calculate_md5_for_fastq = \
                            PythonOperator(
								task_id=f"calculate_md5_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}",
								dag=dag,
								queue="hpc_4G",
								params={
                                    'seqrun_igf_id': seqrun_igf_id,
                                    'formatted_samplesheets': formatted_samplesheets,
                                    'sample_groups': sample_groups,
									'xcom_key_for_bclconvert_output': 'bclconvert_output',
									'xcom_task_for_bclconvert_output': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
									'xcom_key_for_sample_group': 'sample_group',
									'xcom_task_for_sample_group': f'sample_group_{project_id}_{lane_id}_{index_id}.get_samples_for_project_{project_id}_lane_{lane_id}_ig_{index_id}',
									"xcom_key_for_checksum_sample_group": "checksum_sample_group",
									'samplesheet_file_suffix': "Reports/SampleSheet.csv",
									'project_index': project_id,
									'lane_index': lane_id,
									'ig_index': index_id,
									'sample_group_id': sample_id},
								python_callable=calculate_fastq_md5_checksum_func)
                        ## TASK - SAMPLE
                        load_fastq_to_db = \
                            PythonOperator(
                                task_id=f"load_fastq_to_db_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}",
                                dag=dag,
								queue="hpc_4G",
								params={
                                    'seqrun_igf_id': seqrun_igf_id,
                                    'formatted_samplesheets': formatted_samplesheets,
                                    'sample_groups': sample_groups,
									"xcom_key_for_checksum_sample_group": "checksum_sample_group",
									"xcom_task_for_checksum_sample_group": f"sample_group_{project_id}_{lane_id}_{index_id}.calculate_md5_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}",
									"xcom_key_for_collection_group": "collection_group",
									"project_index_column": "project_index",
									'project_index': project_id,
									"lane_index_column": "lane_index",
									"lane_index": lane_id,
									'ig_index_column': 'index_group_index',
									'ig_index': index_id,
									'index_group_column': 'index_group'},
								python_callable=load_fastq_and_qc_to_db_func)
                        ## TASK - SAMPLE
                        copy_fastq_to_irods = \
                            DummyOperator(
                                task_id=f"copy_fastq_to_irods_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}")
                        ## TASK - SAMPLE
                        copy_fastq_to_globus = \
                            DummyOperator(
                                task_id=f"copy_fastq_to_globus_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}")
                        ## TASK - SAMPLE
                        fastqc = \
                            PythonOperator(
                                task_id=f"fastqc_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}",
                                dag=dag,
								queue="hpc_4G",
								params={
                                    'seqrun_igf_id': seqrun_igf_id,
                                    'formatted_samplesheets': formatted_samplesheets,
                                    'sample_groups': sample_groups,
                                    'xcom_key_for_fastqc_output': 'fastqc_output',
                                    'xcom_key_for_fastqc_collection': 'fastqc_collection',
									'xcom_key_for_bclconvert_output': 'bclconvert_output',
									'xcom_task_for_bclconvert_output': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
									"xcom_key_for_collection_group": "collection_group",
									"xcom_task_for_collection_group": f"sample_group_{project_id}_{lane_id}_{index_id}.load_fastq_to_db_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}"},
							    python_callable=fastqc_run_wrapper_for_known_samples_func)
                        ## TASK - SAMPLE
                        copy_fastqc_to_ftp = \
                            PythonOperator(
                                task_id=f"copy_fastqc_to_ftp_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}",
                                dag=dag,
							    queue="hpc_4G",
								params={
                                    'remote_collection_type': 'FTP_FASTQC_HTML_REPORT',
                                    'xcom_key_for_qc_collection': 'fastqc_collection',
                                    'xcom_task_for_qc_collection': f"sample_group_{project_id}_{lane_id}_{index_id}.fastqc_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}"
                                },
                                python_callable=copy_qc_to_ftp_func)
                        ## TASK - SAMPLE
                        fastq_screen = \
                            PythonOperator(
                                task_id=f"fastq_screen_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}",
                                dag=dag,
							    queue="hpc_8G",
								params={
                                    'seqrun_igf_id': seqrun_igf_id,
                                    'formatted_samplesheets': formatted_samplesheets,
                                    'sample_groups': sample_groups,
                                    'xcom_key_for_fastq_screen_output': 'fastq_screen_output',
                                    "xcom_key_for_fastq_screen_collection": "fastq_screen_collection",
								    'xcom_key_for_bclconvert_output': 'bclconvert_output',
								    'xcom_task_for_bclconvert_output': f"bclconvert_for_project_{project_id}_lane_{lane_id}_ig_{index_id}",
								    "xcom_key_for_collection_group": "collection_group",
									"xcom_task_for_collection_group": f"sample_group_{project_id}_{lane_id}_{index_id}.load_fastq_to_db_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}"},
								python_callable=fastqscreen_run_wrapper_for_known_samples_func)
                        ## TASK - SAMPLE
                        copy_fastq_screen_to_ftp = \
                            PythonOperator(
                                task_id=f"copy_fastq_screen_to_ftp_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}",
                                dag=dag,
							    queue="hpc_4G",
								params={
                                    'remote_collection_type': 'FTP_FASTQSCREEN_HTML_REPORT',
                                    'xcom_key_for_qc_collection': 'fastq_screen_collection',
                                    'xcom_task_for_qc_collection': f"sample_group_{project_id}_{lane_id}_{index_id}.fastq_screen_project_{project_id}_lane_{lane_id}_ig_{index_id}_sample_{sample_id}"
                                },
                                python_callable=copy_qc_to_ftp_func)
                        ## PIPELINE - SAMPLE
                        get_samples_for_project_lane_ig >> calculate_md5_for_fastq
                        calculate_md5_for_fastq >> load_fastq_to_db
                        load_fastq_to_db >> copy_fastq_to_irods
                        load_fastq_to_db >> copy_fastq_to_globus
                        load_fastq_to_db >> fastqc >> copy_fastqc_to_ftp
                        load_fastq_to_db >> fastq_screen >> copy_fastq_screen_to_ftp
                        load_fastq_to_db >> prepare_globus_copy_for_project_lane_ig
                        fastqc >> collect_qc_reports_for_samples
                        fastq_screen >> collect_qc_reports_for_samples
                    merge_single_cell_fastq_files >> sample_group
                    bclconvert_for_project_lane_index_group >> collect_qc_reports_for_samples
                    collect_qc_reports_for_samples >> multiqc_for_project_lane_index_group