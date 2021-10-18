import os
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from igf_airflow.utils.dag16_test_illumina_demultiplexing_utils import get_samplesheet_and_decide_flow_func
from igf_airflow.utils.dag16_test_illumina_demultiplexing_utils import  run_demultiplexing_func
#from igf_airflow.utils.dag16_test_illumina_demultiplexing_utils import  prepare_merged_report_func


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
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        default_args=args,
        tags=['hpc'])
with dag:
    ## TASK
    get_samplesheet_and_decide_flow = \
        BranchPythonOperator(
            task_id="get_samplesheet_and_decide_flow",
            dag=dag,
            queue='hpc_4G',
            params={
                'demult_task_prefix': 'run_demult_for_lane',
                'runParameters_xml_file_name': 'runParameters.xml',
                'runinfo_xml_file_name': 'RunInfo.xml',
                'output_path_xcom_key': 'temp_output_path',
                'samplesheet_xcom_key': 'formatted_samplesheets'},
            python_callable=get_samplesheet_and_decide_flow_func)
    ## TASK
    # prepare_merged_report = \
    #    PythonOperator(
    #        task_id='prepare_merged_report',
    #        dag=dag,
    #        queue='hpc_4G',
    #        params={
    #            'samplesheet_xcom_task': 'get_samplesheet_and_decide_flow',
    #            'samplesheet_xcom_key': 'formatted_samplesheets',
    #            'output_path_xcom_key': 'temp_output_path'},
    #        python_callable=prepare_merged_report_func)
    ## TASK
    for i in range(1, 9):
        t = \
            PythonOperator(
                task_id='{0}_{1}'.format('run_demult_for_lane', i),
                dag=dag,
                queue='hpc_4G',
                params={
                    'lane_id': i,
                    'runinfo_xml_file_name': 'RunInfo.xml',
                    'threads': 1,
                    'samplesheet_xcom_task': 'get_samplesheet_and_decide_flow',
                    'samplesheet_xcom_key': 'formatted_samplesheets',
                    'output_path_xcom_key': 'temp_output_path'},
                python_callable=run_demultiplexing_func)
        ## PIPELINE
        get_samplesheet_and_decide_flow >> t

