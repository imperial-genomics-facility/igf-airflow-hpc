import os
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
#from airflow.operators.trigger_dagrun import TriggerDagRunOperator # FIX for v2

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
ANALYSIS_LIST = \
    Variable.get("analysis_dag_list", default_var={})
with dag:
    ## TASK
    find_analysis_designs = \
        BranchPythonOperator(
            task_id="find_analysis_designs",
            dag=dag,
            queue='hpc_4G',
            params={},
            python_callable=None)
    ## TASK
    load_analysis_design_tasks = list()
    for i in range(1, 21):
        t = \
            PythonOperator(
                task_id="load_analysis_design_{i}".format(i),
                dag=dag,
                params={},
                python_callable=None)
        load_analysis_design_tasks.append(t)
    ## TASK
    find_analysis_to_trigger_dags = \
        BranchPythonOperator(
            task_id="find_analysis_to_trigger_dags",
            dag=dag,
            queue='hpc_4G',
            params={},
            python_callable=None)
    ## TASK
    trigger_analysis_dag_tasks = list()
    for analysis_name in ANALYSIS_LIST.keys():
        for i in (range(1, 21)):
            t = \
                TriggerDagRunOperator(
                    task_id="trigger_{0}_{0}".format(analysis_name, i),
                    dag=dag,
                    trigger_dag_id=analysis_name,
                    queue='hpc_4G',
                    params={},
                    python_callable=None)
            trigger_analysis_dag_tasks.append(t)
    ## PIPELINE
    find_analysis_designs >> load_analysis_design_tasks 
    load_analysis_design_tasks >> find_analysis_to_trigger_dags
    find_analysis_to_trigger_dags >> trigger_analysis_dag_tasks