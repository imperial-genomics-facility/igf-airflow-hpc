import os, logging
from datetime import timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
#from airflow.operators.trigger_dagrun import TriggerDagRunOperator # FIX for v2
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import find_analysis_designs_func
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import load_analysis_design_func
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import find_analysis_to_trigger_dags_func
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import get_dag_conf_for_analysis
from igf_airflow.logging.upload_log_msg import send_log_to_channels

SLACK_CONF = \
  Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = \
  Variable.get('ms_teams_conf', default_var=None)

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
## FUNCTION
def trigger_dag_func(context, dag_run_obj):
    try:
        ti = context.get('ti')
        xcom_key = \
            context['params'].get('xcom_key')
        xcom_task = \
            context['params'].get('xcom_task')
        analysis_name = \
            context['params'].get('analysis_name')
        index = \
            context['params'].get('index')
        analysis_list = \
            ti.xcom_pull(
                task_ids=xcom_task,
                key=xcom_key)
        analysis_detail = \
           get_dag_conf_for_analysis(
               analysis_list=analysis_list,
               analysis_name=analysis_name,
               index=index)
        dag_run_obj.payload = analysis_detail
        return dag_run_obj
        ## FIX for v2
        # trigger_dag = \
        #    TriggerDagRunOperator(
        #        task_id="trigger_dag_{0}_{1}".format(analysis_name, index),
        #        trigger_dag_id=analysis_name,
        #        conf=analysis_detail)
        #return trigger_dag.execute(context=context)
    except Exception as e:
        logging.error(e)
        message = \
        'analysis input finding error: {0}'.\
            format(e)
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            comment=message,
            reaction='fail')
        raise
## DAG
with dag:
    ## TASK
    find_analysis_designs = \
        BranchPythonOperator(
            task_id="find_analysis_designs",
            dag=dag,
            queue='hpc_4G',
            params={
                "load_analysis_task_prefix": "load_analysis_design",
                "no_task_name":"no_task",
                "load_task_limit": 20,
                "load_design_xcom_key": "load_design"},
            python_callable=find_analysis_designs_func)
    ## TASK
    no_task = \
        DummyOperator(
            task_id="no_task",
            dag=dag)
    ## TASK
    load_analysis_design_tasks = [no_task]
    for i in range(0, 10):
        t = \
            PythonOperator(
                task_id="load_analysis_design_{0}".format(i),
                dag=dag,
                params={
                    "task_index": i,
                    "load_design_xcom_key": "load_design",
                    "load_design_xcom_task": "find_analysis_designs"},
                python_callable=load_analysis_design_func)
        load_analysis_design_tasks.append(t)
    ## TASK
    find_analysis_to_trigger_dags = \
        BranchPythonOperator(
            task_id="find_analysis_to_trigger_dags",
            dag=dag,
            queue='hpc_4G',
            params={
                "no_trigger_task": "no_trigger",
                "analysis_limit": 40,
                "xcom_key": "analysis_list",
                "trigger_task_prefix": "trigger"},
            trigger_rule='none_failed_or_skipped',
            python_callable=find_analysis_to_trigger_dags_func)
    ## TASK
    no_trigger = \
        DummyOperator(
            task_id="no_trigger",
            dag=dag)
    ## TASK
    trigger_analysis_dag_tasks = [no_trigger]
    for analysis_name in ANALYSIS_LIST.keys():
        for i in range(0, 40):
            t = \
                TriggerDagRunOperator(
                    task_id="trigger_{0}_{1}".format(analysis_name, i),
                    trigger_dag_id=analysis_name,
                    dag=dag,
                    queue='hpc_4G',
                    params={
                        "xcom_key": "analysis_list",
                        "xcom_task": "find_analysis_to_trigger_dags",
                        "analysis_name": analysis_name,
                        "index": i},
                    python_callable=trigger_dag_func)
            trigger_analysis_dag_tasks.append(t)
    ## PIPELINE
    find_analysis_designs >> load_analysis_design_tasks 
    load_analysis_design_tasks >> find_analysis_to_trigger_dags
    find_analysis_to_trigger_dags >> trigger_analysis_dag_tasks