import pendulum
import os
from datetime import timedelta
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.decorators import dag, task_group
from igf_airflow.utils.dag1_calculate_hpc_worker_utils import (
  celery_flower_workers,
  redis_queue_workers,
  calculate_workers,
  decide_scale_out_scale_in_ops,
  scale_in_hpc_workers,
  prep_scale_out_hpc_workers)


HPC_JOB_MODE = \
  Variable.get(
    'hpc_job_mode', default_var='NON_ARRAY')
AIRFLOW_HPC_JOB_SUBMISSION_SCRIPT = \
    Variable.get(
        'airflow_hpc_job_submission_script',
        default_var=None)
TOTAL_HPC_JOBS = \
    Variable.get(
        'hpc_max_total_workers', default_var=50)

hpc_hook = SSHHook(ssh_conn_id='hpc_ph2_conn')

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
doc_md_DAG = """
### Description

* This DAG calculates the number of workers for HPC phase 2 cluster
* It fetches the number of workers from celery flower and redis queue
* It also checks the number of jobs in the HPC queue using SSHOperator
* The DAG is scheduled to run at 5min's interval
* The DAG is not catchup enabled
* The DAG has a timeout of 10min

"""
@dag(
    dag_id=DAG_ID,
	schedule="*/5 * * * *",
	start_date=pendulum.yesterday(),
	catchup=False,
    max_active_runs=1,
    default_view='grid',
    orientation='TB',
    dagrun_timeout=timedelta(minutes=5),
    doc_md=doc_md_DAG,
    tags=['igf-lims', 'wells', 'hpc'])
def dag1_calculate_hpc_ph2_workers():
    ## TASK
    celery_workers = \
        celery_flower_workers()
    ## TASK
    redis_workers = \
        redis_queue_workers()
    ## TASK
    hpc_workers = \
        SSHOperator(
            task_id='check_hpc_queue',
            ssh_hook=hpc_hook,
            retries=0,
            command="""set -o pipefail; source /etc/bashrc; qstat -u $USER  -fw|grep -e "Job Id" -e Job_Name -e job_state|
            awk '
            /^Job Id/ {job_id=$3} 
            /Job_Name/ {job_name=$3} 
            /job_state/ {job_state=$3; print job_id "," job_name "," job_state}'|grep hpc||true""",
            pool='generic_pool',
            queue='generic',
            conn_timeout=10,
            cmd_timeout=10)
    ## TASK
    calculated_workers = \
        calculate_workers(
            hpc_worker_info=hpc_workers.output,
            celery_flower_worker_info=celery_workers,
            redis_queue_info=redis_workers,
            total_hpc_jobs=TOTAL_HPC_JOBS)
    ## TASK
    decide_scale = \
        decide_scale_out_scale_in_ops(
            scaled_workers_data=calculated_workers["scaled_worker_data"],
            scale_in_task='scale_in_hpc_workers',
            scale_out_task='prep_scale_out_hpc_workers')
    ## TASK
    scale_in_ops = \
        scale_in_hpc_workers(
            scaled_worker_data=calculated_workers["scaled_worker_data"],
            raw_worker_data=calculated_workers["raw_worker_data"])
    ## TASK
    prep_scale_out_ops = \
        prep_scale_out_hpc_workers(
            scaled_worker_data=calculated_workers["scaled_worker_data"])
    ## TASK
    scaled_hpc_workers = \
        SSHOperator(
            task_id='scale_out_hpc_workers',
            ssh_hook=hpc_hook,
            retries=0,
            params={"hpc_job_mode": HPC_JOB_MODE,
                    "airflow_job_submission_script": AIRFLOW_HPC_JOB_SUBMISSION_SCRIPT},
            command="""set -o pipefail; source /etc/bashrc;
            {% for queue_conf in task_instance.xcom_pull(task_ids='prep_scale_out_hpc_workers', key='return_value') %}
                {% if params.hpc_job_mode == "ARRAY" %}
                    {% if queue_conf['new_tasks'] > 1 %}
                        qsub -o /dev/null -e /dev/null -k n -m n \
                        -N {{ queue_conf['queue_name'] }} \
                        -J 1{{ queue_conf['new_tasks'] }}  {{ queue_conf['pbs_resource'] }} -- \
                        {{ params.airflow_job_submission_script }} {{ queue_conf['airflow_queue'] }} {{ queue_conf['queue_name'] }}
                    {% else %}
                        qsub -o /dev/null -e /dev/null -k n -m n \
                        -N {{ queue_conf['queue_name'] }} \
                        {{ queue_conf['pbs_resource'] }} -- \
                        {{ params.airflow_job_submission_script }} {{ queue_conf['airflow_queue'] }} {{ queue_conf['queue_name'] }}
                    {% endif %}
                {% else %}
                for i in $(seq 1 {{ queue_conf['new_tasks'] }});
                do
                    qsub -o /dev/null -e /dev/null -k n -m n \
                    -N {{ queue_conf['queue_name'] }} \
                    {{ queue_conf['pbs_resource'] }} -- \
                    {{ params.airflow_job_submission_script }} {{ queue_conf['airflow_queue'] }} {{ queue_conf['queue_name'] }};
                    sleep 1;
                done
                {% endif %}
                sleep 1;
            {% endfor %}
            """,
            pool='generic_pool',
            queue='generic',
            conn_timeout=30,
            cmd_timeout=30)
    ## PIPELINE
    celery_workers >> calculated_workers
    redis_workers >> calculated_workers
    hpc_workers >> calculated_workers >> decide_scale >> scale_in_ops
    decide_scale >> prep_scale_out_ops >> scaled_hpc_workers

dag1_calculate_hpc_ph2_workers()