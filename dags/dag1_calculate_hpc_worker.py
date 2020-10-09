import json
from airflow.models import DAG,Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.dates import days_ago
from igf_airflow.check_celery_queue import fetch_queue_list_from_redis_server,airflow_utils_for_redis
from igf_airflow.check_celery_queue import calculate_new_workers

args = {
    'owner':'airflow',
    'start_date':days_ago(2),
    'provide_context': True,
}

hpc_hook = SSHHook(ssh_conn_id='hpc_conn')

dag = DAG(
        dag_id='dag1_calculate_hpc_worker',
        schedule_interval=None,
        default_args=args,
        tags=['igf-lims',]
      )


def get_new_workers(**kwargs):
  try:
    if 'ti' not in kwargs:
      raise ValueError('ti not present in kwargs')
    ti = kwargs.get('ti')
    active_tasks = ti.xcom_pull(task_ids='fetch_active_jobs_from_hpc')
    active_tasks = active_tasks.decode()
    active_tasks = json.loads(active_tasks)
    queued_tasks = ti.xcom_pull(task_ids='fetch_queue_list_from_redis')
    worker_to_submit,unique_queue_list = \
      calculate_new_workers(
        queue_list=queued_tasks,
        active_jobs_dict=active_tasks)
    for key,value in worker_to_submit.items():
      ti.xcom_push(key=key,value=value)
    unique_queue_list = \
      [q for q in unique_queue_list if q.startswith('hpc')]
    return unique_queue_list
  except Exception as e:
    raise ValueError('Failed to get new workers, error: {0}'.format(e))



with dag:
  fetch_queue_list_from_redis = \
    PythonOperator(
      task_id='fetch_queue_list_from_redis',
      dag=dag,
      python_callable=airflow_utils_for_redis,
      op_kwargs={"redis_conf_file":Variable.get('redis_conn_file')},
      queue='igf-lims'
    )

  check_hpc_queue = \
    SSHOperator(
      task_id='check_hpc_queue',
      ssh_hook=hpc_hook,
      dag=dag,
      command='source /etc/bashrc;qstat',
      queue='igf-lims'
    )

  fetch_active_jobs_from_hpc = \
    SSHOperator(
      task_id='fetch_active_jobs_from_hpc',
      ssh_hook=hpc_hook,
      dag=dag,
      command='source /etc/bashrc;bash /project/tgu/data2/airflow_test/github/igf-airflow-hpc/scripts/hpc/hpc_job_count_runner.sh ',
      do_xcom_push=True,
      queue='igf-lims'
    )

  calculate_new_worker_size_and_branch = \
    BranchPythonOperator(
      task_id='calculate_new_worker_size_and_branch',
      dag=dag,
      python_callable=get_new_workers,
      queue='igf-lims',
    )

  check_hpc_queue >> fetch_active_jobs_from_hpc
  calculate_new_worker_size_and_branch << [fetch_queue_list_from_redis,fetch_active_jobs_from_hpc]

  hpc_queue_list = Variable.get('hpc_queue_list')
  for q,data in hpc_queue_list.items():
    pbs_resource = data.get('pbs_resource')
    airflow_queue = data.get('airflow_queue')
    t = SSHOperator(
      task_id=q,
      ssh_hook=hpc_hook,
      dag=dag,
      queue='igf-lims',
      command="""
      {% if ti.xcom_pull(key=params.job_name,task_ids="calculate_new_worker_size_and_branch" ) > 1 %}
        source /etc/bashrc;qsub -k n -m n -N {{ params.job_name }} -J 1-{{ ti.xcom_pull(key=params.job_name,task_ids="calculate_new_worker_size_and_branch" ) }}  {{ params.pbs_resource }} -- /project/tgu/data2/airflow_test/github/igf-airflow-hpc/scripts/hpc/airflow_worker.sh {{  params.airflow_queue }} {{ params.job_name }}
      {% else %}
        source /etc/bashrc;qsub -k n -m n -N {{ params.job_name }} {{ params.pbs_resource }} -- /project/tgu/data2/airflow_test/github/igf-airflow-hpc/scripts/hpc/airflow_worker.sh {{  params.airflow_queue }} {{ params.job_name }}
      {% endif %}
      """,
      params={'pbs_resource':pbs_resource,'airflow_queue':airflow_queue,'job_name':q}
    )
    calculate_new_worker_size_and_branch >> t