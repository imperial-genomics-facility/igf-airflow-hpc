import json,logging
from airflow.models import DAG,Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.dates import days_ago
from igf_airflow.celery.check_celery_queue import fetch_queue_list_from_redis_server
from igf_airflow.celery.check_celery_queue import calculate_new_workers

args = {
    'owner':'airflow',
    'start_date':days_ago(2),
    'provide_context': True,
}

hpc_hook = SSHHook(ssh_conn_id='hpc_conn')

dag = DAG(
        dag_id='dag1_calculate_hpc_worker',
        catchup=False,
        max_active_runs=1,
        schedule_interval="*/15 * * * *",
        default_args=args,
        tags=['igf-lims',]
      )


def airflow_utils_for_redis(**kwargs):
  """
  A function for dag1, TO DO
  """
  try:
    if 'redis_conf_file' not in kwargs:
      raise ValueError('redis_conf_file info is not present in the kwargs')

    redis_conf_file = kwargs.get('redis_conf_file')
    json_data = dict()
    with open(redis_conf_file,'r') as jp:
      json_data = json.load(jp)
    if 'redis_db' not in json_data:
      raise ValueError('redis_db key not present in the conf file')
    url = json_data.get('redis_db')
    queue_list = fetch_queue_list_from_redis_server(url=url)
    return queue_list
  except Exception as e:
    logging.error('Failed to run, error:{0}'.format(e))
    raise


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
        active_jobs_dict=active_tasks,
        max_workers_per_queue=Variable.get('hpc_max_workers_per_queue'),
        max_total_workers=Variable.get('hpc_max_total_workers'))
    for key,value in worker_to_submit.items():
      ti.xcom_push(key=key,value=value)
    unique_queue_list = \
      [q for q in unique_queue_list if q.startswith('hpc')]
    return unique_queue_list
  except Exception as e:
    logging.error('Failed to get new workers, error: {0}'.format(e))
    raise


with dag:
  ## TASK
  fetch_queue_list_from_redis = \
    PythonOperator(
      task_id='fetch_queue_list_from_redis',
      dag=dag,
      python_callable=airflow_utils_for_redis,
      op_kwargs={"redis_conf_file":Variable.get('redis_conn_file')},
      queue='igf-lims')
  ## TASK
  check_hpc_queue = \
    SSHOperator(
      task_id='check_hpc_queue',
      ssh_hook=hpc_hook,
      dag=dag,
      command='source /etc/bashrc;qstat',
      queue='igf-lims')
  ## TASK
  fetch_active_jobs_from_hpc = \
    SSHOperator(
      task_id='fetch_active_jobs_from_hpc',
      ssh_hook=hpc_hook,
      dag=dag,
      command="""
        source /etc/bashrc;\
        source /project/tgu/data2/airflow_test/secrets/hpc_env.sh;\
        python /project/tgu/data2/airflow_test/github/data-management-python/scripts/hpc/count_active_jobs_in_hpc.py """,
      do_xcom_push=True,
      queue='igf-lims')
  ## TASK
  calculate_new_worker_size_and_branch = \
    BranchPythonOperator(
      task_id='calculate_new_worker_size_and_branch',
      dag=dag,
      python_callable=get_new_workers,
      queue='igf-lims')
  ## TASK
  queue_tasks = list()
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
        source /etc/bashrc; \
        qsub \
          -o /dev/null \
          -e /dev/null \
          -k n -m n \
          -N {{ params.job_name }} \
          -J 1-{{ ti.xcom_pull(key=params.job_name,task_ids="calculate_new_worker_size_and_branch" ) }}  {{ params.pbs_resource }} -- \
            /project/tgu/data2/airflow_test/github/data-management-python/scripts/hpc/airflow_worker.sh {{  params.airflow_queue }} {{ params.job_name }}
      {% else %}
        source /etc/bashrc;\
        qsub \
          -o /dev/null \
          -e /dev/null \
          -k n -m n \
          -N {{ params.job_name }} {{ params.pbs_resource }} -- \
            /project/tgu/data2/airflow_test/github/data-management-python/scripts/hpc/airflow_worker.sh {{  params.airflow_queue }} {{ params.job_name }}
      {% endif %}
      """,
      params={'pbs_resource':pbs_resource,
              'airflow_queue':airflow_queue,
              'job_name':q})
    queue_tasks.\
      append(t)

  ## PIPELINE
  check_hpc_queue >> fetch_active_jobs_from_hpc
  calculate_new_worker_size_and_branch << \
    [fetch_queue_list_from_redis,
     fetch_active_jobs_from_hpc]
  calculate_new_worker_size_and_branch >> queue_tasks