import os, json, logging, requests, base64
from datetime import timedelta
from requests.auth import HTTPBasicAuth
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from igf_airflow.celery.check_celery_queue import fetch_queue_list_from_redis_server
from igf_airflow.celery.check_celery_queue import calculate_new_workers


CELERY_FLOWER_BASE_URL = Variable.get('celery_flower_base_url', default_var=None)
CELERY_FLOWER_CONFIG = Variable.get('celery_flower_config', default_var=None)
HPC_QUEUE_LIST = Variable.get("hpc_queue_list")

args = {
    'owner':'airflow',
    'start_date':days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
}

hpc_hook = SSHHook(ssh_conn_id='hpc_conn')

dag = DAG(
        dag_id='dag1_calculate_hpc_worker',
        catchup=False,
        max_active_runs=1,
        schedule_interval="*/3 * * * *",
        default_args=args,
        tags=['igf-lims', 'wells']
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
    if isinstance(active_tasks, str):
      active_tasks = \
        base64.b64decode(
          active_tasks.encode('ascii')).\
        decode('utf-8').\
        strip()
    elif isinstance(active_tasks, bytes):
      active_tasks = active_tasks.decode('utf-8')
    active_tasks = json.loads(active_tasks)
    queued_tasks = ti.xcom_pull(task_ids='fetch_queue_list_from_redis')
    worker_to_submit,unique_queue_list = \
      calculate_new_workers(
        queue_list=queued_tasks,
        active_jobs_dict=active_tasks,
        max_workers_per_queue=Variable.get('hpc_max_workers_per_queue'),
        max_total_workers=Variable.get('hpc_max_total_workers'))
    for key,value in worker_to_submit.items():
      if key in HPC_QUEUE_LIST:
        ti.xcom_push(key=key,value=value)
    unique_queue_list = \
      [q for q in unique_queue_list if q.startswith('hpc')]
    celery_worker_key = kwargs['params'].get('celery_worker_key')
    base_queue = kwargs['params'].get('base_queue')
    empty_celery_worker_key = kwargs['params'].get('empty_celery_worker_key')
    celery_workers = \
      ti.xcom_pull(task_ids='fetch_celery_workers',key=celery_worker_key)
    cleanup_list = list()
    for flower_entry in celery_workers:
      active_jobs_count = flower_entry.get('active_jobs')
      worker_id = flower_entry.get('worker_id')
      queue_list = flower_entry.get('queue_lists')
      if base_queue not in queue_list and \
         len(set(queue_list).intersection(set(unique_queue_list))) == 0 and \
         active_jobs_count == 0:
        cleanup_list.\
				  append(worker_id)
    if len(cleanup_list) > 0:
      ti.xcom_push(key=empty_celery_worker_key,value=cleanup_list)
      unique_queue_list.\
        append('cleanup_celery_workers')
    return unique_queue_list
  except Exception as e:
    logging.error('Failed to get new workers, error: {0}'.format(e))
    raise


def fetch_celery_worker_list(**context):
  """
  A function for fetching list of celery workers from flower server
  """
  try:
    ti = context.get('ti')
    celery_worker_key = context['params'].get('celery_worker_key')
    if CELERY_FLOWER_CONFIG is not None:
      ## new config file
      if not os.path.exists(CELERY_FLOWER_CONFIG):
        raise IOError("Celery flower config file not found")
      with open(CELERY_FLOWER_CONFIG, 'r') as jp:
        flower_config = json.load(jp)
        celery_url = '{0}/api/workers'.format(flower_config.get('flower_url'))
        flower_user = flower_config.get('flower_user')
        flower_pass = flower_config.get('flower_pass')
    else:
      ## support for legacy config
      celery_url = '{0}/api/workers'.format(CELERY_FLOWER_BASE_URL)
      celery_basic_auth = os.environ.get('AIRFLOW__CELERY__FLOWER_BASIC_AUTH')
      if celery_basic_auth is None:
        raise ValueError('Missing env for flower basic auth')
      flower_user, flower_pass = celery_basic_auth.split(':')
    res = requests.get(celery_url, auth=HTTPBasicAuth(flower_user, flower_pass))
    if res.status_code != 200:
      raise ValueError('Failed to fetch celery workers')
    data = res.content.decode()
    data = json.loads(data)
    worker_list = list()
    for worker_id, val in data.items():
      worker_list.append({
        'worker_id': worker_id, 
        'active_jobs': len(val.get('active')), 
        'queue_lists': [i.get('name') for i in val.get('active_queues')]})
    ti.xcom_push(key=celery_worker_key,value=worker_list)
  except Exception as e:
    logging.error('Failed to get celery workers, error: {0}'.format(e))
    raise


def stop_celery_workers(**context):
  """
  A function for stopping celery workers
  """
  try:
    ti = context.get('ti')
    empty_celery_worker_key = context['params'].get('empty_celery_worker_key')
    if CELERY_FLOWER_CONFIG is not None:
      ## new config file
      if not os.path.exists(CELERY_FLOWER_CONFIG):
        raise IOError("Celery flower config file not found")
      with open(CELERY_FLOWER_CONFIG, 'r') as jp:
        flower_config = json.load(jp)
        celery_url = flower_config.get('flower_url')
        flower_user = flower_config.get('flower_user')
        flower_pass = flower_config.get('flower_pass')
    else:
      ## support for legacy config
      celery_url = '{0}/api/workers'.format(CELERY_FLOWER_BASE_URL)
      celery_basic_auth = os.environ.get('AIRFLOW__CELERY__FLOWER_BASIC_AUTH')
      if celery_basic_auth is None:
        raise ValueError('Missing env for flower basic auth')
      flower_user, flower_pass = celery_basic_auth.split(':')
    # celery_basic_auth = os.environ['AIRFLOW__CELERY__FLOWER_BASIC_AUTH']
    # flower_user, flower_pass = celery_basic_auth.split(':')
    celery_workers = \
		  ti.xcom_pull(
        task_ids='calculate_new_worker_size_and_branch',
        key=empty_celery_worker_key)
    for worker_id in celery_workers:
      # flower_shutdown_url = \
      #   '{0}/api/worker/shutdown/{1}'.\
      #     format(CELERY_FLOWER_BASE_URL, worker_id)
      flower_shutdown_url = \
        f'{celery_url}/api/worker/shutdown/{worker_id}'
      res = requests.post(
              flower_shutdown_url,
              auth=HTTPBasicAuth(flower_user, flower_pass))
      if res.status_code != 200:
        raise ValueError('Failed to delete worker {0}'.\
          format(worker_id))
  except Exception as e:
    logging.error('Failed to stop celery workers, error: {0}'.format(e))
    raise


with dag:
  ## TASK
  fetch_queue_list_from_redis = \
    PythonOperator(
      task_id='fetch_queue_list_from_redis',
      dag=dag,
      python_callable=airflow_utils_for_redis,
      op_kwargs={"redis_conf_file":Variable.get('redis_conn_file')},
      pool='generic_pool',
      queue='generic')
  ## TASK
  check_hpc_queue = \
    SSHOperator(
      task_id='check_hpc_queue',
      ssh_hook=hpc_hook,
      dag=dag,
      command='source /etc/bashrc;qstat',
      pool='generic_pool',
      queue='generic')
  ## TASK
  fetch_active_jobs_from_hpc = \
    SSHOperator(
      task_id='fetch_active_jobs_from_hpc',
      ssh_hook=hpc_hook,
      dag=dag,
      pool='generic_pool',
      command="""
        source /etc/bashrc;\
        source /project/tgu/data2/airflow_v2/secrets/hpc_env.sh;\
        python /project/tgu/data2/airflow_v2/github/data-management-python/scripts/hpc/count_active_jobs_in_hpc.py """,
      do_xcom_push=True,
      queue='generic')
  ## TASK
  fetch_celery_workers = \
    PythonOperator(
      task_id='fetch_celery_workers',
      dag=dag,
      pool='generic_pool',
      queue='generic',
      python_callable=fetch_celery_worker_list,
      params={'celery_worker_key':'celery_workers'}
    )
  ## TASK
  calculate_new_worker_size_and_branch = \
    BranchPythonOperator(
      task_id='calculate_new_worker_size_and_branch',
      dag=dag,
      python_callable=get_new_workers,
      queue='generic',
      pool='generic_pool',
      params={'celery_worker_key':'celery_workers',
              'empty_celery_worker_key':'empty_celery_worker',
              'base_queue':'generic'})
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
      queue='generic',
      pool='generic_pool',
      command="""
      {% if ti.xcom_pull(key=params.job_name,task_ids="calculate_new_worker_size_and_branch" ) > 1 %}
        source /etc/bashrc; \
        qsub \
          -o /dev/null \
          -e /dev/null \
          -k n -m n \
          -N {{ params.job_name }} \
          -J 1-{{ ti.xcom_pull(key=params.job_name,task_ids="calculate_new_worker_size_and_branch" ) }}  {{ params.pbs_resource }} -- \
            /project/tgu/data2/airflow_v2/github/data-management-python/scripts/hpc/airflow_worker.sh {{  params.airflow_queue }} {{ params.job_name }}
      {% else %}
        source /etc/bashrc;\
        qsub \
          -o /dev/null \
          -e /dev/null \
          -k n -m n \
          -N {{ params.job_name }} {{ params.pbs_resource }} -- \
            /project/tgu/data2/airflow_v2/github/data-management-python/scripts/hpc/airflow_worker.sh {{  params.airflow_queue }} {{ params.job_name }}
      {% endif %}
      """,
      params={'pbs_resource':pbs_resource,
              'airflow_queue':airflow_queue,
              'job_name':q})
    queue_tasks.\
      append(t)

  ## TASK
  cleanup_celery_workers = \
    PythonOperator(
      task_id='cleanup_celery_workers',
      dag=dag,
      queue='generic',
      pool='generic_pool',
      params={'empty_celery_worker_key':'empty_celery_worker'},
      python_callable=stop_celery_workers)
  ## PIPELINE
  check_hpc_queue >> fetch_active_jobs_from_hpc
  calculate_new_worker_size_and_branch << \
    [fetch_queue_list_from_redis,
     fetch_active_jobs_from_hpc,
     fetch_celery_workers]
  calculate_new_worker_size_and_branch >> queue_tasks
  calculate_new_worker_size_and_branch >> cleanup_celery_workers