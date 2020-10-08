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

#hpc_hook = SSHHook(
#      remote_host=Variable.get('hpc_host'),
#      username=Variable.get('hpc_user'),
#      key_file=Variable.get('igf_lims_ssh_key_file')
#    )
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
    active_tasks = ti.xcom_pull(task_ids='qstat_on_remote')
    active_tasks = active_tasks.decode()
    active_tasks = json.loads(active_tasks)
    queued_tasks = ti.xcom_pull(task_ids='fetch_queue_list_from_redis')
    #print(queued_tasks,active_tasks)
    worker_to_submit,unique_queue_list = \
      calculate_new_workers(
        queue_list=queued_tasks,
        active_jobs_dict=active_tasks)
    print(worker_to_submit,unique_queue_list)
    for key,value in worker_to_submit.items():
      ti.xcom_push(key=key,value=value)

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
      command='/project/tgu/data2/airflow_test/github/igf-airflow-hpc;python scripts/hpc/count_active_jobs_in_hpc.py',
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
  """
  1Gb_worker = \
    BashOperator(
      task_id='1Gb',
      dag=dag,
      queue='igf-lims',
      bash_command='echo "{{ ti.xcom_pull(key="1Gb",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  1Gb4t_worker = \
    BashOperator(
      task_id='1Gb4t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="1Gb4t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  2Gb4t_worker = \
    BashOperator(
      task_id='2Gb4t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="2Gb4t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  2Gb72hr_worker = \
    BashOperator(
      task_id='2Gb72hr',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="2Gb72hr",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  4Gb_worker = \
    BashOperator(
      task_id='4Gb',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="4Gb",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  4Gb8t_worker = \
    BashOperator(
      task_id='4Gb8t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="4Gb8t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  4Gb16t_worker = \
    BashOperator(
      task_id='4Gb16t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="4Gb16t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  8Gb_worker = \
    BashOperator(
      task_id='8Gb',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="8Gb",task_ids="calculate_new_worker_size_and_branch") }}"'
    )
  
  8Gb8t_worker = \
    BashOperator(
      task_id='8Gb8t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="8Gb8t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  8Gb16t_worker = \
    BashOperator(
      task_id='8Gb16t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="8Gb16t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

    16Gb_worker = \
    BashOperator(
      task_id='16Gb',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="16Gb",task_ids="calculate_new_worker_size_and_branch") }}"'
    )
  
  16Gb8t_worker = \
    BashOperator(
      task_id='16Gb8t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="16Gb8t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  16Gb16t_worker = \
    BashOperator(
      task_id='16Gb16t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="16Gb16t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )
  
  32Gb8t_worker = \
    BashOperator(
      task_id='32Gb8t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="32Gb8t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )

  32Gb16t_worker = \
    BashOperator(
      task_id='32Gb16t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="32Gb16t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )
  
  42Gb16t_worker = \
    BashOperator(
      task_id='42Gb16t',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="42Gb16t",task_ids="calculate_new_worker_size_and_branch") }}"'
    )
  
  64Gb16t48hr_worker = \
    BashOperator(
      task_id='64Gb16t48hr',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="64Gb16t48hr",task_ids="calculate_new_worker_size_and_branch") }}"'
    )
  
  128Gb16t72hr_worker = \
    BashOperator(
      task_id='128Gb16t72hr',
      dag=dag,
      queue='igf_lims_queue',
      bash_command='echo "{{ ti.xcom_pull(key="128Gb16t72hr",task_ids="calculate_new_worker_size_and_branch") }}"'
    )
  """
  check_hpc_queue >> fetch_active_jobs_from_hpc
  calculate_new_worker_size_and_branch << [fetch_queue_list_from_redis,fetch_active_jobs_from_hpc]
  """
  fetch_active_jobs_from_hpc >> [1Gb_worker,
                                 1Gb4t_worker,
                                 2Gb4t_worker,
                                 2Gb72hr_worker,
                                 4Gb_worker,
                                 4Gb8t_worker,
                                 4Gb16t_worker,
                                 8Gb_worker,
                                 8Gb8t_worker,
                                 8Gb16t_worker,
                                 16Gb_worker,
                                 16Gb8t_worker,
                                 16Gb16t_worker,
                                 32Gb8t_worker,
                                 32Gb16t_worker,
                                 42Gb16t_worker,
                                 64Gb16t48hr_worker,
                                 128Gb16t72hr_worker
                                ]
  """