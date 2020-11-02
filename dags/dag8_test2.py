from datetime import timedelta

from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = \
  DAG(
    dag_id='dag8_test2',
    catchup=False,
    schedule_interval="0 */2 * * *",
    max_active_runs=1,
    tags=['orwell','hpc'],
    default_args=default_args)

orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host='orwell.hh.med.ic.ac.uk')

with dag:
  generate_file_list = \
    SSHOperator(
      task_id = 'generate_file_list',
      dag = dag,
      ssh_hook = orwell_ssh_hook,
      queue='hpc_4G',
      command = 'python /home/igf/igf_code/t1.py')
  branch_to = \
    BranchPythonOperator(
      task_id='calculate_new_worker_size_and_branch',
      dag=dag,
      python_callable=lambda: ['copy_files_to_hpc1',],
      queue='hpc_4G')
  copy_files_to_hpc1 = \
    BashOperator(
      task_id = 'copy_files_to_hpc1',
      dag = dag,
      queue='hpc_4G',
      bash_command = 'bash /project/tgu/data2/airflow_test/github/t1.sh ')
  copy_files_to_hpc2 = \
    BashOperator(
      task_id = 'copy_files_to_hpc2',
      dag = dag,
      queue='hpc_4G',
      bash_command = 'bash /project/tgu/data2/airflow_test/github/t2.sh ')
  generate_file_list >> branch_to >> [copy_files_to_hpc1,copy_files_to_hpc2]