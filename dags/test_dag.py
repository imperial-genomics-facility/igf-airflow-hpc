from airflow.models import DAG,Variable
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner':'airflow',
    'start_date':days_ago(2),
    'provide_context': True,
}

dag = DAG(
        dag_id='test_dag',
        schedule_interval=None,
        default_args=args,
        tags=['test','hpc',]
      )

with dag:
  task1 = \
    BashOperator(
      task_id='task1',
      dag=dag,
      bash_command='hostname -A',
      queue='hpc_4G',
      xcom_push=True
    )
  task2 = \
    BashOperator(
      task_id='task2',
      dag=dag,
      bash_command='hostname -A',
      queue='hpc_1G',
      xcom_push=True
    )
  task1 >> task2