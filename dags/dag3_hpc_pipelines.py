from datetime import timedelta

from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = \
  DAG(
    dag_id='dag3_hpc_pipelines',
    catchup=False,
    schedule_interval=None,
    max_active_runs=1,
    tags=['hpc'],
    default_args=default_args)

with dag:
  run_demultiplexing_pipeline = \
    BashOperator(
      task_id='run_demultiplexing_pipeline',
      dag=dag,
      queue='hpc_4G',
      bash_command='bash /rds/general/user/igf/home/git_repo/IGF-cron-scripts/hpc/run_demultiplexing_pipeline.sh '
    )

  run_primary_analysis_pipeline = \
    BashOperator(
      task_id='run_primary_analysis_pipeline',
      dag=dag,
      queue='hpc_4G',
      bash_command='bash /rds/general/user/igf/home/git_repo/IGF-cron-scripts/hpc/run_primary_analysis_pipeline.sh '
    )

  run_demultiplexing_pipeline >> run_primary_analysis_pipeline