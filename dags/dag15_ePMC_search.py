import pendulum
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from igf_airflow.utils.dag15_ePMC_search_utils import update_wiki_publication_page_func

# args = {
#     'owner': 'airflow',
#     'start_date': pendulum.today('UTC').add(days=2),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'provide_context': True,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'catchup': False,
#     'max_active_runs': 1,
# }

dag = \
  DAG(
    dag_id='dag15_ePMC_search',
    schedule='@monthly',
    max_active_runs=1,
    catchup=False,
    start_date=pendulum.yesterday(),
    dagrun_timeout=timedelta(minutes=15),
    owner='airflow',
    tags=['hpc'])

with dag:
  ## TASK
  update_wiki_publication_page = \
    PythonOperator(
      task_id='update_wiki_publication_page',
      dag=dag,
      retry_delay=timedelta(minutes=5),
      retries=1,
      queue='hpc_4G',
      python_callable=update_wiki_publication_page_func)
  ## PUBLICATION
  update_wiki_publication_page