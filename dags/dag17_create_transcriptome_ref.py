import os
import pendulum
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import download_gtf_file_func
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import create_star_index_func
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import create_rsem_index_func
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import create_reflat_index_func
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import create_ribosomal_interval_func
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import create_cellranger_ref_func
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import add_refs_to_db_collection_func


# args = {
#     'owner': 'airflow',
#     'start_date': pendulum.today('UTC').add(days=2),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'provide_context': True,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'catchup': False,
#     'max_active_runs': 1}
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")
dag = \
    DAG(
        dag_id=DAG_ID,
        schedule=None,
        max_active_runs=1,
        catchup=False,
        start_date=pendulum.yesterday(),
        tags=['hpc'])
with dag:
    ## TASK
    download_gtf_file = \
        PythonOperator(
            task_id="download_gtf_file",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'gtf_xcom_key': 'gtf_file'
            },
            python_callable=download_gtf_file_func)
    ## TASK
    create_star_index = \
        PythonOperator(
            task_id="create_star_index",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_42G16t',
            params={
                'gtf_xcom_task': 'download_gtf_file',
                'gtf_xcom_key': 'gtf_file',
                'star_ref_xcom_key': 'star_ref',
                'threads': 12,
                'star_options': [
                    '--sjdbOverhang', 149]
            },
            python_callable=create_star_index_func)
    ## TASK
    create_rsem_index = \
        PythonOperator(
            task_id="create_rsem_index",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'gtf_xcom_task': 'download_gtf_file',
                'gtf_xcom_key': 'gtf_file',
                'rsem_ref_xcom_key': 'rsem_ref',
                'threads': 8
            },
            python_callable=create_rsem_index_func)
    ## TASK
    create_reflat_index = \
        PythonOperator(
            task_id='create_reflat_index',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'gtf_xcom_task': 'download_gtf_file',
                'gtf_xcom_key': 'gtf_file',
                'refflat_ref_xcom_key': 'refflat_ref'
            },
            python_callable=create_reflat_index_func)
    ## TASK
    create_ribosomal_interval = \
        PythonOperator(
            task_id='create_ribosomal_interval',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'gtf_xcom_task': 'download_gtf_file',
                'gtf_xcom_key': 'gtf_file',
                'ribosomal_ref_xcom_key': 'ribosomal_ref',
                'skip_gtf_rows': 5
            },
            python_callable=create_ribosomal_interval_func)
    ## TASK
    create_cellranger_ref = \
        PythonOperator(
            task_id='create_cellranger_ref',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_32G8t',
            params={
                'gtf_xcom_task': 'download_gtf_file',
                'gtf_xcom_key': 'gtf_file',
                'cellranger_ref_xcom_key': 'cellranger_ref',
                'skip_gtf_rows': 5,
                'threads': 7,
                'memory': 30
            },
            python_callable=create_cellranger_ref_func)
    ## TASK
    add_refs_to_db_collection = \
        PythonOperator(
            task_id='add_refs_to_db_collection',
            dag=dag,
            retry_delay=timedelta(minutes=5),
            retries=1,
            queue='hpc_4G',
            params={
                'task_list': [
                    ['download_gtf_file', 'gtf_file', 'GENE_GTF'],
                    ['create_star_index', 'star_ref', 'TRANSCRIPTOME_STAR'],
                    ['create_rsem_index', 'rsem_ref', 'TRANSCRIPTOME_RSEM'],
                    ['create_reflat_index', 'refflat_ref', 'GENE_REFFLAT'],
                    ['create_ribosomal_interval', 'ribosomal_ref', 'RIBOSOMAL_INTERVAL'],
                    ['create_cellranger_ref', 'cellranger_ref', 'TRANSCRIPTOME_TENX']
                ]
            },
            python_callable=add_refs_to_db_collection_func)
    ## PIPELINE
    download_gtf_file >> create_star_index >> add_refs_to_db_collection
    download_gtf_file >> create_rsem_index >> add_refs_to_db_collection
    download_gtf_file >> create_reflat_index >> add_refs_to_db_collection
    download_gtf_file >> create_ribosomal_interval >> add_refs_to_db_collection
    download_gtf_file >> create_cellranger_ref >> add_refs_to_db_collection