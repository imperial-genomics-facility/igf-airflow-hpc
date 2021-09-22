from datetime import timedelta
from airflow.models import DAG,Variable
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
#from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import check_and_transfer_run_func
#from igf_airflow.utils.dag14_crick_seqrun_transfer_utils import extract_tar_file_func

FTP_SEQRUN_SERVER = Variable.get('crick_ftp_seqrun_hostname')
FTP_CONFIG_FILE = Variable.get('crick_ftp_config_file_wells')
SEQRUN_BASE_PATH = Variable.get('seqrun_base_path')
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path')

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 1,
}

## SSH HOOK
orwell_ssh_hook = \
  SSHHook(
    key_file=Variable.get('hpc_ssh_key_file'),
    username=Variable.get('hpc_user'),
    remote_host=Variable.get('orwell_server_hostname'))

dag = \
  DAG(
    dag_id='dag14_crick_seqrun_transfer',
    schedule_interval=None,
    default_args=args,
    tags=['ftp', 'hpc', 'orwell', 'wells'])


with dag:
  ## TASK
  # not working on HPC
  #check_and_transfer_run = \
  #  PythonOperator(
  #    task_id='check_and_transfer_run',
  #    dag=dag,
  #    pool='crick_ftp_pool',
  #    queue='hpc_4G',
  #    params={'ftp_seqrun_server': FTP_SEQRUN_SERVER,
  #           'hpc_seqrun_base_path': HPC_SEQRUN_BASE_PATH,
  #           'ftp_config_file': FTP_CONFIG_FILE},
  #    python_callable=check_and_transfer_run_func)
  ## TASK
  #extract_tar_file = \
  #  PythonOperator(
  #    task_id='extract_tar_file',
  #    dag=dag,
  #    queue='hpc_4G',
  #    params={'hpc_seqrun_base_path': HPC_SEQRUN_BASE_PATH},
  #    python_callable=extract_tar_file_func)
  check_and_transfer_run = \
    SSHOperator(
      task_id='check_and_transfer_run',
      dag=dag,
      pool='crick_ftp_pool',
      ssh_hook=orwell_ssh_hook,
      do_xcom_push=False,
      queue='wells',
      params={'ftp_seqrun_server': FTP_SEQRUN_SERVER,
              'seqrun_base_path': HPC_SEQRUN_BASE_PATH,
              'ftp_config_file': FTP_CONFIG_FILE},
      command="""
        python /rds/general/user/igf/home/data2/airflow_test/github/data-management-python/scripts/ftp_seqrun_transfer/transfer_seqrun_from_crick.py \
          -f {{ params.ftp_seqrun_server }} \
          -s {{ dag_run.conf["seqrun_id"] }} \
          -d {{ params.seqrun_base_path }} \
          -c {{ params.ftp_config_file }}
        """)
  # TASK
  extract_tar_file = \
    SSHOperator(
      task_id='extract_tar_file',
      dag=dag,
      do_xcom_push=False,
      queue='wells',
      params={'seqrun_base_path': HPC_SEQRUN_BASE_PATH},
      command="""
      cd {{ params.seqrun_base_path }};
      if [ -d temp_{{ dag_run.conf["seqrun_id"] }} ];
      then
        echo "Seqrun dir exists";
        exit 1;
      else
        mkdir -p temp_{{ dag_run.conf["seqrun_id"] }};
        tar \
          --no-same-owner \
          --no-same-permissions \
          --owner=igf \
          -xzf {{ dag_run.conf["seqrun_id"] }}.tar.gz \
          -C temp_{{ dag_run.conf["seqrun_id"] }};
        find temp_{{ dag_run.conf["seqrun_id"] }} \
          -type d \
          -exec chmod 700 {} \;
        chmod -R u+r temp_{{ dag_run.conf["seqrun_id"] }};
        chmod -R u+w temp_{{ dag_run.conf["seqrun_id"] }};
      fi
      """
    )
  ## TASK
  move_seqrun_dir = \
    SSHOperator(
      task_id='move_seqrun_dir',
      dag=dag,
      pool='orwell_exe_pool',
      ssh_hook=orwell_ssh_hook,
      do_xcom_push=False,
      queue='hpc_4G',
      params={'seqrun_base_path': SEQRUN_BASE_PATH},
      command="""
        cd {{ params.seqrun_base_path }};
        if [ -d {{ dag_run.conf["seqrun_id"] }} ];
        then
          echo "Seqrun dir exists";
          exit 1;
        fi
        ls temp_{{ dag_run.conf["seqrun_id"] }}/camp/stp/sequencing/inputs/instruments/sequencers/{{ dag_run.conf["seqrun_id"] }};
        move temp_{{ dag_run.conf["seqrun_id"] }}/camp/stp/sequencing/inputs/instruments/sequencers/{{ dag_run.conf["seqrun_id"] }} {{ params.seqrun_base_path }}/{{ dag_run.conf["seqrun_id"] }};
      """
    )
  ## PIPELINE
  check_and_transfer_run >> extract_tar_file >> move_seqrun_dir
