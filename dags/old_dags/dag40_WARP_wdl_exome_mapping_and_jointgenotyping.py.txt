import os
import json
import pendulum
import logging
import shutil
import pandas as pd
from airflow import XComArg
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from igf_data.utils.bashutils import bash_script_wrapper
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from igf_airflow.utils.generic_airflow_utils import (
    send_airflow_failed_logs_to_channels,
    send_airflow_pipeline_logs_to_channels)
from igf_data.utils.fileutils import (
  check_file_path,
  copy_local_file,
  get_temp_dir,
  get_date_stamp)
from igf_airflow.utils.dag22_bclconvert_demult_utils import (
  _create_output_from_jinja_template)
from igf_airflow.utils.dag33_geomx_processing_util import (
	mark_analysis_running,
	no_work,
    fetch_analysis_design_from_db,
    copy_data_to_globus,
    send_email_to_user,
    mark_analysis_finished,
	mark_analysis_failed)

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)

## DAG
DAG_ID = \
    os.path.basename(__file__).\
        replace(".pyc", "").\
        replace(".py", "")

@dag(
  dag_id=DAG_ID,
  schedule=None,
  start_date=datetime(2024, 4, 1),#pendulum.yesterday(),
  catchup=True,
  max_active_runs=1,
  default_view='grid',
  orientation='TB',
  tags=["wdl", "exome", "warp", "hpc"])
def dag40_WARP_wdl_exome_mapping_and_jointgenotyping():
  ## TASK 1: mark analysis as running
  analysis_running = \
    mark_analysis_running(
      next_task="fetch_analysis_design",
      last_task="no_work")
  ## TASK 2: fetch analysis design from db
  sample_group_info = \
    fetch_analysis_design_from_db()
  ## TASK 3: create input json for input design
  ## TO DO
  json_input = \
    create_json_input_for_analysis_design(sample_group_info)
  ## TASK 4: read input json and return as map for dynamic task
  rg_umabs = \
    read_input_json_for_ubam_conversion(json_input)
  
  # LAST TASKS
  # copy_globus = \
  # copy_data_to_globus(loaded_files_info)
  # copy_globus >> send_email_to_user() >> mark_analysis_finished() >> mark_analysis_failed()