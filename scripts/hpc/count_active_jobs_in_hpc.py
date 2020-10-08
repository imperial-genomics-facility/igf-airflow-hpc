import json
from igf_airflow.hpc_queue import get_pbspro_job_count

if __name__=='__main__':
  hpc_jobs = get_pbspro_job_count(job_name_prefix='')
  hpc_jobs = json.dump(hpc_jobs)
  print(hpc_jobs)