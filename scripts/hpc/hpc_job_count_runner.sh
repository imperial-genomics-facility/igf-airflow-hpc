#!/bin/bash

module load anaconda3/personal
source activate airflow1.10.12

export PYTHONPATH=${PYTHONPATH}:/project/tgu/data2/airflow_test/github/igf-airflow-hpc

python /project/tgu/data2/airflow_test/github/igf-airflow-hpc/scripts/hpc/count_active_jobs_in_hpc.py
