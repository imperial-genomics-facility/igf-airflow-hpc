#!/bin/bash

job_queue=${1:?'Missing job queue'}
job_name=${1:?'Missing job name'}

source /path/hpc_env.sh

airflow worker --pid $TMPDIR -cn hpc-${PBS_JOBID}-${job_name} -q ${job_queue}
