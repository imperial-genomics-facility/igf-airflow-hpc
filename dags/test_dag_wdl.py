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

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('analysis_slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('analysis_ms_teams_conf',default_var=None)

JSON_INPUT = "/rds/general/project/genomics-facility-archive-2019/live/adatta17/test_dir/test146_exome_wdl/wdl_fastq_to_ubam_input.json"

SAMPLE_INPUT_TEMPLATE = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/wdl_templates/sample_input.json"
WDL_CMD_TEMPLATE = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/wdl_templates/exome_cmd.sh"

FINAL_WORK_DIR = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/merged_dir"

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
	tags=["wdl", "test", "hpc"],
)
def test_dag_wdl():
    rg_groups = read_rg_list()
    ubam_list = collect_ubams()
    ubams = fastq_to_ubam.expand(fastq_entry=rg_groups)
    ubams >> ubam_list
    grp_gvcf_list = wdl_tg.expand(ubam_entry=ubam_list)
    gvcf_entry = \
        collect_gvcf_and_prepare_joinGenotype_input(grp_gvcf_list)



@task_group
def wdl_tg(ubam_entry: dict) -> None:
    wdl_prep = fromat_wdl_run(ubam_entry=ubam_entry)
    wdl_out = run_wdl(wdls_entry=wdl_prep)
    output_entry = get_vcf_and_cleanup_input(work_dir=wdl_out)
    return output_entry


## TASK
@task(
  task_id="read_rg_list",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def read_rg_list() -> list:
    try:
        with open(JSON_INPUT, 'r') as fp:
            json_data = json.load(fp)
        return json_data
    except Exception as e:
        log.error(e)
        send_airflow_failed_logs_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            message_prefix=e)
        raise ValueError(e)

## TASK
@task(
  task_id="collect_ubams",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def collect_ubams() -> dict:
    try:
        ubam_output_list = list()
        context = get_current_context()
        ti = context.get('ti')
        all_lazy_task_ids = \
            context['task'].\
                get_direct_relative_ids(upstream=True)
        lazy_xcom = ti.xcom_pull(task_ids=all_lazy_task_ids)
        for entry in lazy_xcom:
            unmapped_bam = entry.get("unmapped_bam")
            sample_name = entry.get("sample_name")
            if unmapped_bam is not None and \
               sample_name is not None:
                ubam_output_list.append({
                    "sample_name": sample_name,
                    "unmapped_bam": unmapped_bam})
        per_sample_ubams = \
            pd.DataFrame(ubam_output_list).\
            groupby('sample_name', as_index=False).\
            agg({"unmapped_bam": list}).\
            to_dict(orient="records")
        return per_sample_ubams
    except Exception as e:
        log.error(e)
        send_airflow_failed_logs_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            message_prefix=e)
        raise ValueError(e)

## TASK
@task(
  task_id="fastq_to_ubam",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_16G4t',
  multiple_outputs=False)
def fastq_to_ubam(fastq_entry: dict) -> dict:
    try:
        temp_dir = get_temp_dir(use_ephemeral_space=True)
        gatk_path = "/rds/general/project/genomics-facility-archive-2019/ephemeral/cromwell_test/tools/gatk-4.5.0.0/gatk"
        fastq_1 = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.fastq_1")
        fastq_2 = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.fastq_2")
        readgroup_name = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.readgroup_name")
        sample_name = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.sample_name")
        library_name = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.library_name")
        platform_unit = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.platform_unit")
        run_date = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.run_date")
        platform_name = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.platform_name")
        sequencing_center = fastq_entry.get("ConvertPairedFastQsToUnmappedBamWf.sequencing_center")
        mem = 4
        unmapped_bam = os.path.join(temp_dir, f"{readgroup_name}.unmapped.bam")
        command_template = f"""module load anaconda3/personal;
            source activate java;
            taskset -a -c 0,1 {gatk_path} \
            --java-options "-XX:ParallelGCThreads=1 -Djava.io.tmpdir=$EPHEMERAL -DGATK_STACKTRACE_ON_USER_EXCEPTION=true -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=1 -Xmx{mem}g" \
            FastqToSam \
            --FASTQ {fastq_1} \
            --FASTQ2 {fastq_2} \
            --OUTPUT {unmapped_bam} \
            --READ_GROUP_NAME {readgroup_name} \
            --SAMPLE_NAME {sample_name} \
            --LIBRARY_NAME {library_name} \
            --PLATFORM_UNIT {platform_unit} \
            --RUN_DATE {run_date} \
            --PLATFORM {platform_name} \
            --SEQUENCING_CENTER {sequencing_center}"""
        run_script = os.path.join(temp_dir, "cmd.sh")
        with open(run_script, 'w') as fp:
            fp.write(command_template)
        stdout_file = None
        stderr_file = None
        try:
            stdout_file, stderr_file = \
                bash_script_wrapper(
                    script_path=run_script)
        except Exception as e:
            raise ValueError(
                f"Failed to run script, Script: {run_script}, error file: {stderr_file}")
        return {"sample_name": sample_name, "unmapped_bam": unmapped_bam}
    except Exception as e:
        log.error(e)
        send_airflow_failed_logs_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            message_prefix=e)
        raise ValueError(e)

## TASK
@task(
  task_id="fromat_wdl_run",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',
  multiple_outputs=False)
def fromat_wdl_run(ubam_entry: dict) -> dict:
    try:
        sample_name = ubam_entry.get("sample_name")
        unmapped_bam = ubam_entry.get("unmapped_bam")
        work_dir = get_temp_dir(use_ephemeral_space=True)
        input_json_file = \
            os.path.join(
                work_dir,
                os.path.basename(SAMPLE_INPUT_TEMPLATE))
        _create_output_from_jinja_template(
            template_file=SAMPLE_INPUT_TEMPLATE,
            output_file=input_json_file,
            autoescape_list=['xml',],
            data=dict(
                SAMPLE_NAME=sample_name,
                UNMAPPED_BAMS=unmapped_bam))
        wdl_cmd_file = \
            os.path.join(
                work_dir,
                os.path.basename(WDL_CMD_TEMPLATE))
        _create_output_from_jinja_template(
            template_file=WDL_CMD_TEMPLATE,
            output_file=wdl_cmd_file,
            autoescape_list=['xml',],
            data=dict(
                SAMPLE_NAME=sample_name,
                SAMPLE_INPUT_JSON=input_json_file,
                WORK_DIR=work_dir))
        return {"work_dir": work_dir, "script_file": wdl_cmd_file}
    except Exception as e:
        log.error(e)
        send_airflow_failed_logs_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            message_prefix=e)
        raise ValueError(e)

## TASK
@task(
  task_id="run_wdl",
  retry_delay=timedelta(minutes=5),
  retries=20,
  queue='hpc_32G16t',
  pool='batch_job',
  multiple_outputs=False)
def run_wdl(wdls_entry: dict) -> dict:
    try:
        work_dir = wdls_entry.get("work_dir")
        script_file = wdls_entry.get("script_file")
        stdout_file = None
        stderr_file = None
        try:
            send_airflow_pipeline_logs_to_channels(
                slack_conf=SLACK_CONF,
                ms_teams_conf=MS_TEAMS_CONF,
                message_prefix=f"Started WDL run. Script: {script_file}, Work dir: {work_dir}")
            stdout_file, stderr_file = \
                bash_script_wrapper(
                    script_path=script_file)
            send_airflow_pipeline_logs_to_channels(
                slack_conf=SLACK_CONF,
                ms_teams_conf=MS_TEAMS_CONF,
                message_prefix=f"Finished WDL run. Work dir: {work_dir}")
        except Exception as e:
            raise ValueError(
                f"Failed to run script, Script: {script_file}, error file: {stderr_file}")
        return work_dir
    except Exception as e:
        log.error(e)
        send_airflow_failed_logs_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            message_prefix=e)
        raise ValueError(e)

## TASK
@task(
  task_id="get_vcf_and_cleanup_input",
  retry_delay=timedelta(minutes=5),
  retries=20,
  queue='hpc_4G',
  multiple_outputs=False)
def get_vcf_and_cleanup_input(work_dir: str) -> dict:
    try:
        ## failsafe for re-run
        check_file_path(work_dir)
        ## read metadata json
        metadata_json = os.path.join(work_dir, "metadata_output.json")
        with open(metadata_json, "r") as fp:
            json_data = json.load(fp)
        ## set output dict
        output_files = \
            dict(
                sample_name=json_data["inputs"]["sample_and_unmapped_bams"]["sample_name"])
        ## get a new temp dir
        new_work_dir = get_temp_dir(use_ephemeral_space=True)
        ## copy metadata json
        src_path = os.path.join(work_dir, "metadata_output.json")
        dest_path = os.path.join(new_work_dir, "metadata_output.json")
        copy_local_file(src_path, dest_path)
        ## set required files
        required_files = [
            'ExomeGermlineSingleSample.output_bqsr_reports',
            'ExomeGermlineSingleSample.gvcf_summary_metrics',
            'ExomeGermlineSingleSample.output_cram_md5',
            'ExomeGermlineSingleSample.gvcf_detail_metrics',
            'ExomeGermlineSingleSample.hybrid_selection_metrics',
            'ExomeGermlineSingleSample.output_cram_index',
            'ExomeGermlineSingleSample.read_group_alignment_summary_metrics',
            'ExomeGermlineSingleSample.agg_quality_distribution_metrics',
            'ExomeGermlineSingleSample.agg_pre_adapter_detail_metrics',
            'ExomeGermlineSingleSample.output_cram',
            'ExomeGermlineSingleSample.agg_insert_size_metrics',
            'ExomeGermlineSingleSample.agg_alignment_summary_metrics',
            'ExomeGermlineSingleSample.output_vcf_index',
            'ExomeGermlineSingleSample.duplicate_metrics',
            'ExomeGermlineSingleSample.fingerprint_detail_metrics',
            'ExomeGermlineSingleSample.agg_bait_bias_summary_metrics',
            'ExomeGermlineSingleSample.agg_bait_bias_detail_metrics',
            'ExomeGermlineSingleSample.agg_insert_size_histogram_pdf',
            'ExomeGermlineSingleSample.output_vcf',
            'ExomeGermlineSingleSample.agg_error_summary_metrics',
            'ExomeGermlineSingleSample.fingerprint_summary_metrics',
            'ExomeGermlineSingleSample.agg_pre_adapter_summary_metrics',
            'ExomeGermlineSingleSample.validate_cram_file_report',
            'ExomeGermlineSingleSample.calculate_read_group_checksum_md5',
            'ExomeGermlineSingleSample.agg_quality_distribution_pdf']
        ## go through list of output files and copy required files to the new dir
        for file_key, file_path in json_data['outputs'].items():
            ## get reuired files and skip lists
            if file_key in required_files and \
               isinstance(file_path, str):
                dest_path = \
                    os.path.join(
                        new_work_dir,
                        os.path.basename(file_path))
                copy_local_file(file_path, dest_path)
                output_files.update({file_key: dest_path})
        ## dump json file
        output_json_file = \
            os.path.join(
                new_work_dir,
                "new_metadata_output.json")
        with open(output_json_file, 'w') as fp:
            json.dump(output_files, fp)
        ## cleanup
        if len(output_files) == 1:
            raise ValueError(f"No required file found in path {work_dir}")
        shutil.rmtree(work_dir, ignore_errors=True)
        return {"metadata_file": output_json_file, "output_dir": new_work_dir}
    except Exception as e:
        log.error(e)
        send_airflow_failed_logs_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            message_prefix=e)
        raise ValueError(e)

## TASK
@task(
  task_id="collect_gvcf_and_prepare_joinGenotype_input",
  retry_delay=timedelta(minutes=5),
  retries=2,
  queue='hpc_4G',
  multiple_outputs=False)
def collect_gvcf_and_prepare_joinGenotype_input(gvcf_list: list) -> dict:
    try:
        new_gvcf_list = list()
        os.makedirs(FINAL_WORK_DIR, exist_ok=True)
        for entry in gvcf_list:
            metadata_file = entry.get("metadata_file")
            output_dir = entry.get("output_dir")
            with open(metadata_file, 'r') as fp:
                json_data = json.load(fp)
            if isinstance(json_data, list):
                json_data = json_data[0]
            sample_name = json_data.get("sample_name")
            sample_dir = os.path.join(FINAL_WORK_DIR, sample_name)
            if os.path.exists(sample_dir):
                raise ValueError(f"Cleanup path {sample_dir} before copy gvcf.")
            shutil.copytree(output_dir, sample_dir)
            gvcf_file = json_data.get("ExomeGermlineSingleSample.output_vcf")
            new_gvcf_path = \
                os.path.join(sample_dir, os.path.basename(gvcf_file))
            new_gvcf_list.append({"sample": sample_name, "gvcf": new_gvcf_path})
        df = pd.DataFrame(new_gvcf_list)
        df.to_csv(
            os.path.join(FINAL_WORK_DIR, "gvcf_list"),
            sep="\t",
            header=False,
            index=False)
        return {"output_dir": FINAL_WORK_DIR, "gvcf_list": os.path.join(FINAL_WORK_DIR, "gvcf_list")}
    except Exception as e:
        log.error(e)
        send_airflow_failed_logs_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            message_prefix=e)
        raise ValueError(e)

test_dag_wdl()