import os
import unittest
from airflow import DAG
from airflow.models import DagBag
from jinja2 import Environment, FileSystemLoader, select_autoescape

def check_task_dependency(
      source: dict,
      dag: DAG,
      flow: str = 'downstream') \
        -> None:
  try:
    if flow == 'downstream':
      for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)
    elif flow == 'upstream':
      for task_id, upstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.upstream_task_ids == set(upstream_list)
    else:
      raise ValueError('flow must be either "downstream" or "upstream"')
  except:
    raise


class TestDemultDag(unittest.TestCase):

  LOAD_SECOND_THRESHOLD = 2

  def setUp(self):
    template_file = \
      'dynamic_dag_templates/bclconvert_demult_template.py'
    output_file = \
      'dags/bclconvert_demult.py'
    autoescape_list = ['txt',]
    template_env = \
      Environment(
        loader=FileSystemLoader(
          searchpath=os.path.dirname(template_file)),
          autoescape=select_autoescape(autoescape_list))
    template = \
      template_env.\
        get_template(
          os.path.basename(template_file))
    data = \
      dict(
        SEQRUN_IGF_ID="SEQ00001",
        SAMPLE_GROUPS={
          1: {
            1: {
              1: 3},
            2:{
              1: 3},
          },
          2: {
            1: {
              1: 5}
          },
          3: {
            1: {
              1: 3}
          },
          4: {
            1: {
              1: 1}
          }
        },
        FORMATTED_SAMPLESHEETS=[{
          'project': 'P1',
          'project_index': 1,
          'lane': 1,
          'lane_index': 1,
          'bases_mask': 'Y150N1;I8N2;N2I8;Y150N1',
          'index_group': '16_NA',
          'index_group_index': 1,
          'sample_counts': 3,
          'samplesheet_file': 'SampleSheet_P1_1_16_NA.csv'
        }, {
          'project': 'P1',
          'project_index': 1,
          'lane': 2,
          'lane_index': 2,
          'bases_mask': 'Y150N1;I8N2;N2I8;Y150N1',
          'index_group': '16_NA',
          'index_group_index': 1,
          'sample_counts': 3,
          'samplesheet_file': 'SampleSheet_P1_2_16_NA.csv'
        },{
          'project': 'P2',
          'project_index': 2,
          'lane': 1,
          'lane_index': 1,
          'bases_mask': 'Y150N1;I8N2;N10;Y150N1',
          'index_group': '8_10X',
          'index_group_index': 1,
          'sample_counts': 5,
          'samplesheet_file': 'SampleSheet_P2_1_8_10X.csv'
        }, {
          'project': 'P3',
          'project_index': 3,
          'lane': 1,
          'lane_index': 1,
          'bases_mask': 'Y150N1;I8N2;N10;Y150N1',
          'index_group': '8_NA',
          'index_group_index': 1,
          'sample_counts': 3,
          'samplesheet_file': 'SampleSheet_P3_1_8_NA.csv'
        }, {
          'project': 'P4',
          'project_index': 4,
          'lane': 1,
          'lane_index': 1,
          'bases_mask': 'Y150N1;I8N2;N2I8;Y150N1',
          'index_group': '16_NA',
          'index_group_index': 1,
          'sample_counts': 1,
          'samplesheet_file': 'SampleSheet_P4_1_16_NA.csv'
        }]
      )
    template.\
      stream(**data).\
      dump(output_file)
    self.dagbag = DagBag('dags')

  def tearDown(self) -> None:
    os.remove('dags/bclconvert_demult.py')

  def test_bclconvert_demult(self):
    dag = self.dagbag.get_dag(dag_id="bclconvert_demult")
    self.assertIsNotNone(dag)
    task_list = \
        dag.task_dict.keys()
    task_list = \
        list(task_list)
    self.assertTrue(dag.has_task('format_and_split_samplesheet'))
    check_task_dependency({
      'format_and_split_samplesheet': [
        'setup_qc_page_for_project_1',
        'setup_qc_page_for_project_2',
        'setup_qc_page_for_project_3',
        'setup_qc_page_for_project_4'
      ]
    }, dag, flow='downstream')
    self.assertTrue(dag.has_task('get_lanes_for_project_1'))
    check_task_dependency({
      'get_lanes_for_project_1': [
        'get_igs_for_project_1_lane_1',
        'get_igs_for_project_1_lane_2'
      ]
    }, dag, flow='downstream')
    check_task_dependency({
      'get_igs_for_project_1_lane_2': [
        'bclconvert_for_project_1_lane_2_ig_1'
      ]
    }, dag, flow='downstream')
    self.assertTrue(dag.has_task('sample_group_1_2_1.get_samples_for_project_1_lane_2_ig_1'))
    check_task_dependency({
      'sample_group_1_2_1.get_samples_for_project_1_lane_2_ig_1': [
        'sample_group_1_2_1.calculate_md5_project_1_lane_2_ig_1_sample_1',
        'sample_group_1_2_1.calculate_md5_project_1_lane_2_ig_1_sample_2',
        'sample_group_1_2_1.calculate_md5_project_1_lane_2_ig_1_sample_3'
      ]
    }, dag, flow='downstream')
    self.assertTrue(dag.has_task('collect_qc_reports_project_1_lane_2_ig_1'))
    check_task_dependency({
      'collect_qc_reports_project_1_lane_2_ig_1': [
        'bclconvert_for_project_1_lane_2_ig_1',
        'sample_group_1_2_1.get_fastqs_and_copy_to_globus_for_project_1_lane_2_ig_1',
        'sample_group_1_2_1.fastqc_project_1_lane_2_ig_1_sample_1',
        'sample_group_1_2_1.fastqc_project_1_lane_2_ig_1_sample_2',
        'sample_group_1_2_1.fastqc_project_1_lane_2_ig_1_sample_3',
        'sample_group_1_2_1.fastq_screen_project_1_lane_2_ig_1_sample_1',
        'sample_group_1_2_1.fastq_screen_project_1_lane_2_ig_1_sample_2',
        'sample_group_1_2_1.fastq_screen_project_1_lane_2_ig_1_sample_3',
      ]
    }, dag, flow='upstream')
    self.assertTrue(dag.has_task('build_qc_page_for_project_1'))
    check_task_dependency({
      'build_qc_page_for_project_1': [
        'build_qc_page_for_project_1_lane_1',
        'build_qc_page_for_project_1_lane_2'
      ]
    }, dag, flow='upstream')


if __name__=='__main__':
  checks = \
    unittest.\
      TestLoader().\
        loadTestsFromTestCase(TestDemultDag)
  unittest.\
    TextTestRunner(verbosity=1).\
      run(checks)
