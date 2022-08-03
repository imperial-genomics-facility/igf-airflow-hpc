import os
import unittest
from airflow.models import DagBag, Variable
from jinja2 import Template, Environment, FileSystemLoader, select_autoescape

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

  def test_bclconvert_demult(self):
    dag = self.dagbag.get_dag(dag_id="bclconvert_demult")
    self.assertIsNotNone(dag)
    task_list = \
        dag.task_dict.keys()
    task_list = \
        list(task_list)
    bclconvert_tasks = \
        [task for task in task_list if task.startswith("bclconvert_")]
    self.assertEqual(len(bclconvert_tasks), 5)
    self.assertTrue(dag.has_task('format_and_split_samplesheet'))
    task = dag.get_task('format_and_split_samplesheet')
    downstream_task_ids = task.downstream_task_ids
    self.assertEqual(len(downstream_task_ids), 4)
    self.assertTrue('setup_qc_page_for_project_4' in downstream_task_ids)
    self.assertTrue(dag.has_task('get_lanes_for_project_1'))
    task = dag.get_task('get_lanes_for_project_1')
    downstream_task_ids = task.downstream_task_ids
    self.assertEqual(len(downstream_task_ids), 2)
    self.assertTrue('get_igs_for_project_1_lane_2' in downstream_task_ids)
    task = dag.get_task('get_igs_for_project_1_lane_2')
    downstream_task_ids = task.downstream_task_ids
    self.assertEqual(len(downstream_task_ids), 1)
    self.assertTrue('bclconvert_for_project_1_lane_2_ig_1' in downstream_task_ids)
    self.assertTrue(dag.has_task('sample_group_1_2_1.get_samples_for_project_1_lane_2_ig_1'))
    task = dag.get_task('sample_group_1_2_1.get_samples_for_project_1_lane_2_ig_1')
    downstream_task_ids = task.downstream_task_ids
    self.assertEqual(len(downstream_task_ids), 3)
    self.assertTrue('sample_group_1_2_1.calculate_md5_project_1_lane_2_ig_1_sample_3' in downstream_task_ids)
    self.assertTrue(dag.has_task('collect_qc_reports_project_1_lane_2_ig_1'))
    task = dag.get_task('collect_qc_reports_project_1_lane_2_ig_1')
    upstream_tasks_ids = task.upstream_task_ids
    self.assertEqual(len(upstream_tasks_ids), 8)
    self.assertTrue('bclconvert_for_project_1_lane_2_ig_1' in upstream_tasks_ids)
    self.assertTrue('sample_group_1_2_1.get_fastqs_and_copy_to_globus_for_project_1_lane_2_ig_1' in upstream_tasks_ids)
    self.assertTrue(dag.has_task('build_qc_page_for_project_1'))
    task = dag.get_task('build_qc_page_for_project_1')
    upstream_tasks_ids = task.upstream_task_ids
    self.assertEqual(len(upstream_tasks_ids), 2)
    self.assertTrue('build_qc_page_for_project_1_lane_1' in upstream_tasks_ids)


if __name__=='__main__':
  checks = \
    unittest.\
      TestLoader().\
        loadTestsFromTestCase(TestDemultDag)
  unittest.\
    TextTestRunner(verbosity=1).\
      run(checks)
