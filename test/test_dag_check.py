import os
import unittest
from airflow.models import DagBag
from jinja2 import Environment, FileSystemLoader, select_autoescape

class TestAllDagIntegrity(unittest.TestCase):

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
              1: 3}
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

  def test_dag_import(self):
    self.assertFalse(
      len(self.dagbag.import_errors),
      'DAG import errors: {}'.format(
        self.dagbag.import_errors))

if __name__=='__main__':
  checks = \
    unittest.\
      TestLoader().\
        loadTestsFromTestCase(TestAllDagIntegrity)
  unittest.\
    TextTestRunner(verbosity=1).\
      run(checks)
