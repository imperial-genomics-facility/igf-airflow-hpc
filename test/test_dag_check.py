import unittest
from airflow.models import DagBag,Variable

class TestAllDagIntegrity(unittest.TestCase):

  LOAD_SECOND_THRESHOLD = 2

  def setUp(self):
    self.dagbag = DagBag('dags')


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
