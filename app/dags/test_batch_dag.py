import unittest
from airflow.models import DagBag

class TestBatchDAG(unittest.TestCase):

    def setUp(self):
        """Set up the test case by loading the DAG."""
        self.dagbag = DagBag(dag_folder='.', include_examples=False)  # Adjust path if needed
        self.dag = self.dagbag.get_dag('batch_pipeline_dag')

    def test_dag_import(self):
        """Test if the DAG is imported correctly."""
        self.assertIsNotNone(self.dag)
        self.assertEqual(self.dag.dag_id, 'batch_pipeline_dag')

    def test_dag_structure(self):
        """Test if the DAG contains the expected tasks."""
        task_ids = [task.task_id for task in self.dag.tasks]
        self.assertIn('process_batch_data', task_ids)

    def test_task_dependencies(self):
        """Test if the task dependencies are set correctly."""
        task = self.dag.get_task('process_batch_data')
        self.assertEqual(len(task.upstream_list), 0)  # No upstream tasks
        self.assertEqual(len(task.downstream_list), 0)  # No downstream tasks

if __name__ == '__main__':
    unittest.main()
