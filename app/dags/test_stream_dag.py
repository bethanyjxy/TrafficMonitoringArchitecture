import unittest
from airflow.models import DagBag

class TestStreamDAG(unittest.TestCase):

    def setUp(self):
        """Set up the test case by loading the DAG."""
        self.dagbag = DagBag(dag_folder='.', include_examples=False)  # Adjust path if needed
        self.dag = self.dagbag.get_dag('kafka_spark_dag')

    def test_dag_import(self):
        """Test if the DAG is imported correctly."""
        self.assertIsNotNone(self.dag)
        self.assertEqual(self.dag.dag_id, 'kafka_spark_dag')

    def test_dag_structure(self):
        """Test if the DAG contains the expected tasks."""
        task_ids = [task.task_id for task in self.dag.tasks]
        expected_task_ids = [
            'start_kafka_producer',
            'start_kafka_consumer',
            'start_spark_streaming'
        ]
        for expected_task_id in expected_task_ids:
            self.assertIn(expected_task_id, task_ids)

    def test_task_dependencies(self):
        """Test if the task dependencies are set correctly."""
        producer_task = self.dag.get_task('start_kafka_producer')
        consumer_task = self.dag.get_task('start_kafka_consumer')
        streaming_task = self.dag.get_task('start_spark_streaming')

        self.assertEqual(len(producer_task.downstream_list), 1)  # Should lead to consumer
        self.assertEqual(len(consumer_task.downstream_list), 1)  # Should lead to streaming
        self.assertEqual(producer_task.downstream_list[0], consumer_task)
        self.assertEqual(consumer_task.downstream_list[0], streaming_task)

if __name__ == '__main__':
    unittest.main()
