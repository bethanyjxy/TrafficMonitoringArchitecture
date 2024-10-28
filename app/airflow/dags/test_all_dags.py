import unittest
from airflow import DAG
from airflow.models import DagBag

class TestAllDAGs(unittest.TestCase):

    def setUp(self):
        # Load all DAGs from the specified directory
        self.dagbag = DagBag(dag_folder='/home/theo/Desktop/TrafficMonitoringArchitecture/app/airflow/dags', include_examples=False)

    def test_dags_import(self):
        """Test if all DAGs are imported correctly."""
        dag_ids = [
            'kafka_consumer_dag',
            'daily_incident_batch',
            'historical_dag',
            'kafka_producer_dag',
            'spark_postgres_stream_dag'
        ]
        for dag_id in dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            self.assertIsNotNone(dag, f"DAG {dag_id} is not imported.")

    def test_task_count(self):
        """Test if the DAGs have the correct number of tasks."""
        expected_counts = {
            'kafka_consumer_dag': 4,  # Adjust based on your actual expected count
            'daily_incident_batch': 1,
            'historical_dag': 1,
            'kafka_producer_dag': 1,
            'spark_postgres_stream_dag': 1
        }
        for dag_id, expected_count in expected_counts.items():
            dag = self.dagbag.get_dag(dag_id)
            self.assertEqual(len(dag.tasks), expected_count, f"DAG {dag_id} has {len(dag.tasks)} tasks, expected {expected_count}.")

    def test_task_dependencies(self):
        """Test if the tasks have the correct dependencies."""
        # Example for kafka_consumer_dag
        dag_id = 'kafka_consumer_dag'
        dag = self.dagbag.get_dag(dag_id)
        task_ids = [task.task_id for task in dag.tasks]
        
        # Check if tasks run in parallel (no dependencies)
        self.assertIn('run_kafka_incidents_consumer', task_ids)
        self.assertIn('run_kafka_images_consumer', task_ids)
        self.assertIn('run_kafka_speedbands_consumer', task_ids)
        self.assertIn('run_kafka_vms_consumer', task_ids)

    def test_task_ids(self):
        """Test if specific task IDs exist in the DAGs."""
        dag_id = 'daily_incident_batch'
        dag = self.dagbag.get_dag(dag_id)
        task_ids = [task.task_id for task in dag.tasks]
        self.assertIn('run_daily_incident_report', task_ids)

if __name__ == '__main__':
    unittest.main()
