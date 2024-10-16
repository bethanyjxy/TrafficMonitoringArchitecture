import unittest
from airflow.models import DagBag

class TestAllDAGs(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()

    def test_dags_import(self):
        """Test if all DAGs are imported correctly."""
        dag_ids = [
            'batch_pipeline_dag',
            'kafka_consumer_dag',
            'historical_dag',
            'kafka_producer_dag',
            'realtime_pipeline',
        ]
        for dag_id in dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            self.assertIsNotNone(dag, f"DAG {dag_id} is not imported.")
            self.assertEqual(dag.dag_id, dag_id)

    def test_task_count(self):
        """Test if the DAGs have the correct number of tasks."""
        task_counts = {
            'batch_pipeline_dag': 1,
            'kafka_consumer_dag': 4,
            'historical_dag': 1,
            'kafka_producer_dag': 1,
            'realtime_pipeline': 7,  # Including health checks
        }
        for dag_id, expected_count in task_counts.items():
            dag = self.dagbag.get_dag(dag_id)
            self.assertEqual(len(dag.tasks), expected_count, f"DAG {dag_id} has {len(dag.tasks)} tasks, expected {expected_count}.")

    def test_task_ids(self):
        """Test if specific task IDs exist in the DAGs."""
        dag_task_ids = {
            'batch_pipeline_dag': ['process_batch_data'],
            'kafka_consumer_dag': ['run_kafka_incidents_consumer', 'run_kafka_images_consumer', 'run_kafka_speedbands_consumer', 'run_kafka_vms_consumer'],
            'historical_dag': ['historical_dag'],
            'kafka_producer_dag': ['run_kafka_incidents_producer'],
            'realtime_pipeline': ['start_kafka_producer', 'start_kafka_vms_consumer', 'start_kafka_images_consumer', 'start_kafka_speedbands_consumer', 'start_kafka_incidents_consumer', 'check_producer_health', 'start_spark_streaming'],
        }
        for dag_id, expected_tasks in dag_task_ids.items():
            dag = self.dagbag.get_dag(dag_id)
            task_ids = [task.task_id for task in dag.tasks]
            for task in expected_tasks:
                self.assertIn(task, task_ids, f"Task {task} not found in DAG {dag_id}.")

    def test_task_dependencies(self):
        """Test if the tasks have the correct dependencies."""
        dag_dependencies = {
            'realtime_pipeline': {
                'start_kafka_producer': ['start_kafka_vms_consumer', 'start_kafka_images_consumer', 'start_kafka_speedbands_consumer', 'start_kafka_incidents_consumer', 'check_producer_health'],
                'check_producer_health': ['start_spark_streaming'],
                'check_vms_consumer_health': ['start_spark_streaming'],
                'check_images_consumer_health': ['start_spark_streaming'],
                'check_speedbands_consumer_health': ['start_spark_streaming'],
                'check_incidents_consumer_health': ['start_spark_streaming'],
            }
        }

        for dag_id, dependencies in dag_dependencies.items():
            dag = self.dagbag.get_dag(dag_id)
            for task_id, downstream_tasks in dependencies.items():
                task = dag.get_task(task_id)
                downstream_ids = [t.task_id for t in task.downstream_list]
                for downstream in downstream_tasks:
                    self.assertIn(downstream, downstream_ids, f"Task {downstream} is not a downstream of {task_id} in DAG {dag_id}.")

if __name__ == '__main__':
    unittest.main()
