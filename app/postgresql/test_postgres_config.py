import unittest
from postgres_config import POSTGRES_DB, SPARK_POSTGRES

class TestPostgresConfig(unittest.TestCase):

    def test_postgres_db_config(self):
        """Test that PostgreSQL configuration is set correctly."""
        self.assertEqual(POSTGRES_DB['dbname'], 'traffic_db')
        self.assertEqual(POSTGRES_DB['user'], 'traffic_admin')
        self.assertEqual(POSTGRES_DB['password'], 'traffic_pass')
        self.assertEqual(POSTGRES_DB['host'], 'postgres')
        self.assertEqual(POSTGRES_DB['port'], '5432')

    def test_spark_postgres_config(self):
        """Test that Spark PostgreSQL configuration is set correctly."""
        self.assertEqual(SPARK_POSTGRES['url'], 'jdbc:postgresql://localhost:5432/traffic_db')
        self.assertEqual(SPARK_POSTGRES['properties']['user'], 'traffic_admin')
        self.assertEqual(SPARK_POSTGRES['properties']['password'], 'traffic_pass')
        self.assertEqual(SPARK_POSTGRES['properties']['driver'], 'org.postgresql.Driver')

if __name__ == '__main__':
    unittest.main()
