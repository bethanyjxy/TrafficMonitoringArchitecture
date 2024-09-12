# Configuration for PostgreSQL connection
POSTGRES_DB = {
    'dbname': 'traffic_db', # Default database
    'user': 'traffic_admin',   # User from docker-compose
    'password': 'traffic_pass',  # Password from docker-compose
    'host': 'localhost',  # Host as configured in docker-compose
    'port': '5432'
}

# Spark PostgreSQL connection properties
SPARK_POSTGRES = {
    'url': 'jdbc:postgresql://localhost:5432/traffic_db',
    'properties': {
        'user': 'traffic_admin',
        'password': 'traffic_pass',
        'driver': 'org.postgresql.Driver'
    }
}
