# Configuration for PostgreSQL connection
POSTGRES_DB = {
    'dbname': 'traffic_db', # Default database
    'user': 'admin',   # User from docker-compose
    'password': 'admin',  # Password from docker-compose
    'host': 'localhost',  # Host as configured in docker-compose
    'port': '5432'
}

# Spark PostgreSQL connection properties
SPARK_POSTGRES = {
    'url': 'jdbc:postgresql://localhost:5432/traffic_db',
    'properties': {
        'user': 'admin',
        'password': 'admin',
        'driver': 'org.postgresql.Driver'
    }
}
