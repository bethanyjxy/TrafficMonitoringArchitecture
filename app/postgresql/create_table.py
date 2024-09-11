import psycopg2
from psycopg2 import sql

def create_database():
    # Connect to the default PostgreSQL database with the correct credentials
    conn = psycopg2.connect(
        dbname="traffic_db",         # Default database
        user="traffic_admin",      # User from docker-compose
        password="traffic_pass",   # Password from docker-compose
        host="localhost"           # Host as configured in docker-compose
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create the new database if it doesn't exist
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'traffic_db'")
    if not cursor.fetchone():
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier('traffic_db')))
    
    cursor.close()
    conn.close()

def create_tables():
    # Connect to the new database
    conn = psycopg2.connect(
        dbname="traffic_db",        # Database created/used in docker-compose
        user="traffic_admin",       # User from docker-compose
        password="traffic_pass",    # Password from docker-compose
        host="localhost",           # Host as configured in docker-compose
        port="5432"                 # Default port for PostgreSQL
    )
    cursor = conn.cursor()

    # Create tables
    tables = [
        """
        CREATE TABLE IF NOT EXISTS incident_table (
            id SERIAL PRIMARY KEY,
            Type VARCHAR(255),
            Latitude DOUBLE PRECISION,
            Longitude DOUBLE PRECISION,
            Message TEXT
)
        """,
        """
        CREATE TABLE IF NOT EXISTS speedbands_table (
            id SERIAL PRIMARY KEY,
            LinkID VARCHAR(255),
            RoadName VARCHAR(255),
            RoadCategory VARCHAR(255),
            SpeedBand INTEGER,
            MinimumSpeed INTEGER,
            MaximumSpeed INTEGER,
            StartLon DOUBLE PRECISION
)
        """,
        """
        CREATE TABLE IF NOT EXISTS image_table (
            id SERIAL PRIMARY KEY,
            CameraID VARCHAR(255),
            Latitude DOUBLE PRECISION,
            Longitude DOUBLE PRECISION,
            ImageLink TEXT
        )
        """
    ]

    for table in tables:
        cursor.execute(table)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_database()
    create_tables()
    print("Database and tables created successfully.")