from postgres_config import POSTGRES_DB
import psycopg2
from psycopg2 import sql


def create_database():
    # Connect to the default PostgreSQL database with the correct credentials
    conn = psycopg2.connect(
        dbname=POSTGRES_DB['dbname'],
        user=POSTGRES_DB['user'],
        password=POSTGRES_DB['password'],
        host=POSTGRES_DB['host']
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
        dbname=POSTGRES_DB['dbname'],
        user=POSTGRES_DB['user'],
        password=POSTGRES_DB['password'],
        host=POSTGRES_DB['host'],
        port = POSTGRES_DB['port']
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
            StartLat DOUBLE PRECISION
            EndLon DOUBLE PRECISION
            EndLat DOUBLE PRECISION
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
        """,
        
        """
        CREATE TABLE IF NOT EXISTS vms_table (
            EquipmentID VARCHAR(255),
            Latitude DOUBLE PRECISION,
            Longitude DOUBLE PRECISION,
            Message TEXT
        )
        """,

        """
        CREATE TABLE IF NOT EXISTS erp_table (
            VehicleType VARCHAR(255),
            DayType VARCHAR(255),
            StartTime VARCHAR(255),
            EndTime VARCHAR(255),
            ZoneID VARCHAR(255),
            ChargeAmount INTEGER,
            EffectiveDate DATE
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