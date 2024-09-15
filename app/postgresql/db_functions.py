import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../config')))
from postgres_config import POSTGRES_DB

import psycopg2
from psycopg2 import sql


def connect_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            dbname=POSTGRES_DB['dbname'],
            user=POSTGRES_DB['user'],
            password=POSTGRES_DB['password'],
            host=POSTGRES_DB['host'],
            port=POSTGRES_DB['port']
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
    
def check_db_connection():
    """Check if a connection to the database can be established."""
    connection = None
    try:
        connection = connect_db()
        if connection is None:
            return "Connection failed."
        return "Successfully connected to PostgreSQL!"
    except Exception as e:
        return f"Connection failed: {e}"
    finally:
        if connection:
            connection.close()


def fetch_data_from_table(table_name):
    """Fetch all data from the specified table."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Use sql.Identifier for safe table name injection
    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
    cursor.execute(query)
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    cursor.close()
    return data_dicts
