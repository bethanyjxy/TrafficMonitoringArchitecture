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
            host=POSTGRES_DB['host']
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
    
def check_db_connection():
    """Check if a connection to the database can be established."""
    try:
        connection = connect_db()
        if connection is None:
            return "Connection failed."
        connection.close()
        return "Successfully connected to PostgreSQL!"
    except Exception as e:
        return f"Connection failed: {e}"
    

def fetch_data_from_table(table_name):
    """Fetch all data from the specified table."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()
    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
    cursor.execute(query)
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    cursor.close()
    conn.close()
    
    # Convert fetched data into a list of dictionaries with lowercase keys
    data_dicts = [dict(zip(column_names, row)) for row in data]
    return data_dicts

