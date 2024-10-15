from postgresql.postgres_config import POSTGRES_DB
import pandas as pd
from datetime import datetime, timedelta

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

def fetch_stream_table(table_name):
    """Fetch data from the specified table where the timestamp is within the last 2 days."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Calculate the timestamp for 1 days ago
    days_ago = datetime.now() - timedelta(days=1)

    # Use sql.Identifier for safe table name injection
    query = sql.SQL("""
        SELECT * FROM {} 
        WHERE CAST("timestamp" AS timestamp) >= %s
        LIMIT 500
    """).format(sql.Identifier(table_name))
    
    # Execute the query with the timestamp as a parameter
    cursor.execute(query, (days_ago,))
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    conn.close()
    return data_dicts

def fetch_speedband_location(location):
    conn = connect_db()
    if not conn:
        return pd.DataFrame()  # Return an empty DataFrame if the connection fails

    cursor = conn.cursor()
    
    # Construct the SQL query based on whether a location is provided
    if location == "":
        query = '''
        SELECT DISTINCT ON ("RoadName") * 
        FROM speedbands_table 
        ORDER BY "RoadName", timestamp DESC;
        '''
        cursor.execute(query)  # No parameters needed for this query
    else:
        query = '''
        SELECT DISTINCT ON ("LinkID") * 
        FROM speedbands_table 
        WHERE "RoadName" = %s
        ORDER BY "LinkID", timestamp DESC;
        '''
        cursor.execute(query, (location,))  # Pass the location as a tuple

    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Get column names from cursor description

    # Close the database connection
    conn.close()
    
    # Create a DataFrame from the fetched data
    df = pd.DataFrame(data, columns=column_names)
    return df  # Return the DataFrame



def fetch_unique_location():
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Use sql.Identifier for safe table name injection
    query = sql.SQL('SELECT DISTINCT "RoadName" FROM "speedbands_table" ORDER BY "RoadName" ASC')
    cursor.execute(query)
    
    # Fetch all rows and column names
    data = cursor.fetchall()

    conn.close()
    # Format data for the dropdown
    return [{'label': row[0], 'value': row[0]} for row in data]
    

# Fetch the count of incidents for today
def fetch_incident_count_today():
    """Fetch the count of incidents that occurred today."""
    query = """
    SELECT COUNT(*) AS incident_count
    FROM incident_table
    WHERE TO_DATE(incident_date || '/' || EXTRACT(YEAR FROM CURRENT_DATE), 'DD/MM/YYYY') = CURRENT_DATE
    LIMIT 100;

    """
    conn = connect_db()
    if not conn:
        return 0

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
    
    conn.close()
    return result[0] if result else 0


# Fetch the incidents over time for the past month
def fetch_incidents_over_time():
    """Fetch the number of incidents per day over the past 30 days."""
    current_year = datetime.now().year  # Get the current year
    query = f"""
    SELECT TO_TIMESTAMP("incident_date" || '/{current_year} ' || "incident_time", 'DD/MM/YYYY HH24:MI') AS incident_datetime, 
           COUNT(*) AS incident_count
    FROM incident_table
    GROUP BY incident_datetime
    ORDER BY incident_datetime;
    """
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    
    conn.close()

    # Convert result to pandas DataFrame
    df = pd.DataFrame(result, columns=["incident_date", "incident_count"])

    return df


# Fetch vehicle types involved in incidents and their counts
def fetch_vehicle_type_incidents():
    """Fetch the breakdown of vehicle types involved in incidents."""
    query = """
    SELECT "Type" AS vehicle_type, COUNT(*) AS vehicle_count
    FROM incident_table
    GROUP BY vehicle_type
    ORDER BY vehicle_count DESC;
    """
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    conn.close()

    # Convert result to pandas DataFrame
    df = pd.DataFrame(result, columns=["vehicle_type", "vehicle_count"])
    return df


def fetch_recent_images():
    """Fetch images from the image_table where the timestamp is within the last 5 minutes."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Define the query to select images with a timestamp within the last 5 minutes
    query = """
    SELECT * FROM image_table 
    WHERE to_timestamp(timestamp, 'YYYY-MM-DD HH24:MI:SS') >= NOW() - INTERVAL '5 minutes'
    """
    
    # Execute query
    cursor.execute(query)
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    conn.close()  # Always close the connection when done
    return data_dicts