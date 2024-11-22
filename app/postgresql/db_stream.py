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

    timestamp_column = "img_timestamp" if table_name == "image_table" else "timestamp"

    # Use sql.Identifier for safe table and column name injection
    query = sql.SQL("""
        SELECT * FROM {table}
        WHERE (TO_TIMESTAMP({timestamp_column}, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'Asia/Singapore') <= %s
        LIMIT 500
    """).format(
        table=sql.Identifier(table_name),
        timestamp_column=sql.Identifier(timestamp_column)
    )
    
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

def fetch_incident_table():
    # Define your database connection parameters
    conn = connect_db()
    if not conn:
        return pd.DataFrame()  # Return an empty DataFrame if the connection fails

    cursor = conn.cursor()
    
    # SQL query to fetch latest incidents
    query = """
    SELECT incident_date, incident_time, incident_message
    FROM incident_table
    ORDER BY TO_TIMESTAMP(incident_date || ' ' || incident_time, 'DD/MM HH24:MI') DESC
    LIMIT 50;
    """
    
    # Execute query and fetch data
    with conn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    
    # Convert to DataFrame for easy manipulation
    df = pd.DataFrame(data, columns=['incident_date', 'incident_time', 'incident_message'])
    
    # Close connection
    conn.close()
    
    return df


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
    WHERE TO_DATE(incident_date || '/' || EXTRACT(YEAR FROM TIMEZONE('Asia/Singapore', NOW())), 'DD/MM/YYYY') = TIMEZONE('Asia/Singapore', NOW())::date
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

def fetch_incident_map():
    """Fetch all records from the incident_table where the incident_date is today."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Get today's date in 'YYYY-MM-DD' format
    today_date = datetime.now().strftime('%Y-%m-%d')

    # Query to select all records where the incident_date is today
    query = sql.SQL("""
        SELECT * FROM incident_table
        WHERE TO_DATE(incident_date || '/' || EXTRACT(YEAR FROM TIMEZONE('Asia/Singapore', NOW())), 'DD/MM/YYYY') = TIMEZONE('Asia/Singapore', NOW())::date
    """)

    # Execute the query with today's date as a parameter
    cursor.execute(query, (today_date,))
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    conn.close()
    return data_dicts

# Fetch the incidents over time for the past month
def fetch_incidents_today():
    current_year = datetime.now().year
    current_date = datetime.now().strftime('%d/%m')  # Format to match incident_date format (e.g., "04/11")
    
    query = f"""
    WITH recent_incidents AS (
        SELECT 
            TO_TIMESTAMP(incident_date || '/' || EXTRACT(YEAR FROM CURRENT_DATE) || ' ' || incident_time, 'DD/MM/YYYY HH24:MI') AS incident_datetime,
            COUNT(*) AS incident_count
        FROM incident_table
        WHERE TO_DATE(incident_date || '/' || EXTRACT(YEAR FROM CURRENT_DATE), 'DD/MM/YYYY') = CURRENT_DATE
        GROUP BY incident_datetime
        ORDER BY incident_datetime
    )
    SELECT * FROM recent_incidents;
    """
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        cursor.execute(query, (current_date,))
        result = cursor.fetchall()
    
    conn.close()

    # Convert result to pandas DataFrame
    df = pd.DataFrame(result, columns=["incident_datetime", "incident_count"])

    return df


# Fetch vehicle types involved in incidents and their counts
def fetch_vehicle_type_incidents():
    """Fetch the breakdown of vehicle types involved in incidents."""
    query = """
    SELECT "Type" AS vehicle_type, COUNT(*) AS vehicle_count
    FROM incident_table
    WHERE TO_DATE(incident_date || '/' || EXTRACT(YEAR FROM CURRENT_DATE), 'DD/MM/YYYY') = CURRENT_DATE
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


def fetch_images_table():
    conn = connect_db()
    if not conn:
        return pd.DataFrame()  # Return an empty DataFrame if connection fails

    cursor = conn.cursor()

    # Define the query to select all images 
    query = """
    SELECT DISTINCT ON (camera_id) *
        FROM image_table
        ORDER BY camera_id, img_timestamp DESC;
    """
    #ORDER BY camera_id, img_timestamp::timestamp with time zone DESC;
    
    # Execute the query
    cursor.execute(query)
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    conn.close()  # Always close the connection when done
    
    # Convert the list of dictionaries to a Pandas DataFrame
    return pd.DataFrame(data_dicts)

def fetch_traffic_jams():
    """Fetch traffic jams by selecting records with SpeedBand < 3 and calculating total count and average speed."""
    query = """
    SELECT 
    COUNT(*) AS jam_count,
    AVG((CAST("MinimumSpeed" AS FLOAT) + CAST("MaximumSpeed" AS FLOAT)) / 2) AS avg_speed
    FROM speedbands_table
    WHERE "SpeedBand" < 3
    AND DATE(("timestamp"::timestamp AT TIME ZONE 'UTC') + INTERVAL '8 hours') = CURRENT_DATE;
        
    """
    
    conn = connect_db()
    if not conn:
        return {"jam_count": 0, "avg_speed": None}

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
    
    conn.close()

    # Return a dictionary with jam count and average speed
    return {"jam_count": result[0], "avg_speed": result[1] if result else None}

def fetch_incident_density():
    """Fetch incident data with coordinates for density map visualization."""
    query = """
    SELECT "Latitude", "Longitude", COUNT(*) AS incident_density
    FROM incident_table
    WHERE TO_DATE(incident_date || '/' || EXTRACT(YEAR FROM TIMEZONE('Asia/Singapore', NOW())), 'DD/MM/YYYY') = TIMEZONE('Asia/Singapore', NOW())::date
    GROUP BY "Latitude", "Longitude"
    HAVING COUNT(*) > 1  -- Filter to show areas with multiple incidents
    ORDER BY incident_density DESC
    LIMIT 500;  -- Adjust the limit based on desired map detail
    """
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    conn.close()
    df = pd.DataFrame(result, columns=["Latitude", "Longitude", "incident_density"])
    return df


def fetch_speed_trend_data():
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    query = """
    SELECT timestamp, AVG("MaximumSpeed") AS average_speed
    FROM speedbands_table
    WHERE DATE(("timestamp"::timestamp AT TIME ZONE 'UTC') + INTERVAL '8 hours') = CURRENT_DATE
    GROUP BY timestamp
    ORDER BY timestamp
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    conn.close()
    return pd.DataFrame(result, columns=["timestamp", "average_speed"])

def fetch_road_speed_performance_data():
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    query = """
    SELECT "RoadName", AVG((CAST("MinimumSpeed" AS FLOAT) + CAST("MaximumSpeed" AS FLOAT)) / 2) AS avg_speed
    FROM speedbands_table
    WHERE DATE(("timestamp"::timestamp AT TIME ZONE 'UTC') + INTERVAL '8 hours') = CURRENT_DATE
    GROUP BY "RoadName"
    ORDER BY avg_speed DESC
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    conn.close()
    return pd.DataFrame(result, columns=["RoadName", "average_speed"])


def fetch_recent_vms_messages():
    """Fetch recent messages from the VMS table."""
    query = """
    SELECT 
    ("timestamp"::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Singapore' AS "timestamp_sgt",
    "Message"
    FROM vms_table
    ORDER BY "timestamp_sgt" DESC
    LIMIT 10; -- Limit to the most recent 10 messages
    """
    conn = connect_db()
    if not conn:
        return []

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    conn.close()
    messages = [{"timestamp": row[0], "Message": row[1]} for row in result]
    return messages
