import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../config')))
from postgres_config import POSTGRES_DB
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


def fetch_data_from_table(table_name):
    """Fetch all data from the specified table."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Use sql.Identifier for safe table name injection
    query = sql.SQL("SELECT * FROM {} LIMIT 500").format(sql.Identifier(table_name))
    cursor.execute(query)
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    cursor.close()
    return data_dicts

# Fetch the count of incidents for today
def fetch_incident_count_today():
    """Fetch the count of incidents that occurred today."""
    query = """
    SELECT COUNT(*) AS incident_count
    FROM incident_table
    WHERE DATE(created_at) = CURRENT_DATE;
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
    query = """
    SELECT DATE(created_at) AS incident_date, COUNT(*) AS incident_count
    FROM incident_table
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY incident_date
    ORDER BY incident_date;
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
    SELECT Type AS vehicle_type, COUNT(*) AS vehicle_count
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


# Fetch ERP charges over time by vehicle type
def fetch_erp_charges_over_time():
    """Fetch ERP charges over time grouped by vehicle type."""
    query = """
    SELECT StartTime, EndTime, VehicleType, SUM(ChargeAmount) AS total_charge
    FROM erp_table
    WHERE EffectiveDate = CURRENT_DATE
    GROUP BY StartTime, EndTime, VehicleType
    ORDER BY StartTime;
    """
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    conn.close()

    # Convert result to pandas DataFrame
    df = pd.DataFrame(result, columns=["StartTime", "EndTime", "VehicleType", "total_charge"])
    return df


# Fetch ERP charges by vehicle type (stacked bar chart data)
def fetch_erp_charges_by_vehicle_type():
    """Fetch the total ERP charges by vehicle type."""
    query = """
    SELECT VehicleType, SUM(ChargeAmount) AS total_charge
    FROM erp_table
    WHERE EffectiveDate = CURRENT_DATE
    GROUP BY VehicleType
    ORDER BY total_charge DESC;
    """
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    conn.close()

    # Convert result to pandas DataFrame
    df = pd.DataFrame(result, columns=["VehicleType", "total_charge"])
    return df

def fetch_today_table(table_name):
    """Fetch all incidents where the date matches the current day and month."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Get current day and month
    current_day_month = datetime.now().strftime('%-d/%-m')  

    # Use sql.Identifier for safe table name injection
    query = sql.SQL("SELECT * FROM {} WHERE date = %s LIMIT 500").format(sql.Identifier(table_name))
    
    # Execute query with the current day and month
    cursor.execute(query, [current_day_month])
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    cursor.close()
    conn.close()  # Always close the connection when done
    return data_dicts

def fetch_recent_images():
    """Fetch images from the image_table where the timestamp is within the last 5 minutes."""
    conn = connect_db()
    if not conn:
        return []

    cursor = conn.cursor()

    # Calculate the timestamp for 5 minutes ago
    five_minutes_ago = datetime.now() - timedelta(minutes=5)
    five_minutes_ago_str = five_minutes_ago.strftime('%Y-%m-%d %H:%M:%S')  # Format for PostgreSQL timestamp

    # Define the query to select images with a timestamp within the last 5 minutes
    query = sql.SQL("SELECT * FROM image_table WHERE timestamp >= %s")
    
    # Execute query with the timestamp parameter
    cursor.execute(query, [five_minutes_ago_str])
    
    # Fetch all rows and column names
    data = cursor.fetchall()
    column_names = [desc[0].lower() for desc in cursor.description]  # Convert column names to lowercase
    
    # Convert each row to a dictionary mapping column names to values
    data_dicts = [dict(zip(column_names, row)) for row in data]

    cursor.close()
    conn.close()  # Always close the connection when done
    return data_dicts


