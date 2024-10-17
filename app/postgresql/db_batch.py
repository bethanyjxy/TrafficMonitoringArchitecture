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

    conn.close()
    return data_dicts

def fetch_population_make_table(type, selected):
    conn = connect_db()
    if not conn:
        return pd.DataFrame()  # Return an empty DataFrame

    # Use context manager for cursor
    with conn.cursor() as cursor:
        if type == 'cars':
            dt = 'cars_make'
        elif type == 'motorcycles':
            dt = 'mc_make'
        else:
            return pd.DataFrame()  # Return an empty DataFrame if type is not recognized
        
        # Prepare the SQL query with parameterized selection
        query = sql.SQL("SELECT year,SUM(number) AS total_number FROM {} WHERE make = %s GROUP BY YEAR").format(sql.Identifier(dt))
        cursor.execute(query, (selected,))  # Execute with the selected parameter

        # Fetch all rows
        data = cursor.fetchall()
        
        conn.close()

    # Convert fetched data to a DataFrame
    df = pd.DataFrame(data, columns=['year', 'total_number'])
    return df  # Return the DataFrame

def fetch_population_cc_table(type, selected):
    conn = connect_db()
    if not conn:
        return pd.DataFrame()  # Return an empty DataFrame

    # Use context manager for cursor
    with conn.cursor() as cursor:
        if type == 'cars':
            dt = 'cars_cc'
        elif type == 'motorcycles':
            dt = 'mc_cc'
        else:
            return pd.DataFrame()  # Return an empty DataFrame if type is not recognized
        
        # Prepare the SQL query with parameterized selection
        query = sql.SQL("SELECT year, SUM(number) AS total_number FROM {} WHERE cc_rating = %s GROUP BY YEAR").format(sql.Identifier(dt))
        cursor.execute(query, (selected,))  # Execute with the selected parameter

        # Fetch all rows
        data = cursor.fetchall()
        
        conn.close()
    
    # Convert fetched data to a DataFrame
    df = pd.DataFrame(data, columns=['year', 'total_number'])
    return df  # Return the DataFrame


def fetch_population_year_table(type, filtertype):
    conn = connect_db()
    if not conn:
        return pd.DataFrame()  # Return an empty DataFrame

    # Use context manager for cursor
    with conn.cursor() as cursor:
        if type == 'cars' and filtertype == 'make':
            dt = 'cars_make'
        elif type == 'cars' and filtertype == 'cc':
            dt = 'cars_cc'
        elif type == 'motorcycles' and filtertype == 'make':
            dt = 'mc_make'
        elif type == 'motorcycles' and filtertype == 'cc':
            dt = 'mc_cc'
        else:
            return pd.DataFrame()  # Return an empty DataFrame

        query = sql.SQL("SELECT CAST(year AS INTEGER) AS year, SUM(number) as total_number FROM {} GROUP BY year").format(sql.Identifier(dt))
        cursor.execute(query)

        # Fetch all rows
        data = cursor.fetchall()
    
    conn.close()  # Close connection

    # Convert fetched data to a DataFrame
    df = pd.DataFrame(data, columns=['year', 'total_number'])
    return df  # Return the DataFrame

def fetch_unique_type_table(type, filtertype):
    conn = connect_db()
    if not conn:
        return []

    # Use context manager for cursor
    with conn.cursor() as cursor:
        if type == 'cars' and filtertype == 'make':
            dt = 'cars_make'
        elif type == 'cars' and filtertype == 'cc':
            dt = 'cars_cc'
        elif type == 'motorcycles'and filtertype == 'make':
            dt = 'mc_make'
        elif type == 'motorcycles' and filtertype == 'cc':
            dt = 'mc_cc'
        else:
            return []  # Return empty list if type is not recognized
        
        if filtertype == 'make':
            query = sql.SQL("SELECT make FROM {} GROUP BY  make").format(sql.Identifier(dt))
            cursor.execute(query)
        elif filtertype == 'cc':
            query = sql.SQL("SELECT cc_rating FROM {} GROUP BY cc_rating").format(sql.Identifier(dt))
            cursor.execute(query)

        # Fetch all rows and column names
        data = cursor.fetchall()
        # Flatten the list of tuples into a list of strings (or numbers)
        if filtertype == 'make':
            return [make[0] for make in data]  # Extract first element from each tuple
        elif filtertype == 'cc':
            return [cc[0] for cc in data]  # Extract first element from each tuple
    conn.close()  # Close connection





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


def fetch_vms_incident_correlation():
    """Fetch the correlation between VMS messages and incident occurrences."""
    query = """
    SELECT v."Message" AS vms_message, COUNT(*) AS incident_count
    FROM vms_table v
    LEFT JOIN incident_table i
    ON i."Latitude" BETWEEN v."Latitude" - 0.1 AND v."Latitude" + 0.1
    AND i."Longitude" BETWEEN v."Longitude" - 0.1 AND v."Longitude" + 0.1
    AND ABS(EXTRACT(EPOCH FROM (
        TO_TIMESTAMP(i."incident_date" || '/2024 ' || i."incident_time", 'DD/MM/YYYY HH24:MI') - v."timestamp"::TIMESTAMP))) <= 3600
    GROUP BY v."Message"
    ORDER BY incident_count DESC;
    ;
    """
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    
    conn.close()
    
    # Convert result to pandas DataFrame
    df = pd.DataFrame(result, columns=["vms_message", "incident_count"])
    
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

def fetch_report_incident():
    """Fetch the number of incidents per day for the current month."""
    # Get the current date and calculate the first day of the month
    today = datetime.now()
    first_day_of_month = today.replace(day=1)

    # SQL query to fetch incidents for the current month
    query = """
    SELECT "date", "result"
    FROM report_incident
    WHERE "date" >= %s AND "date" < %s
    """

    # Calculate the first day of the next month to use in the query
    if today.month == 12:  # Handle December case (next month is January of the next year)
        first_day_of_next_month = today.replace(year=today.year + 1, month=1, day=1)
    else:
        first_day_of_next_month = today.replace(month=today.month + 1, day=1)

    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        # Execute the query, passing the parameters for the start of the current month and next month
        cursor.execute(query, (first_day_of_month, first_day_of_next_month))
        result = cursor.fetchall()
    
    conn.close()

    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=["date", "result"])

    return df

def fetch_average_speedband(road_name):
    conn = connect_db()
    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        query = """
        SELECT "RoadName", "hour_of_day", "average_speedband", 
               "rounded_speedband", "speedband_description", "recorded_at"
        FROM traffic_speedband_prediction
        WHERE recorded_at <= NOW() AND "RoadName" = %s
        ORDER BY recorded_at;
        """
        cursor.execute(query, (road_name,))
        data = cursor.fetchall()

        # Fetch column names from the cursor description
        colnames = [desc[0] for desc in cursor.description]

    conn.close() 
    df = pd.DataFrame(data, columns=colnames)
    return df
