import logging
import azure.functions as func
from datetime import datetime, timedelta
import statistics
import pyodbc
import time
from functools import wraps
import os

app = func.FunctionApp()

# Initialize lists to store the attributes from the incoming events
fraction_medium_a_list = []
fraction_medium_b_list = []

# Initialize the start time to track intervals
start_time = datetime.now()

# Fetch the environment variables
# OPTIMIZATION 1: no secrets in code but in environment variables
database_user = os.getenv('DATABASEUSER')
database_pw = os.getenv('DATABASEPW')

# Construct the connection string using environment variables
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:dataengprojecttw.database.windows.net,1433;Database=eventstore;Uid={database_user};Pwd={database_pw};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

# Decorator to add retry logic in case of failures when interacting with the database
def retry_on_failure(max_retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    # Try executing the function
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    # Log error with attempt count
                    logging.error(f"Attempt {retries} failed with error: {e}")
                    if retries < max_retries:
                        # Delay between retries to handle transient issues
                        time.sleep(delay)
                    else:
                        # Raise the exception if max retries are reached
                        raise
        return wrapper
    return decorator

# Function to write the calculated statistics to the database
# OPTIMIZATION 2: retry on failure decorator
@retry_on_failure()
def write_to_database(calculation_timestamp, avg_a, stddev_a, median_a, avg_b, stddev_b, median_b, num_events):
    # Establish a connection to the database
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        # Execute SQL query to insert the statistics into the database
        cursor.execute("""
            INSERT INTO SensorDataStatistics (CalculationTimestamp, AverageFractionA, StdDevFractionA, MedianFractionA, AverageFractionB, StdDevFractionB, MedianFractionB, ValueCount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (calculation_timestamp, avg_a, stddev_a, median_a, avg_b, stddev_b, median_b, num_events))
        conn.commit()

# Function to calculate the statistics (average, standard deviation, median) for the lists
# OPTIMIZATION 3: helper function
def calculate_statistics():
    # Calculate statistics for fraction medium A
    avg_a = statistics.mean(fraction_medium_a_list)
    stddev_a = statistics.stdev(fraction_medium_a_list)
    median_a = statistics.median(fraction_medium_a_list)
    
    # Calculate statistics for fraction medium B
    avg_b = statistics.mean(fraction_medium_b_list)
    stddev_b = statistics.stdev(fraction_medium_b_list)
    median_b = statistics.median(fraction_medium_b_list)
    
    # Count the number of events (size of the lists)
    num_events = len(fraction_medium_a_list)
    
    # Get the current timestamp in the correct format (to store in the database)
    calculation_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Log the results for debugging and monitoring purposes
    logging.info(f'Calculation Timestamp: {calculation_timestamp}')
    logging.info(f'Number of Events: {num_events}')
    logging.info(f'Fraction Medium A - Average: {avg_a}, Standard Deviation: {stddev_a}, Median: {median_a}')
    logging.info(f'Fraction Medium B - Average: {avg_b}, Standard Deviation: {stddev_b}, Median: {median_b}')
    
    # Write the results to the database using the helper function
    write_to_database(calculation_timestamp, avg_a, stddev_a, median_a, avg_b, stddev_b, median_b, num_events)

# Event grid trigger that listens for incoming events
# OPTIMIZATION 4: code refactoring into smaller functions
@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    global start_time, fraction_medium_a_list, fraction_medium_b_list

    # Get the event data (assumed to be in JSON format)
    event_data = azeventgrid.get_json()
    
    # Extract the attributes from the event data and append them to the respective lists
    fraction_medium_a_list.append(event_data['fraction medium A'])
    fraction_medium_b_list.append(event_data['fraction medium B'])
    
    # Check if one minute has passed since the last calculation
    if datetime.now() - start_time >= timedelta(minutes=1):
        # Calculate statistics for the current interval
        calculate_statistics()
        
        # Reset the lists and start time for the next interval
        fraction_medium_a_list = []
        fraction_medium_b_list = []
        start_time = datetime.now()
