import logging
import azure.functions as func
from datetime import datetime, timedelta
import statistics
import pyodbc

app = func.FunctionApp()

# Initialize lists to store the attributes
fraction_medium_a_list = []
fraction_medium_b_list = []

# Initialize the start time
start_time = datetime.now()

# Database connection string
connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:dataengprojecttw.database.windows.net,1433;Database=eventstore;Uid=thilowiltsadmin;Pwd=abc123abc123!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    global start_time, fraction_medium_a_list, fraction_medium_b_list

    # Get the event data
    event_data = azeventgrid.get_json()
    
    # Extract the attributes and append to the lists
    fraction_medium_a_list.append(event_data['fraction medium A'])
    fraction_medium_b_list.append(event_data['fraction medium B'])
    
    # Check if one minute has passed
    if datetime.now() - start_time >= timedelta(minutes=1):
        # Calculate the statistics
        avg_a = statistics.mean(fraction_medium_a_list)
        stddev_a = statistics.stdev(fraction_medium_a_list)
        median_a = statistics.median(fraction_medium_a_list)
        
        avg_b = statistics.mean(fraction_medium_b_list)
        stddev_b = statistics.stdev(fraction_medium_b_list)
        median_b = statistics.median(fraction_medium_b_list)
        
        num_events = len(fraction_medium_a_list)
        
        # Get the current timestamp in the correct format
        calculation_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Log the results
        logging.info(f'Calculation Timestamp: {calculation_timestamp}')
        logging.info(f'Number of Events: {num_events}')
        logging.info(f'Fraction Medium A - Average: {avg_a}, Standard Deviation: {stddev_a}, Median: {median_a}')
        logging.info(f'Fraction Medium B - Average: {avg_b}, Standard Deviation: {stddev_b}, Median: {median_b}')
        
        # Write the results to the database
        try:
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO SensorDataStatistics (CalculationTimestamp, AverageFractionA, StdDevFractionA, MedianFractionA, AverageFractionB, StdDevFractionB, MedianFractionB, ValueCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (calculation_timestamp, avg_a, stddev_a, median_a, avg_b, stddev_b, median_b, num_events))
                conn.commit()
                logging.info("Statistics written to the database successfully.")
        except Exception as e:
            logging.error(f"Error writing to the database: {e}")
        
        # Reset the lists and start time for the next interval
        fraction_medium_a_list = []
        fraction_medium_b_list = []
        start_time = datetime.now()