# Azure Function for Event Grid Trigger and SQL Database Integration

This repository contains an Azure Function that processes events from an Event Grid, calculates statistics, and stores the results in a SQL database. The function collects events for one minute, calculates statistics, and then writes the results to the database.

# Prerequisites

Python 3.8 or higher
Azure Functions Core Tools
SQL Server or Azure SQL Database
# Setup

## Clone the repository:

git clone <repository-url>
cd <repository-directory>
Install dependencies:

pip install -r requirements.txt
## Configure SQL Database connection:

Update the connection_string variable in the code with your SQL Server connection string.
Function Details

# Event Grid Trigger

The function is triggered by Event Grid events. It collects events for one minute, calculates statistics, and stores the results in a SQL database.

# Code Overview

## SQL Database Initialization:

The SQL database connection is initialized with the provided connection string. If the connection fails, appropriate error messages are logged.

connection_string = "<your-sql-connection-string>"
## Event Processing:

The function processes events from Event Grid, extracts attributes, and appends them to lists. After one minute, it calculates statistics and writes the results to the SQL database.

@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    event_data = azeventgrid.get_json()
    fraction_medium_a_list.append(event_data['fraction medium A'])
    fraction_medium_b_list.append(event_data['fraction medium B'])
    
    if datetime.now() - start_time >= timedelta(minutes=1):
        avg_a = statistics.mean(fraction_medium_a_list)
        stddev_a = statistics.stdev(fraction_medium_a_list)
        median_a = statistics.median(fraction_medium_a_list)
        
        avg_b = statistics.mean(fraction_medium_b_list)
        stddev_b = statistics.stdev(fraction_medium_b_list)
        median_b = statistics.median(fraction_medium_b_list)
        
        num_events = len(fraction_medium_a_list)
        
        calculation_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO SensorDataStatistics (CalculationTimestamp, AverageFractionA, StdDevFractionA, MedianFractionA, AverageFractionB, StdDevFractionB, MedianFractionB, ValueCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (calculation_timestamp, avg_a, stddev_a, median_a, avg_b, stddev_b, median_b, num_events))
                conn.commit()
        except Exception as e:
            logging.error(f"Error writing to the database: {e}")
        
        fraction_medium_a_list.clear()
        fraction_medium_b_list.clear()
        start_time = datetime.now()
## Logging

The function logs important steps and errors to help with debugging and monitoring.

## Error Handling

If the SQL database connection fails, the function logs the error and skips writing the results to the database.

## License

This project is licensed under the MIT License.
