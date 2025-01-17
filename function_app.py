import os
import logging
import azure.functions as func
from pymongo import MongoClient
from datetime import datetime, timedelta
import json

app = func.FunctionApp()

# Read MongoDB username and password from environment variables
# OPTIMIZATION 1
database_user = os.getenv('DATABASEUSER')
database_pw = os.getenv('DATABASEPW')

# Check if the required environment variables are set
if not database_user or not database_pw:
    logging.error("DATABASEUSER or DATABASEPW environment variables are not set.")
    raise EnvironmentError("DATABASEUSER and DATABASEPW must be set as environment variables.")

# Construct the MongoDB connection string using environment variables
connection_string = f"mongodb+srv://{database_user}:{database_pw}@dataengprojectbatchdb.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"

# Store events in a list
events = []

try:
    logging.info('Attempting to create MongoClient')
    # Initialize MongoDB client with the connection string
    client = MongoClient(connection_string)
    logging.info('MongoClient created successfully')
    
    # MongoDB database and container details
    database_name = 'dataengprojectbatchdb'
    container_name = 'events'
    database = client[database_name]
    container = database[container_name]
    logging.info('Successfully created database and container clients')
except Exception as e:
    logging.error(f"Failed to create MongoClient: {e}")
    logging.error(f"Exception type: {type(e).__name__}")
    logging.error(f"Exception details: {e.args}")
    # Set client, database, and container to None if MongoDB connection fails
    client = None
    database = None
    container = None

@app.event_grid_trigger(arg_name="azeventgrid")
def BatchGridTrigger(azeventgrid: func.EventGridEvent):
    # If MongoDB container is not initialized, skip event processing
    if container is None:
        logging.error('Container client is not available. Skipping event processing.')
        return

    logging.info('Python EventGrid trigger processed an event')
    
    # Append the event data to the events list
    events.append(azeventgrid.get_json())

    # Check if 10 events have been collected
    if len(events) >= 10:
        try:
            # Store events in MongoDB, each batch with a timestamp
            container.insert_one({
                'id': str(datetime.utcnow()),
                'events': events
            })
            logging.info('Stored events in MongoDB')
        except Exception as e:
            logging.error(f"Failed to store events in MongoDB: {e}")
        
        # Clear the events list after storing the batch
        logging.info('Clear events')
        events.clear()
