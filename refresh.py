import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
from datetime import datetime, timedelta, timezone
import time
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv()

# PostgreSQL client configuration
conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

API_KEY = os.getenv('API_KEY')
METABASE_URL = os.getenv('METABASE_URL')

logging.basicConfig(filename='refresh.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')


# Get the current timestamp
def get_current_timestamp():
    return datetime.now(timezone.utc).isoformat()

# Calc the next run timestamp
def calculate_next_run(interval_hours):
    now = datetime.now(timezone.utc)
    next_run = now + timedelta(hours=interval_hours)
    return next_run.isoformat()

# Refresh Model Call
def refresh_persisted_model(model_id):
    url = f"{METABASE_URL}/api/card/{model_id}/refresh"
    headers = {
        "x-api-key": API_KEY
    }
    
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        logging.info(f"API call successful for ID: {model_id} - {response.text}")
    except requests.RequestException as error:
        logging.error(f"API call failed for ID: {model_id} - {error}")

# Check state of the refresh
def check_refresh_state(model_id):
    url = f"{METABASE_URL}/api/persist/card/{model_id}"
    headers = {
        "x-api-key": API_KEY
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        logging.info(f"Current state for ID: {model_id} - {result}")
    except requests.RequestException as error:
        logging.error(f"Failed to get current state for ID: {model_id} - {error}")

# Main function to handle the scheduling
def handle_scheduling():
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor(cursor_factory=DictCursor)

        # Query to get the schedules that need an API call
        query = """
            SELECT s.id AS schedule_id, s.model_id, s.interval_hours
            FROM schedules s
            JOIN ids i ON s.model_id = i.model_id
            WHERE s.next_run <= %s;
        """
        cur.execute(query, (get_current_timestamp(),))
        rows = cur.fetchall()

        for row in rows:
            schedule_id, model_id, interval_hours = row['schedule_id'], row['model_id'], row['interval_hours']

            # Make the API call
            refresh_persisted_model(model_id)

            # Check the current state of the refresh
            check_refresh_state(model_id)

            # Update the last_run and next_run fields
            update_schedules = """
                UPDATE schedules
                SET last_run = %s, next_run = %s
                WHERE id = %s;
            """
            now = get_current_timestamp()
            next_run = calculate_next_run(interval_hours)
            cur.execute(update_schedules, (now, next_run, schedule_id))

        conn.commit()
        cur.close()
        conn.close()

    except Exception as error:
        logging.error('Error handling scheduling:', error)

# Run the scheduling function at a defined interval (e.g., every minute)
if __name__ == "__main__":
    while True:
        handle_scheduling()
        time.sleep(60)
