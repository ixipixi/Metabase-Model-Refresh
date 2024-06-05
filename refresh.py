import psycopg2
from psycopg2.extras import DictCursor
import requests
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

# Function to get the current timestamp
def get_current_timestamp():
    return datetime.now(timezone.utc).isoformat()

# Calculate next run time
def calculate_next_run(interval_hours):
    now = datetime.now(timezone.utc)
    next_run = now + timedelta(hours=interval_hours)
    return next_run.isoformat()

# Call model persistence endpoint
def refresh_model(model_id):
    url = f"{METABASE_URL}/api/card/{model_id}/refresh"
    headers = {
        "x-api-key": API_KEY,
    }
    
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        logging.info(f"API call successful for ID: {model_id} - {response.text}")
        return True
    except requests.RequestException as error:
        logging.error(f"API call failed for ID: {model_id} - {error}")
        return False

# Check refresh state in Metabase

def check_refresh_state(model_id):
    url = f"{METABASE_URL}/api/persist/card/{model_id}"
    headers = {
        "x-api-key": API_KEY
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()

        refresh_begin = result.get("refresh_begin")
        refresh_end = result.get("refresh_end")

        if refresh_begin or refresh_end:
            try:
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()

                update_query = """
                    UPDATE public.schedules
                    SET mb_refresh_begin = %s, mb_refresh_end = %s
                    WHERE model_id = %s;
                """
                cur.execute(update_query, (refresh_begin, refresh_end, model_id))

                conn.commit()
                cur.close()
                conn.close()

                logging.info(f"Updated refresh times for Model ID: {model_id}")
            except Exception as db_error:
                logging.error(f"Failed to update refresh times for ID: {model_id} - {db_error}")
        else:
            logging.warning(f"Missing refresh times in the response for Model ID: {model_id}")

    except requests.RequestException as error:
        logging.error(f"Failed to get current state for ID: {model_id} - {error}")
        
# Maintain scheduling
def handle_scheduling():
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor(cursor_factory=DictCursor)

        # Query to get the schedules that need an API call
        query = """
            SELECT model_id, interval_hours
            FROM schedules
            WHERE next_run <= %s;
        """
        cur.execute(query, (get_current_timestamp(),))
        rows = cur.fetchall()

        for row in rows:
            model_id, interval_hours = row['model_id'], row['interval_hours']
            logging.info(f"Processing Model ID: {model_id}, Interval: {interval_hours} hours")

            # Make the API call
            api_call_success = refresh_model(model_id)

            if api_call_success:
                # Check the current state of the refresh
                check_refresh_state(model_id)

                # Update the last_run and next_run fields
                update_query = """
                    UPDATE schedules
                    SET last_run = %s, next_run = %s
                    WHERE model_id = %s;
                """
                now = get_current_timestamp()
                next_run = calculate_next_run(interval_hours)
                cur.execute(update_query, (now, next_run, model_id))
                logging.info(f"Updated schedule for Model ID: {model_id} with last_run: {now} and next_run: {next_run}")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as error:
        logging.error(f'Error handling scheduling: {error}')
        
# Run the scheduling function at a defined interval (e.g., every minute)
if __name__ == "__main__":
    while True:
        handle_scheduling()
        time.sleep(60)
