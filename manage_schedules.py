import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment vars from .env file
load_dotenv()

# DB connection details
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def insert_or_update_model_schedules(csv_file):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cursor = conn.cursor()

        # Load data from the CSV file into temp table
        cursor.execute("""
        CREATE TEMP TABLE tmp_schedules (
            model_id int4,
            interval_hours int4
        )
        """)

        with open(csv_file, 'r') as f:
            next(f)  # Skip the header row
            cursor.copy_expert("COPY tmp_schedules FROM STDIN WITH CSV", f)
            
        # Remove any records not in temp table
        cursor.execute("""
        DELETE FROM public.schedules
        WHERE model_id NOT IN (SELECT model_id FROM tmp_schedules)
        """)

        # Insert or update data in the 'schedules' table
        cursor.execute("""
        INSERT INTO public.schedules (model_id, interval_hours, last_run, next_run)
        SELECT model_id, interval_hours, NULL, NOW()
        FROM tmp_schedules
        ON CONFLICT (model_id)
        DO UPDATE SET
            interval_hours = EXCLUDED.interval_hours,
            next_run = CASE
                WHEN public.schedules.last_run IS NULL THEN NOW()
                ELSE public.schedules.last_run + INTERVAL '1 hour' * EXCLUDED.interval_hours
            END
        """)

        conn.commit()
        cursor.close()
        conn.close()

        print("Data imported or updated successfully.")
    except Exception as e:
        print(f"Failed to import or update data: {e}")

if __name__ == "__main__":
    csv_file = input("Enter the path to the CSV file: ")
    insert_or_update_model_schedules(csv_file)
