import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection details
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def create_database():
    try:
        # Connect to the default PostgreSQL database to check if the new database exists
        conn = psycopg2.connect(dbname='postgres', user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if the database already exists
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [DB_NAME])
        exists = cursor.fetchone()
        
        if not exists:
            # Create the new database
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
            print(f"Database {DB_NAME} created successfully.")
        else:
            print(f"Database {DB_NAME} already exists.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to create database: {e}")

def create_tables():
    try:
        # Connect to the newly created database
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cursor = conn.cursor()
        
        
        # Create the 'schedules' table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.schedules (
        model_id int4 NOT NULL,
        interval_hours int4 NOT NULL,
        last_run timestamp NULL,
        next_run timestamp NOT NULL,
        mb_refresh_begin timestamp NULL,
        mb_refresh_end timestamp NULL,
        CONSTRAINT schedules_model_id_key UNIQUE (model_id)
        )
        """)

        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Tables created successfully.")
    except Exception as e:
        print(f"Failed to create tables: {e}")

if __name__ == "__main__":
    create_database()
    create_tables()
