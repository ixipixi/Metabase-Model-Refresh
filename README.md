# Metabase Model Refresher

## Description

This is a Python script that refreshes persisted models in Metabase via API. The purpose is to illustrate the concept and provide a simple, modifiable solution that anyone can use as a starting point. This includes a simple method for importing and managing schedules with a flat file but you could also epxlore using actions in metabase to maintain the schedules in the table from a dashboard!

## Installation

1. **Set up a PostgreSQL instance**:
    - Ensure you have a running PostgreSQL instance.
    - Create a user with access to create and update tables.

2. **Install Python and required packages**:
    - Install Python if it's not already installed.
    - Install the required Python packages:
      ```bash
      pip install -r requirements.txt
      ```

3. **Update the .env file**:
    - Populate the `.env` file with your Metabase API token, Metabase location, and PostgreSQL connection details.

4. **Run the setup script**:
    - Create the scheduler table by running:
      ```bash
      python setup.py
      ```

## Schedule Management

1. **Refresh intervals**:
    - Models can be refreshed on 1-hour intervals.

2. **Import/update/delete intervals using a CSV file**:
    - A sample file called `schedules.csv` is included.
    - Add your models and intervals to the file.
    - Run `manage_schedules.py` to update the schedules loaded in the table:
      ```bash
      python manage_schedules.py
      ```
    - The script takes a delta of the file and table and updates models accordingly:
      - If you remove models from the file, they will be removed from the database.
      - If you change the interval for a model, the `next_run` will be recalculated based on the `last_run`.
      - If `next_run` is in the past, the model will refresh on the next script execution and recalulate the next_run.
	  
	  
## Run the Scheduler

Once initiated, the script runs every minute to check for models that need to be refreshed.

1. **Load schedules into the table**:
    - Ensure schedules are loaded into the table.

2. **Execute the scheduler**:
    - Run the scheduler script:
      ```bash
      python refresh.py
      ```

3. **Monitor execution**:
    - The script runs every minute to check for models that need to be refreshed.
    - Failures will be logged in `refresh.log`.
