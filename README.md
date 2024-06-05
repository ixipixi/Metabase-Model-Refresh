## Description

This is a litte Python script that refreshes peristed Models in Metabase via API. The purpose is to illustrate the concept and provide a simple modifiable solution that anyone can use as a starting point.


## Installation

1. Set up a Postgres instance
2. Set up a user with access to create and update tables
3. Install Pyhton and the required Python packages

    pip install -r requirements.txt

4. Update the .env file with your Metabase API token, Metabase location & Postgres connection details
5. Run the setup.py script to create the scheduler table

python setup.py


## Schedule Management

1. Models can be refreshed on 1 hour intervals
2. A CSV file can be used to import / update / delete intervals for models(a sample file called "schedules.csv has been included)
3. Add your own models and intervals to the file and run "manage schedules.py" to change the schedules loaded in the table

	- This script takes a delta of the file and table and updates modles - so if you remove models from the file they will be removed from the database
	- If you change the interval for a model the next_run will be recalculated based on last run
	- If next_run is in the past the model will refresh on the next interval
	
## Run the Scheduler

Once initiated the script runs once a minute to check for models that need to be refreshed.

1. Once you've loaded schedules into the table you can execute the scheduler

python refresh.py

2. Once initiated the script runs once a minute to check for models that need to be refreshed.
3. Failures will be logged in "refresh.log"

