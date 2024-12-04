# Project Setup and Usage Guide

## Setup Instructions

### 1. Create and Activate a Virtual Environment

1. Open a terminal and navigate to your project folder:
   ```bash
   cd /path/to/your/project
   ```

2. Create a virtual environment:
   ```bash
   virtualenv venv
   ```

3. Activate the virtual environment:
   - On Linux/MacOS:
     ```bash
     source venv/bin/activate
     ```
   - On Windows:
     ```bash
     .\venv\Scripts\activate
     ```

### 2. Install Requirements

With the virtual environment activated, install the required dependencies:
```bash
pip install -r requirements.txt
```

### 3. Initialize and Start Airflow

1. Replace the value of input_db_url in etl_patient_journey_schedule.py as per your local setup

Note: Below steps can be skipped to avoid running pipeline via DAG. The script can be run directly by running:
 ```bash
 python etl_patient_journey_schedule.py
 ```

2. Initialize the Airflow database:
   ```bash
   airflow db init
   ```

3. Create an admin user for Airflow:
   ```bash
   airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com \
     --password yourpassword
   ```

4. Start the Airflow web server:
   ```bash
   airflow webserver --port 8080
   ```
   By default, the web server will be accessible at `http://localhost:8080`.

5. Start the Airflow scheduler in a separate terminal:
   ```bash
   airflow scheduler
   ```

### 4. Running Tests with Pytest


1. Run the following command to execute tests:
   ```bash
   pytest
   ```

2. To view more detailed output, use:
   ```bash
   pytest -v
   ```

3. If you want to run a specific test file or function, use:
   ```bash
   pytest path/to/test_file.py::test_function_name
   ```

## Additional Notes

- Always activate the virtual environment before running any of the above commands.
- To deactivate the virtual environment, use:
  ```bash
  deactivate
  ```
