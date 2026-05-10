# This is Apache Airflow. This is what replaces your Cron scheduler. Cron just says "Run this file at 6 AM." 
# Airflow says, "Run Task A at 6 AM. If Task A succeeds, run Task B. If Task B fails, email the team and retry it in 5 minutes."

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
import sys
import os

# Airflow needs to know where our custom Python files (transform.py, load.py) live.
# This code dynamically finds the 'src' folder on the server so we can import our functions.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from transform import reconcile_transactions
from load import load_to_database

# default_args defines the rules for this specific pipeline. 
# THIS is why Airflow is better than Cron.
default_args = {
    'owner': 'finance_data_enablement',
    'depends_on_past': False, # If yesterday's job failed, should today's job still run? False = yes, run it.
    'start_date': datetime(2026, 5, 1), # When the scheduler officially becomes active
    'email_on_failure': True, # CRITICAL FOR FINANCE: Automatically emails the team if the ETL breaks!
    'retries': 1,             # If the database is busy and fails, try again 1 more time...
    'retry_delay': timedelta(minutes=5), # ...but wait 5 minutes before trying. (Cron can't do this easily!)
}


# This is the wrapper function that glues our Extract, Transform, and Load steps together.
def run_reconciliation():
    # Define where the files live
    ledger = '/opt/airflow/data/internal_ledger.csv'
    clearing = '/opt/airflow/data/external_clearing.csv'
    
    # We use SQLite here because it's a lightweight database built into Python. 
    # In the RBC interview, you can say: "I used SQLite for the prototype, but this URL would 
    # just be swapped out for RBC's Oracle or Vertica database string."
    db_url = 'sqlite:////opt/airflow/data/regulatory_reporting.db'
    
    # Call our Pandas function
    reconciled_df = reconcile_transactions(ledger, clearing)
    
    # Call our SQLAlchemy function
    load_to_database(reconciled_df, db_url, 'daily_reconciliation_results')

# Here we define the DAG (Directed Acyclic Graph). This is the actual scheduled job.
# A collection of tasks with a defined order, that never loops back on itself.
with DAG('daily_regulatory_reconciliation',
         default_args=default_args,
         # schedule_interval uses the exact same syntax as Cron! '0 6 * * *' = 6:00 AM every day.
         # This makes you look great in the interview—you understand both Cron and Airflow scheduling.
         schedule_interval='0 6 * * *', #Run every day at 6:00 AM.minute,hour,day-of-month,month,day-of-week
         catchup=False) as dag:

    # The PythonOperator tells Airflow to execute a specific Python function (run_reconciliation).
    # If we had multiple steps, we would create multiple operators and chain them together.
    run_recon_task = PythonOperator(
        task_id='execute_reconciliation_and_load',
        python_callable=run_reconciliation
    )



    # =========================================================================================================
# wait_for_ledger ┐
#                  ├── extract → transform → load → validate → slack_alert
# wait_for_clearing┘
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# Make src/ importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from transform import reconcile_transactions
from load import load_to_database


@dag(
    dag_id='daily_regulatory_reconciliation_v2',
    schedule_interval='0 6 * * *',  # 6 AM daily
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args={
        'owner': 'finance_data_enablement',
        'email_on_failure': True,
        'email': ['risk-team@company.com'],
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def reconciliation_dag():

    # -----------------------------
    # 1. Sensors — wait for files
    # -----------------------------
    wait_for_ledger = FileSensor(
        task_id='wait_for_internal_ledger',
        filepath='/opt/airflow/data/internal_ledger.csv',
        poke_interval=30,
        timeout=60 * 60
    )

    wait_for_clearing = FileSensor(
        task_id='wait_for_external_clearing',
        filepath='/opt/airflow/data/external_clearing.csv',
        poke_interval=30,
        timeout=60 * 60
    )

    # -----------------------------
    # 2. Extract step
    # -----------------------------
    @task
    def extract():
        return {
            'ledger': '/opt/airflow/data/internal_ledger.csv',
            'clearing': '/opt/airflow/data/external_clearing.csv',
            'db_url': 'sqlite:////opt/airflow/data/regulatory_reporting.db'
        }

    # -----------------------------
    # 3. Transform step
    # -----------------------------
    @task
    def transform(paths):
        df = reconcile_transactions(paths['ledger'], paths['clearing'])
        return df.to_json()

    # -----------------------------
    # 4. Load step
    # -----------------------------
    @task
    def load(json_df, db_url):
        df = pd.read_json(json_df)
        load_to_database(df, db_url, 'daily_reconciliation_results')

    # -----------------------------
    # 5. Validation step
    # -----------------------------
    @task
    def validate(json_df):
        df = pd.read_json(json_df)
        breaks = df['is_break'].sum()
        if breaks > 0:
            raise ValueError(f"Validation failed — {breaks} breaks found.")
        return "Validation passed"

    # -----------------------------
    # 6. Slack alert on failure
    # -----------------------------
    slack_alert = SlackWebhookOperator(
        task_id='slack_failure_alert',
        http_conn_id='slack_connection',
        message='❌ DAG failed: {{ dag.dag_id }} — check Airflow logs.',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # -----------------------------
    # Task chaining
    # -----------------------------
    file_wait = [wait_for_ledger, wait_for_clearing]
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed, extracted['db_url'])
    validated = validate(transformed)

    # If any task fails → send Slack alert
    [loaded, validated] >> slack_alert


dag = reconciliation_dag()
