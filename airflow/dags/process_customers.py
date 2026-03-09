"""
process_customers DAG
─────────────────────
Orchestrates the raw → bronze → silver transformation for customers data
using Dataproc Serverless (PySpark). Runs on a daily schedule.

Delivery pattern: supplier sends a full table dump every day, so the job
reads all raw folders and deduplicates in the bronze → silver step.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
)

# ── Config ───────────────────────────────────────────────────────────────────
PROJECT_ID     = os.environ.get("GCP_PROJECT_ID", "robot-dream-course")
REGION         = os.environ.get("GCP_REGION", "us-central1")
DATA_LAKE      = os.environ.get("DATA_LAKE_BUCKET", "robot-dream-course-data-lake")
SCRIPTS_BUCKET = os.environ.get("SCRIPTS_BUCKET", "robot-dream-course-scripts")

DATAPROC_SA = f"dataproc-spark-sa@{PROJECT_ID}.iam.gserviceaccount.com"

BATCH_ID_RAW_TO_BRONZE    = "process-customers-raw-bronze-{{ ds_nodash }}"
BATCH_ID_BRONZE_TO_SILVER = "process-customers-bronze-silver-{{ ds_nodash }}"
PYSPARK_RAW_TO_BRONZE_URI    = f"gs://{SCRIPTS_BUCKET}/jobs/process_customers_raw_to_bronze.py"
PYSPARK_BRONZE_TO_SILVER_URI = f"gs://{SCRIPTS_BUCKET}/jobs/process_customers_bronze_to_silver.py"

default_args = {
    "owner": "data-team",
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="process_customers",
    description="Raw → Silver transformation for customers data via Dataproc Serverless",
    schedule_interval="@daily",
    start_date=datetime(2024, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["customers", "bronze", "silver"],
) as dag:

    # ── 1. Raw → Bronze ───────────────────────────────────────────────────────
    submit_raw_to_bronze = DataprocCreateBatchOperator(
        task_id="submit_raw_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID_RAW_TO_BRONZE,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_RAW_TO_BRONZE_URI,
                "args": [
                    f"--raw_input=gs://{DATA_LAKE}/raw/customers/",
                    f"--bronze_output=gs://{DATA_LAKE}/bronze/customers/",
                ],
            },
            "runtime_config": {"version": "2.1"},
            "environment_config": {
                "execution_config": {"service_account": DATAPROC_SA}
            },
        },
    )

    # ── 2. Bronze → Silver ────────────────────────────────────────────────────
    submit_bronze_to_silver = DataprocCreateBatchOperator(
        task_id="submit_bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID_BRONZE_TO_SILVER,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_BRONZE_TO_SILVER_URI,
                "args": [
                    f"--bronze_input=gs://{DATA_LAKE}/bronze/customers/",
                    f"--silver_output=gs://{DATA_LAKE}/silver/customers/",
                ],
            },
            "runtime_config": {"version": "2.1"},
            "environment_config": {
                "execution_config": {"service_account": DATAPROC_SA}
            },
        },
    )

    cleanup_raw_to_bronze = DataprocDeleteBatchOperator(
        task_id="cleanup_raw_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID_RAW_TO_BRONZE,
        trigger_rule="all_done",
    )

    cleanup_bronze_to_silver = DataprocDeleteBatchOperator(
        task_id="cleanup_bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID_BRONZE_TO_SILVER,
        trigger_rule="all_done",
    )

    submit_raw_to_bronze >> submit_bronze_to_silver >> [cleanup_raw_to_bronze, cleanup_bronze_to_silver]
