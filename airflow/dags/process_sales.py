"""
process_sales DAG
─────────────────
Orchestrates the bronze → silver transformation for sales data using
Dataproc Serverless (PySpark). The PySpark script path must be uploaded
to the scripts GCS bucket before running this DAG.

Steps:
  1. Upload the PySpark job to GCS (done once, outside this DAG)
  2. This DAG submits a Dataproc Serverless batch job
  3. Dataproc reads raw/sales → writes bronze/sales → writes silver/sales
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
)

# ── Config from environment variables (set in docker-compose.yml) ──────
PROJECT_ID     = os.environ.get("GCP_PROJECT_ID", "robot-dream-course")
REGION         = os.environ.get("GCP_REGION", "us-central1")
DATA_LAKE      = os.environ.get("DATA_LAKE_BUCKET", "robot-dream-course-data-lake")
SCRIPTS_BUCKET = os.environ.get("SCRIPTS_BUCKET", "robot-dream-course-scripts")

DATAPROC_SA = f"dataproc-spark-sa@{PROJECT_ID}.iam.gserviceaccount.com"

BATCH_ID_RAW_TO_BRONZE    = "process-sales-raw-bronze-{{ ds_nodash }}"
BATCH_ID_BRONZE_TO_SILVER = "process-sales-bronze-silver-{{ ds_nodash }}"
PYSPARK_RAW_TO_BRONZE_URI    = f"gs://{SCRIPTS_BUCKET}/jobs/process_sales_raw_to_bronze.py"
PYSPARK_BRONZE_TO_SILVER_URI = f"gs://{SCRIPTS_BUCKET}/jobs/process_sales_bronze_to_silver.py"

default_args = {
    "owner": "data-team",
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="process_sales",
    description="Bronze → Silver transformation for sales data via Dataproc Serverless",
    schedule_interval="@daily",
    start_date=datetime(2024, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["sales", "bronze", "silver"],
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
                    f"--raw_input=gs://{DATA_LAKE}/raw/sales/",
                    f"--bronze_output=gs://{DATA_LAKE}/bronze/sales/",
                ],
            },
            "runtime_config": {
                "version": "2.1",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": DATAPROC_SA,
                }
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
                    f"--bronze_input=gs://{DATA_LAKE}/bronze/sales/",
                    f"--silver_output=gs://{DATA_LAKE}/silver/sales/",
                ],
            },
            "runtime_config": {
                "version": "2.1",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": DATAPROC_SA,
                }
            },
        },
    )

    cleanup_raw_to_bronze = DataprocDeleteBatchOperator(
        task_id="cleanup_raw_to_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID_RAW_TO_BRONZE,
        trigger_rule="all_done",  # cleanup even on failure
    )

    cleanup_bronze_to_silver = DataprocDeleteBatchOperator(
        task_id="cleanup_bronze_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID_BRONZE_TO_SILVER,
        trigger_rule="all_done",  # cleanup even on failure
    )

    submit_raw_to_bronze >> submit_bronze_to_silver >> [cleanup_raw_to_bronze, cleanup_bronze_to_silver]
