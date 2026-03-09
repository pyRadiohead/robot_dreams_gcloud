"""
process_user_profiles DAG
──────────────────────────
Reads raw user_profiles JSONLine from GCS and writes directly to Silver.
No Bronze layer — supplier guarantees perfect data quality.

Triggered manually (no schedule). Run this once before enrich_customers.
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

BATCH_ID       = "process-user-profiles-{{ ds_nodash }}-{{ ts_nodash }}"
PYSPARK_URI    = f"gs://{SCRIPTS_BUCKET}/jobs/process_user_profiles_to_silver.py"

default_args = {
    "owner": "data-team",
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="process_user_profiles",
    description="Raw → Silver for user_profiles JSONLine via Dataproc Serverless (manual)",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["user_profiles", "silver", "manual"],
) as dag:

    submit_job = DataprocCreateBatchOperator(
        task_id="submit_user_profiles_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_URI,
                "args": [
                    f"--raw_input=gs://{DATA_LAKE}/raw/user_profiles/",
                    f"--silver_output=gs://{DATA_LAKE}/silver/user_profiles/",
                ],
            },
            "runtime_config": {"version": "2.1"},
            "environment_config": {
                "execution_config": {"service_account": DATAPROC_SA}
            },
        },
    )

    cleanup = DataprocDeleteBatchOperator(
        task_id="cleanup_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID,
        trigger_rule="all_done",
    )

    submit_job >> cleanup
