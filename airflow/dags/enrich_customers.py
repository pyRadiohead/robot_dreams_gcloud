"""
enrich_customers DAG
─────────────────────
Joins silver/customers with silver/user_profiles and writes the enriched
result to BigQuery gold.user_profiles_enriched.

Triggered manually (no schedule). Must be run after both:
  - process_customers has completed successfully
  - process_user_profiles has completed successfully
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
BQ_DATASET     = os.environ.get("BIGQUERY_GOLD_DATASET", "gold")

DATAPROC_SA = f"dataproc-spark-sa@{PROJECT_ID}.iam.gserviceaccount.com"

BATCH_ID    = "enrich-customers-{{ ds_nodash }}-{{ ts_nodash }}"
PYSPARK_URI = f"gs://{SCRIPTS_BUCKET}/jobs/enrich_customers.py"

default_args = {
    "owner": "data-team",
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="enrich_customers",
    description="Join silver customers + user_profiles → BigQuery gold (manual)",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["customers", "user_profiles", "gold", "manual"],
) as dag:

    submit_job = DataprocCreateBatchOperator(
        task_id="submit_enrich_customers",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_URI,
                "args": [
                    f"--customers_input=gs://{DATA_LAKE}/silver/customers/",
                    f"--user_profiles_input=gs://{DATA_LAKE}/silver/user_profiles/",
                    f"--bq_project={PROJECT_ID}",
                    f"--bq_dataset={BQ_DATASET}",
                    "--bq_table=user_profiles_enriched",
                    f"--bq_temp_bucket={DATA_LAKE}",
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
