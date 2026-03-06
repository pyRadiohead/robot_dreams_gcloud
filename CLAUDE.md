# Claude Instructions

## Response Formatting

- Use tables to compare options, summarize bugs, or show before/after
- Use emoji icons to categorize items: 🐛 bug, ⚠️ missing/risk, 🗑️ redundant, 📛 naming, 🔐 auth/security, 📦 infra, 🔧 fix
- Use code blocks with language tags for all code and shell commands
- Use bold for key terms and section highlights
- Prefer structured lists over long prose paragraphs

## Project Context

- GCP project: `robot-dream-course`, region: `us-central1`
- Stack: Airflow (Docker Compose) → Dataproc Serverless (PySpark) → GCS (raw/bronze/silver) → BigQuery (gold)
- Spark jobs live in `spark_jobs/`, DAGs in `airflow/dags/`, infra in `terraform/`
- Service account: `dataproc-spark-sa@robot-dream-course.iam.gserviceaccount.com`
- Scripts bucket: `gs://robot-dream-course-scripts/jobs/`
- Data lake bucket: `gs://robot-dream-course-data-lake/`
