# -------------------------------------------------------
# Service Account for Dataproc Serverless (PySpark Jobs)
# -------------------------------------------------------

resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-spark-sa"
  display_name = "Dataproc Serverless Service Account"
  description  = "Used by Dataproc Serverless batch jobs to read/write GCS and BigQuery"
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Required for Dataproc Serverless: the submitter SA must be able to act as
# the execution SA, even when they are the same account.
resource "google_service_account_iam_member" "dataproc_sa_self_impersonation" {
  service_account_id = google_service_account.dataproc_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "dataproc_gcs_access" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "dataproc_bq_access" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "dataproc_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# -------------------------------------------------------
# Cloud Composer Service Account — DISABLED (cost saving)
# Uncomment this block only if you re-enable composer.tf
# -------------------------------------------------------

# resource "google_service_account" "composer_sa" {
#   account_id   = "composer-airflow-sa"
#   display_name = "Cloud Composer Service Account"
#   description  = "Used by Cloud Composer to orchestrate Dataproc and BigQuery jobs"
# }
#
# resource "google_project_iam_member" "composer_worker" {
#   project = var.project_id
#   role    = "roles/composer.worker"
#   member  = "serviceAccount:${google_service_account.composer_sa.email}"
# }
#
# resource "google_project_iam_member" "composer_gcs_access" {
#   project = var.project_id
#   role    = "roles/storage.objectAdmin"
#   member  = "serviceAccount:${google_service_account.composer_sa.email}"
# }
#
# resource "google_project_iam_member" "composer_dataproc_admin" {
#   project = var.project_id
#   role    = "roles/dataproc.admin"
#   member  = "serviceAccount:${google_service_account.composer_sa.email}"
# }
#
# resource "google_project_iam_member" "composer_bq_job_user" {
#   project = var.project_id
#   role    = "roles/bigquery.jobUser"
#   member  = "serviceAccount:${google_service_account.composer_sa.email}"
# }
#
# resource "google_project_iam_member" "composer_bq_data_editor" {
#   project = var.project_id
#   role    = "roles/bigquery.dataEditor"
#   member  = "serviceAccount:${google_service_account.composer_sa.email}"
# }
