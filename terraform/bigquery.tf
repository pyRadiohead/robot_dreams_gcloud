# -------------------------------------------------------
# BigQuery Gold Dataset (Serving Layer)
# -------------------------------------------------------

resource "google_bigquery_dataset" "gold" {
  dataset_id    = "gold"
  friendly_name = "Gold Layer"
  description   = "Serving layer with enriched user profiles and final analytical tables"
  location      = var.location
}

# -------------------------------------------------------
# External Tables pointing to Silver GCS Parquet data
#
# IMPORTANT: autodetect = false + explicit schema is used
# so Terraform can create the table definitions even before
# the pipelines have run and silver/ files exist.
# -------------------------------------------------------

resource "google_bigquery_table" "silver_sales_external" {
  dataset_id  = google_bigquery_dataset.gold.dataset_id
  table_id    = "silver_sales"
  description = "External table over silver/sales Parquet partitioned by purchase_date"

  external_data_configuration {
    source_uris   = ["gs://${var.data_lake_bucket_name}/silver/sales/*"]
    source_format = "PARQUET"
    autodetect    = false
    # Note: hive_partitioning_options will be added back after silver/sales/ data exists.
    # BigQuery validates the source_uri_prefix on creation, which requires files to be present.
  }

  schema = jsonencode([
    { name = "client_id",      type = "STRING",  mode = "NULLABLE" },
    { name = "purchase_date",  type = "DATE",    mode = "NULLABLE" },
    { name = "product_name",   type = "STRING",  mode = "NULLABLE" },
    { name = "price",          type = "FLOAT64", mode = "NULLABLE" }
  ])

  deletion_protection = false
}

resource "google_bigquery_table" "silver_customers_external" {
  dataset_id  = google_bigquery_dataset.gold.dataset_id
  table_id    = "silver_customers"
  description = "External table over silver/customers Parquet"

  external_data_configuration {
    source_uris   = ["gs://${var.data_lake_bucket_name}/silver/customers/*"]
    source_format = "PARQUET"
    autodetect    = false
  }

  schema = jsonencode([
    { name = "client_id",          type = "STRING", mode = "NULLABLE" },
    { name = "first_name",         type = "STRING", mode = "NULLABLE" },
    { name = "last_name",          type = "STRING", mode = "NULLABLE" },
    { name = "email",              type = "STRING", mode = "NULLABLE" },
    { name = "registration_date",  type = "DATE",   mode = "NULLABLE" },
    { name = "state",              type = "STRING", mode = "NULLABLE" }
  ])

  deletion_protection = false
}

resource "google_bigquery_table" "silver_user_profiles_external" {
  dataset_id  = google_bigquery_dataset.gold.dataset_id
  table_id    = "silver_user_profiles"
  description = "External table over silver/user_profiles Parquet"

  external_data_configuration {
    source_uris   = ["gs://${var.data_lake_bucket_name}/silver/user_profiles/*"]
    source_format = "PARQUET"
    autodetect    = false
  }

  schema = jsonencode([
    { name = "email",        type = "STRING", mode = "NULLABLE" },
    { name = "full_name",    type = "STRING", mode = "NULLABLE" },
    { name = "state",        type = "STRING", mode = "NULLABLE" },
    { name = "birth_date",   type = "DATE",   mode = "NULLABLE" },
    { name = "phone_number", type = "STRING", mode = "NULLABLE" }
  ])

  deletion_protection = false
}

# -------------------------------------------------------
# Gold table: user_profiles_enriched
# Populated by the enrich_user_profiles Airflow DAG.
# -------------------------------------------------------

resource "google_bigquery_table" "user_profiles_enriched" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "user_profiles_enriched"
  description         = "Enriched customer table combining silver.customers and silver.user_profiles"
  deletion_protection = false

  schema = jsonencode([
    { name = "client_id",          type = "STRING",  mode = "NULLABLE" },
    { name = "first_name",         type = "STRING",  mode = "NULLABLE" },
    { name = "last_name",          type = "STRING",  mode = "NULLABLE" },
    { name = "email",              type = "STRING",  mode = "NULLABLE" },
    { name = "registration_date",  type = "DATE",    mode = "NULLABLE" },
    { name = "state",              type = "STRING",  mode = "NULLABLE" },
    { name = "full_name",          type = "STRING",  mode = "NULLABLE" },
    { name = "birth_date",         type = "DATE",    mode = "NULLABLE" },
    { name = "phone_number",       type = "STRING",  mode = "NULLABLE" }
  ])
}
