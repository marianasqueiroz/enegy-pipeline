terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Bucket no GCS (Data Lake — onde o CSV bruto vai ficar)
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 30 }
    action    { type = "Delete" }
  }
}

# 2. Dataset no BigQuery (Data Warehouse)
resource "google_bigquery_dataset" "energy_dataset" {
  dataset_id = var.bq_dataset
  location   = var.region

  labels = {
    project = "energy-pipeline"
  }
}

# 3. Service Account (identidade para os scripts)
resource "google_service_account" "pipeline_sa" {
  account_id   = "energy-pipeline-sa"
  display_name = "Energy Pipeline Service Account"
}

# Permissões da Service Account no bucket
resource "google_storage_bucket_iam_member" "sa_storage" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Permissões da Service Account no BigQuery
resource "google_project_iam_member" "sa_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}