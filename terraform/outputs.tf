output "bucket_name" {
  description = "Nome do bucket GCS criado"
  value       = google_storage_bucket.data_lake.name
}

output "bigquery_dataset" {
  description = "ID do dataset BigQuery criado"
  value       = google_bigquery_dataset.energy_dataset.dataset_id
}

output "service_account_email" {
  description = "Email da service account"
  value       = google_service_account.pipeline_sa.email
}