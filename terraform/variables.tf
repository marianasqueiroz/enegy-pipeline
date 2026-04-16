variable "project_id" {
  description = "ID do seu projeto no GCP"
  type        = string
}

variable "region" {
  description = "Região dos recursos"
  type        = string
  default     = "us-central1"
}

variable "bq_dataset" {
  description = "Nome do dataset no BigQuery"
  type        = string
  default     = "energy_data"
}

variable "gcs_bucket" {
  description = "Nome único do bucket (deve ser único global)"
  type        = string
}