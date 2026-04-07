output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "raw_dataset" {
  description = "BigQuery raw dataset"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "analytics_staging" {
  description = "BigQuery analytics staging dataset"
  value       = google_bigquery_dataset.analytics_staging.dataset_id
}

output "analytics_intermediate" {
  description = "BigQuery analytics intermediate dataset"
  value       = google_bigquery_dataset.analytics_intermediate.dataset_id
}

output "analytics_marts" {
  description = "BigQuery analytics marts dataset"
  value       = google_bigquery_dataset.analytics_marts.dataset_id
}