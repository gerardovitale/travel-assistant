output "service_account_github_actions_email" {
  description = "Service Account used by GitHub Actions"
  value       = google_service_account.ci_cd_service_account.email
}

output "ci_cd_service_account_key" {
  description = "Service Account Key"
  value = google_service_account_key.ci_cd_service_account_key.private_key
  sensitive = true
}

