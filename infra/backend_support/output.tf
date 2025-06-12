output "service_account_github_actions_email" {
  description = "Service Account used by GitHub Actions"
  value       = google_service_account.cicd_service_account.email
}
