resource "google_service_account" "ci_cd_service_account" {
  account_id   = "${var.PREFIX}-cicd"
  display_name = "CI/CD Service Account"
}

resource "google_project_iam_member" "ci_cd_service_account_roles" {
  for_each = toset([
    "roles/storage.objectViewer",
    "roles/storage.objectAdmin",
  ])

  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.ci_cd_service_account.email}"
  role    = each.value
}

resource "google_service_account_key" "ci_cd_service_account_key" {
  service_account_id = google_service_account.ci_cd_service_account.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}
