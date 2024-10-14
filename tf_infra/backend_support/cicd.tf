resource "google_project_service" "iam" {
  project = var.PROJECT
  service = "iam.googleapis.com"
}

resource "google_service_account" "cicd_service_account" {
  depends_on   = [google_project_service.iam]
  account_id   = "${var.PREFIX}-cicd"
  display_name = "CI/CD Service Account"
}

resource "google_project_iam_member" "cicd_service_account_roles" {
  depends_on = [google_project_service.iam]
  for_each   = toset([
    "roles/resourcemanager.projectIamAdmin",
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.workloadIdentityUser",
    "roles/storage.admin",
    "roles/resourcemanager.tagAdmin",
  ])

  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.cicd_service_account.email}"
  role    = each.value
}
