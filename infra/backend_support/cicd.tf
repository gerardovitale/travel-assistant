# Enable APIs in the new project
resource "google_project_service" "required_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "monitoring.googleapis.com",
  ])
  project = var.PROJECT
  service = each.value
}

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
  for_each = toset([
    "roles/storage.admin",
    "roles/run.admin",
    "roles/workflows.editor",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.workloadIdentityUser",
    "roles/resourcemanager.projectIamAdmin",
  ])

  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.cicd_service_account.email}"
  role    = each.value
}

resource "google_project_iam_member" "cicd_service_account_act_as" {
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.cicd_service_account.email}"
  role    = "roles/iam.serviceAccountUser"
}
