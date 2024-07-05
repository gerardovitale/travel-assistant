resource "google_project_service" "iam" {
  project = var.PROJECT
  service = "iam.googleapis.com"
}

resource "google_service_account" "cicd_service_account" {
  depends_on = [google_project_service.iam]
  account_id   = "${var.PREFIX}-cicd"
  display_name = "CI/CD Service Account"
}

resource "google_project_iam_custom_role" "cicd_custom_role" {
  depends_on = [google_project_service.iam]
  role_id     = "cicdCustomRole"
  title       = "CICD Custom Role"
  description = "Minimum permission for CICD"
  permissions = [
    "storage.buckets.create",
    "storage.buckets.get",
    "iam.serviceAccounts.create",
    "iam.serviceAccounts.actAs",
    "run.services.create",
    "run.services.get",
    "run.services.delete",
  ]
  project = var.PROJECT
}

resource "google_project_iam_member" "cicd_service_account_roles" {
  depends_on = [google_project_service.iam]
  for_each = toset([
    "roles/resourcemanager.projectIamAdmin",
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.workloadIdentityUser",
    "roles/storage.objectViewer",
    "roles/storage.objectAdmin",
    "roles/resourcemanager.tagAdmin",
    google_project_iam_custom_role.cicd_custom_role.name,
  ])

  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.cicd_service_account.email}"
  role    = each.value
}

resource "google_iam_workload_identity_pool" "github" {
  provider                  = google-beta
  project                   = var.PROJECT
  workload_identity_pool_id = "github-actions"
  display_name              = "github-actions"
  description               = "for GitHub Actions"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  provider                           = google-beta
  project                            = var.PROJECT
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "github actions provider"
  description                        = "OIDC identity pool provider for execute GitHub Actions"
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.owner"      = "assertion.repository_owner"
    "attribute.refs"       = "assertion.ref"
  }

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "github_actions" {
  depends_on = [google_project_service.iam]
  service_account_id = google_service_account.cicd_service_account.id
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.REPO_NAME}"
}
