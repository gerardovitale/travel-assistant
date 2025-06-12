locals {
  tags = {
    "environment" = "production",
    "project"     = var.PROJECT,
    "manage_by"   = "cicd-terraform"
  }
}
