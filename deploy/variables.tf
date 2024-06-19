variable "PREFIX" {
  type        = string
  description = "Prefix to set tags help identify resources in AWS"
}

variable "PROJECT" {
  type        = string
  description = "Project name"
}

variable "REGION" {
  type        = string
  description = "GCP region to deploy tf state related resources"
}

variable "REPO_NAME" {
  type        = string
  description = "Name of the target repository"
}
