variable "PREFIX" {
  type        = string
  description = "Prefix to set tags/labels help identify resources in the GCP porject"
}

variable "PROJECT" {
  type        = string
  description = "the GCP Project name"
}

variable "REGION" {
  type        = string
  description = "GCP region to deploy tf state related resources"
}

variable "REPO_NAME" {
  type        = string
  description = "Name of the target repository"
}
