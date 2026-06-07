variable "PROJECT" {
  type        = string
  description = "Project ID"
}

variable "APP_NAME" {
  type        = string
  description = "the GCP Project name"
}

variable "REGION" {
  type        = string
  description = "GCP region to deploy tf state related resources"
}
