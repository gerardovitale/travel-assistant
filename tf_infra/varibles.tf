variable "PREFIX" {
  type        = string
  description = "Prefix to set tags help identify resources"
}

variable "PROJECT" {
  type        = string
  description = "Project ID"
}

variable "REGION" {
  type        = string
  description = "GCP region to deploy tf state related resources"
}
