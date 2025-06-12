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

variable "DOCKER_HUB_USERNAME" {
  type        = string
  description = "Docker Hub username"
}

variable "DOCKER_IMAGE_TAG" {
  type        = string
  description = "Docker image tag"
}
