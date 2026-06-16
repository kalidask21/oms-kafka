variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "cluster_name" {
  description = "GKE Autopilot cluster name"
  type        = string
  default     = "oms-cluster"
}

variable "repo_name" {
  description = "Artifact Registry Docker repository name"
  type        = string
  default     = "oms-kafka"
}
