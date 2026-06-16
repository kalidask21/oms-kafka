output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.oms.name
}

output "artifact_registry_url" {
  description = "Docker image base URL — append /IMAGE:TAG"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repo_name}"
}

output "get_credentials_cmd" {
  description = "Run this to configure kubectl"
  value       = "gcloud container clusters get-credentials ${var.cluster_name} --region ${var.region} --project ${var.project_id}"
}

output "docker_auth_cmd" {
  description = "Run this to authenticate Docker with Artifact Registry"
  value       = "gcloud auth configure-docker ${var.region}-docker.pkg.dev"
}
