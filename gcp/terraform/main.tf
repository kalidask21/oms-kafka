terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Uncomment and set bucket to store state in GCS:
  # backend "gcs" {
  #   bucket = "YOUR_TF_STATE_BUCKET"
  #   prefix = "oms-kafka/state"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "this" {}

# ── Enable required APIs ───────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# ── Artifact Registry ─────────────────────────────────────────────
resource "google_artifact_registry_repository" "oms" {
  depends_on    = [google_project_service.apis]
  repository_id = var.repo_name
  format        = "DOCKER"
  location      = var.region
  description   = "OMS Kafka application images"
}

# ── GKE Autopilot cluster ─────────────────────────────────────────
resource "google_container_cluster" "oms" {
  depends_on       = [google_project_service.apis]
  name             = var.cluster_name
  location         = var.region
  enable_autopilot = true
  network          = "default"
  subnetwork       = "default"

  release_channel {
    channel = "REGULAR"
  }
}

# ── IAM: Cloud Build → Artifact Registry (push images) ───────────
resource "google_project_iam_member" "cb_ar_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${data.google_project.this.number}@cloudbuild.gserviceaccount.com"
}

# ── IAM: Cloud Build → GKE (kubectl apply / set image) ───────────
resource "google_project_iam_member" "cb_gke_developer" {
  project = var.project_id
  role    = "roles/container.developer"
  member  = "serviceAccount:${data.google_project.this.number}@cloudbuild.gserviceaccount.com"
}
