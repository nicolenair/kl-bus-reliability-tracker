terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = var.GOOGLE_CLOUD_PROJECT_ID
  region  = var.region
}

# GCS Bucket
resource "google_storage_bucket" "kl_bus_reliability_tracker_bucket_1" {
  name     = var.gcs_bucket_name
  location = var.location

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "kl_bus_reliability_tracker_dataset_1" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# Artifact Registry
resource "google_artifact_registry_repository" "airflow-dbt" {
  location      = var.region
  repository_id = var.repository_id
  format        = "DOCKER"
  description   = "Docker images for Airflow and dbt"
}

# Static IP
resource "google_compute_address" "static_ip" {
  name   = "airflow-dbt-static-ip"
  region = var.region
}

# The VM
resource "google_compute_instance" "airflow_vm" {
  name         = "airflow-dbt-vm"
  machine_type = "e2-standard-2"
  zone         = "${var.region}-a"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 20
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.static_ip.address
    }
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y docker.io docker-compose
    usermod -aG docker $USER
    systemctl enable docker
    systemctl start docker
  EOF

  metadata = {
    ssh-keys = "${var.vm_ssh_user}:${file(var.vm_ssh_pub_key_path)}"
  }

  tags = ["airflow-server"]
}

# Firewall — SSH only, restricted to your IP
resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  target_tags   = ["airflow-server"]
  source_ranges = [var.allowed_ssh_ip]
}