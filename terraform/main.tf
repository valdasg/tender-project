terraform {
  required_version = ">= 1.0"
  backend "local" {}  
  required_providers {
    google = {
      source    = "hashicorp/google"
    }
  }
}

provider "google" {
  project       = var.project
  region        = var.region  
  }

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}-${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age       = 30  // days
    }
  }

  force_destroy = true
}


# BigQuery dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id   = var.BQ_DATASET
  project      = var.project
  location     = var.region
}

# Virtual Machine
resource "google_compute_instance" "vm_instance" {
  name         = var.name
  machine_type = var.machine_type
  zone         = var.zone
  

  boot_disk {
    initialize_params {
      image    = var.image
      size     = var.size
    }
  }
  
  

  network_interface {
    # A default network is created for all GCP projects
    network = "default"
    access_config {
    }
  }
}
