locals {
  description = "Data lake bucket name prefix"
  data_lake_bucket = "dl"
}

variable "project" {
  description = "Project name"
  default = "eu-pub-tender"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "data_all"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "name" {
  description = "VM Machine name"
  type = string
  default = "gpc-instance"
}

variable "machine_type" {
  description = "VM Machine type"
  type = string
  default = "e2-standard-4"
}

variable "zone" {
  description = "VM Machine zone"
  type = string
  default = "europe-west6-a"
}

variable "size" {
  description = "VM Machine drive size"
  type = string
  default = "100"
}

variable "image" {
  description = "VM Machine OS"
  type = string
  default = "ubuntu-os-cloud/ubuntu-1804-lts"
}