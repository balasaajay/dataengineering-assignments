locals {
  data_lake_bucket = "dtc_data_lake2"
}

variable "project" {
  type        = string
  default     = "de-zoomcamp-ga"
  description = "Data Enginering zoomcamp project"
}

variable "region" {
  type        = string
  default     = "us-west1"
  description = "Data Enginering zoomcamp project region"
}

variable "credentials" {
  type        = string
  default     = "/home/abalasa/de-zc/week1_hw/hw-partB/de-zoomcamp-ga-d10eae6462a3.json"
  description = "Data Enginering zoomcamp project GCP credentials"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all2"
}