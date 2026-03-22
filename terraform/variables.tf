variable "GOOGLE_CLOUD_PROJECT_ID" {
  description = "Project"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "repository_id" {
  description = "repository_id"
  default     = "airflow-dbt"
}


variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "kl_bus_reliability_tracker_dataset_1"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "kl_bus_reliability_tracker_bucket_1"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "service_account_email" {
  description = "Email of the existing service account to attach to the VM"
  type        = string
}

variable "vm_ssh_user" {
  description = "Linux username for SSH access"
  type        = string
}

variable "vm_ssh_pub_key_path" {
  description = "Path to your SSH public key file"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "allowed_ssh_ip" {
  description = "Your local IP to restrict SSH access e.g. 123.456.789.0/32"
  type        = string
}