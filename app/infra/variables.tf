variable "account_id" {
  type        = string
  description = "The AWS account ID"
}

variable "key_name" {
  description = "The SSH key pair to use for EC2 access"
  type        = string
}

variable "postgres_password" {
  description = "The password for the PostgreSQL 'postgres' user"
  type        = string
  sensitive   = true
}