variable "prefix" {
  description = "Project prefix used to name the Redshift Serverless namespace and workgroup"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev | prod)"
  type        = string
}

variable "base_capacity_rpu" {
  description = "Base capacity in Redshift Processing Units (8 for dev, 32 for prod)"
  type        = number
  default     = 8

  validation {
    condition     = var.base_capacity_rpu >= 8 && var.base_capacity_rpu <= 512 && var.base_capacity_rpu % 8 == 0
    error_message = "base_capacity_rpu must be a multiple of 8 between 8 and 512."
  }
}

variable "admin_username" {
  description = "Admin username for the Redshift Serverless namespace"
  type        = string
  default     = "admin"
}

variable "admin_password" {
  description = "Admin password for the Redshift Serverless namespace"
  type        = string
  sensitive   = true
}

variable "db_name" {
  description = "Name of the default database"
  type        = string
  default     = "lakehouse"
}

variable "subnet_ids" {
  description = "VPC subnet IDs for the Redshift Serverless workgroup"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for the Redshift Serverless workgroup"
  type        = list(string)
}

variable "redshift_role_arn" {
  description = "ARN of the Redshift IAM role for Spectrum queries"
  type        = string
}

variable "glue_database_names" {
  description = "Map of layer → Glue database name (for external schema creation)"
  type        = map(string)
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default     = {}
}
