variable "prefix" {
  description = "Project prefix used to name all S3 buckets (e.g. 'myco-lakehouse')"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev | prod)"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be 'dev' or 'prod'."
  }
}

variable "lakehouse_role_arn" {
  description = "ARN of the IAM role that is allowed to access the lakehouse buckets"
  type        = string
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default     = {}
}
