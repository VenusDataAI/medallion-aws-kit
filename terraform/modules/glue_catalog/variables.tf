variable "prefix" {
  description = "Project prefix used to name Glue databases and crawlers"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev | prod)"
  type        = string
}

variable "bucket_names" {
  description = "Map of layer name → bucket name (output from s3_lakehouse module)"
  type        = map(string)
}

variable "glue_role_arn" {
  description = "ARN of the Glue IAM role used by crawlers"
  type        = string
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default     = {}
}
