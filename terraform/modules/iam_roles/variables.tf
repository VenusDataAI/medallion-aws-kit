variable "prefix" {
  description = "Project prefix used to name all IAM roles"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev | prod)"
  type        = string
}

variable "bucket_arns" {
  description = "Map of layer name → bucket ARN (output from s3_lakehouse module)"
  type        = map(string)
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default     = {}
}
