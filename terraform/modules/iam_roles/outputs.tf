output "glue_role_arn" {
  description = "ARN of the Glue IAM role"
  value       = aws_iam_role.glue.arn
}

output "glue_role_name" {
  description = "Name of the Glue IAM role"
  value       = aws_iam_role.glue.name
}

output "redshift_role_arn" {
  description = "ARN of the Redshift IAM role"
  value       = aws_iam_role.redshift.arn
}

output "redshift_role_name" {
  description = "Name of the Redshift IAM role"
  value       = aws_iam_role.redshift.name
}

output "pipeline_role_arn" {
  description = "ARN of the pipeline ingestion IAM role"
  value       = aws_iam_role.pipeline.arn
}

output "pipeline_role_name" {
  description = "Name of the pipeline ingestion IAM role"
  value       = aws_iam_role.pipeline.name
}
