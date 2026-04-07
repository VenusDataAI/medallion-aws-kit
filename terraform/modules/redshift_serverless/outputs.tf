output "namespace_arn" {
  description = "ARN of the Redshift Serverless namespace"
  value       = aws_redshiftserverless_namespace.this.arn
}

output "namespace_name" {
  description = "Name of the Redshift Serverless namespace"
  value       = aws_redshiftserverless_namespace.this.namespace_name
}

output "workgroup_arn" {
  description = "ARN of the Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.this.arn
}

output "workgroup_name" {
  description = "Name of the Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "endpoint" {
  description = "Redshift Serverless workgroup endpoint (host:port)"
  value       = aws_redshiftserverless_workgroup.this.endpoint
}

output "spectrum_ddl_paths" {
  description = "Paths to generated Spectrum external schema DDL files"
  value       = { for k, v in local_file.spectrum_ddl : k => v.filename }
}
