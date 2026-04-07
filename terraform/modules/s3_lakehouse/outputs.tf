output "bucket_arns" {
  description = "Map of layer name → bucket ARN"
  value       = { for k, v in aws_s3_bucket.layer : k => v.arn }
}

output "bucket_names" {
  description = "Map of layer name → bucket name"
  value       = { for k, v in aws_s3_bucket.layer : k => v.id }
}

output "bronze_bucket_arn" {
  description = "ARN of the bronze bucket"
  value       = aws_s3_bucket.layer["bronze"].arn
}

output "silver_bucket_arn" {
  description = "ARN of the silver bucket"
  value       = aws_s3_bucket.layer["silver"].arn
}

output "gold_bucket_arn" {
  description = "ARN of the gold bucket"
  value       = aws_s3_bucket.layer["gold"].arn
}

output "rejected_bucket_arn" {
  description = "ARN of the rejected bucket"
  value       = aws_s3_bucket.layer["rejected"].arn
}

output "scripts_bucket_arn" {
  description = "ARN of the scripts bucket"
  value       = aws_s3_bucket.layer["scripts"].arn
}
