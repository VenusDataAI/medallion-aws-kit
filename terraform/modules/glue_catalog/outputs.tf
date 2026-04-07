output "database_names" {
  description = "Map of layer name → Glue database name"
  value       = { for k, v in aws_glue_catalog_database.layer : k => v.name }
}

output "crawler_names" {
  description = "Map of layer name → Glue crawler name"
  value       = { for k, v in aws_glue_crawler.layer : k => v.name }
}

output "bronze_database_name" {
  description = "Name of the bronze Glue database"
  value       = aws_glue_catalog_database.layer["bronze"].name
}

output "silver_database_name" {
  description = "Name of the silver Glue database"
  value       = aws_glue_catalog_database.layer["silver"].name
}

output "gold_database_name" {
  description = "Name of the gold Glue database"
  value       = aws_glue_catalog_database.layer["gold"].name
}
