# Dev environment variables
# Copy to terraform.tfvars.local and override sensitive values there.

prefix     = "myco-lakehouse"
aws_region = "us-east-1"

# Provide via TF_VAR_redshift_admin_password env var or tfvars.local
# redshift_admin_password = "CHANGEME"

# Replace with actual subnet/SG IDs from your VPC
subnet_ids         = ["subnet-xxxxxxxxxxxxxxxxx", "subnet-yyyyyyyyyyyyyyyyy"]
security_group_ids = ["sg-xxxxxxxxxxxxxxxxx"]
