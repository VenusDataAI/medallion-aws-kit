# Prod environment variables
# NEVER commit secrets — use TF_VAR_* env vars or a secrets manager.

prefix     = "myco-lakehouse"
aws_region = "us-east-1"

# Provide via TF_VAR_redshift_admin_password
# redshift_admin_password = "CHANGEME"

# Replace with actual production subnet/SG IDs
subnet_ids         = ["subnet-xxxxxxxxxxxxxxxxx", "subnet-yyyyyyyyyyyyyyyyy"]
security_group_ids = ["sg-xxxxxxxxxxxxxxxxx"]
