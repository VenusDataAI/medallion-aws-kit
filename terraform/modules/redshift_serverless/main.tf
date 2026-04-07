data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ── Namespace ─────────────────────────────────────────────────────────────────

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = "${var.prefix}-ns-${var.environment}"
  admin_username      = var.admin_username
  admin_user_password = var.admin_password
  db_name             = var.db_name

  iam_roles = [var.redshift_role_arn]

  tags = merge(var.tags, {
    Environment = var.environment
  })
}

# ── Workgroup ─────────────────────────────────────────────────────────────────

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = "${var.prefix}-wg-${var.environment}"

  base_capacity       = var.base_capacity_rpu
  publicly_accessible = false

  subnet_ids         = var.subnet_ids
  security_group_ids = var.security_group_ids

  tags = merge(var.tags, {
    Environment = var.environment
  })
}

# ── External schemas (Redshift Spectrum → Glue Catalog) ───────────────────────
# These are provisioned via a null_resource + AWS CLI because the Redshift
# provider does not expose a schema resource for Serverless.
# In a real deployment you'd run these DDLs via a post-apply step or
# a Redshift Data API call.

locals {
  external_schema_ddls = {
    for layer, db_name in var.glue_database_names :
    layer => <<-SQL
      CREATE EXTERNAL SCHEMA IF NOT EXISTS ${layer}_spectrum
      FROM DATA CATALOG
      DATABASE '${db_name}'
      IAM_ROLE '${var.redshift_role_arn}'
      CREATE EXTERNAL DATABASE IF NOT EXISTS;
    SQL
  }
}

# Output the DDLs so operators can apply them manually or via CI
resource "local_file" "spectrum_ddl" {
  for_each = local.external_schema_ddls

  filename        = "${path.module}/../../generated/${var.environment}_spectrum_${each.key}.sql"
  content         = each.value
  file_permission = "0644"
}
