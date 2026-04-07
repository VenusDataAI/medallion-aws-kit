terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.prefix
      Environment = "prod"
      ManagedBy   = "Terraform"
    }
  }
}

# ── Variables ─────────────────────────────────────────────────────────────────

variable "prefix" {
  type = string
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "redshift_admin_password" {
  type      = string
  sensitive = true
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

# ── Modules ───────────────────────────────────────────────────────────────────

module "s3_lakehouse" {
  source = "../../modules/s3_lakehouse"

  prefix             = var.prefix
  environment        = "prod"
  lakehouse_role_arn = module.iam_roles.glue_role_arn

  tags = {
    Project     = var.prefix
    Environment = "prod"
  }
}

module "iam_roles" {
  source = "../../modules/iam_roles"

  prefix      = var.prefix
  environment = "prod"
  bucket_arns = module.s3_lakehouse.bucket_arns

  tags = {
    Project     = var.prefix
    Environment = "prod"
  }
}

module "glue_catalog" {
  source = "../../modules/glue_catalog"

  prefix        = var.prefix
  environment   = "prod"
  bucket_names  = module.s3_lakehouse.bucket_names
  glue_role_arn = module.iam_roles.glue_role_arn

  tags = {
    Project     = var.prefix
    Environment = "prod"
  }
}

module "redshift_serverless" {
  source = "../../modules/redshift_serverless"

  prefix             = var.prefix
  environment        = "prod"
  base_capacity_rpu  = 32
  admin_password     = var.redshift_admin_password
  subnet_ids         = var.subnet_ids
  security_group_ids = var.security_group_ids
  redshift_role_arn  = module.iam_roles.redshift_role_arn
  glue_database_names = module.glue_catalog.database_names

  tags = {
    Project     = var.prefix
    Environment = "prod"
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "bucket_names" {
  value = module.s3_lakehouse.bucket_names
}

output "glue_databases" {
  value = module.glue_catalog.database_names
}

output "redshift_endpoint" {
  value = module.redshift_serverless.endpoint
}

output "pipeline_role_arn" {
  value = module.iam_roles.pipeline_role_arn
}
