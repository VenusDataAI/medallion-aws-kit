# Remote state backend — update bucket/key/region before first run.
# Run `terraform init` after editing this file.

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

  # Uncomment and fill in to use S3 remote state:
  # backend "s3" {
  #   bucket         = "my-terraform-state-bucket"
  #   key            = "medallion-aws-kit/${terraform.workspace}/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-state-lock"
  # }
}
