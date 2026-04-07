locals {
  layers = ["bronze", "silver", "gold", "rejected", "scripts"]

  lifecycle_rules = {
    bronze   = 30
    silver   = 60
    gold     = null
    rejected = 30
    scripts  = null
  }
}

# ── Buckets ──────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "layer" {
  for_each = toset(local.layers)

  bucket        = "${var.prefix}-${each.key}"
  force_destroy = var.environment == "dev"

  tags = merge(var.tags, {
    Layer       = each.key
    Environment = var.environment
  })
}

# ── Versioning ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ── Server-side encryption ────────────────────────────────────────────────────

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# ── Block public access ───────────────────────────────────────────────────────

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each = aws_s3_bucket.layer

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Lifecycle rules ───────────────────────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each = {
    for layer, days in local.lifecycle_rules : layer => days
    if days != null
  }

  bucket = aws_s3_bucket.layer[each.key].id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    filter {}

    transition {
      days          = each.value
      storage_class = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  depends_on = [aws_s3_bucket_versioning.layer]
}

# ── Bucket policy: allow only the lakehouse role ──────────────────────────────

data "aws_iam_policy_document" "bucket_policy" {
  for_each = aws_s3_bucket.layer

  statement {
    sid    = "DenyNonLakehouseRole"
    effect = "Deny"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      each.value.arn,
      "${each.value.arn}/*",
    ]

    condition {
      test     = "ArnNotLike"
      variable = "aws:PrincipalArn"
      values   = [var.lakehouse_role_arn]
    }

    condition {
      test     = "BoolIfExists"
      variable = "aws:PrincipalIsAWSService"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id
  policy = data.aws_iam_policy_document.bucket_policy[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.layer]
}
