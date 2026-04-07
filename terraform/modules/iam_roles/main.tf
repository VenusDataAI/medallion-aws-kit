# ── Trust policies ────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "pipeline_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com", "ecs-tasks.amazonaws.com"]
    }
  }
}

# ── lakehouse_glue_role ───────────────────────────────────────────────────────

resource "aws_iam_role" "glue" {
  name               = "${var.prefix}-glue-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    sid    = "GlueS3ReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = flatten([
      for arn in values(var.bucket_arns) : [arn, "${arn}/*"]
    ])
  }
}

resource "aws_iam_policy" "glue_s3" {
  name   = "${var.prefix}-glue-s3-policy-${var.environment}"
  policy = data.aws_iam_policy_document.glue_s3.json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue_s3.arn
}

# ── lakehouse_redshift_role ───────────────────────────────────────────────────

resource "aws_iam_role" "redshift" {
  name               = "${var.prefix}-redshift-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "redshift_permissions" {
  statement {
    sid    = "RedshiftS3Read"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = flatten([
      for layer, arn in var.bucket_arns :
      [arn, "${arn}/*"]
      if contains(["bronze", "silver", "gold"], layer)
    ])
  }

  statement {
    sid    = "RedshiftGlueRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "redshift_permissions" {
  name   = "${var.prefix}-redshift-policy-${var.environment}"
  policy = data.aws_iam_policy_document.redshift_permissions.json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "redshift_permissions" {
  role       = aws_iam_role.redshift.name
  policy_arn = aws_iam_policy.redshift_permissions.arn
}

# ── lakehouse_pipeline_role ───────────────────────────────────────────────────

resource "aws_iam_role" "pipeline" {
  name               = "${var.prefix}-pipeline-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "pipeline_s3" {
  statement {
    sid    = "PipelineBronzeReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      var.bucket_arns["bronze"],
      "${var.bucket_arns["bronze"]}/*",
    ]
  }

  statement {
    sid    = "PipelineGlueCrawlerTrigger"
    effect = "Allow"
    actions = [
      "glue:StartCrawler",
      "glue:GetCrawler",
      "glue:GetCrawlerMetrics",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "pipeline_s3" {
  name   = "${var.prefix}-pipeline-policy-${var.environment}"
  policy = data.aws_iam_policy_document.pipeline_s3.json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "pipeline_s3" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.pipeline_s3.arn
}
