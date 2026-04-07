locals {
  layers = ["bronze", "silver", "gold"]

  # Bronze crawls every 30 min; silver/gold are on-demand (no schedule)
  crawler_schedules = {
    bronze = "cron(0/30 * * * ? *)"
    silver = null
    gold   = null
  }
}

# ── Glue Databases ────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "layer" {
  for_each = toset(local.layers)

  name        = "${replace(var.prefix, "-", "_")}_${each.key}_db"
  description = "Lakehouse ${each.key} layer — ${var.environment}"

  tags = merge(var.tags, {
    Layer       = each.key
    Environment = var.environment
  })
}

# ── Glue Crawlers ─────────────────────────────────────────────────────────────

resource "aws_glue_crawler" "layer" {
  for_each = toset(local.layers)

  name          = "${var.prefix}-${each.key}-crawler-${var.environment}"
  database_name = aws_glue_catalog_database.layer[each.key].name
  role          = var.glue_role_arn
  description   = "Crawls the ${each.key} S3 layer"

  dynamic "schedule" {
    for_each = local.crawler_schedules[each.key] != null ? [local.crawler_schedules[each.key]] : []
    content {
      schedule_expression = schedule.value
    }
  }

  s3_target {
    path = "s3://${var.bucket_names[each.key]}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  tags = merge(var.tags, {
    Layer       = each.key
    Environment = var.environment
  })
}
