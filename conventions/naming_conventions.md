# Naming Conventions

> All names follow these rules unless an external system imposes its own
> constraints (e.g. AWS service character limits).

---

## 1. S3 Buckets

Pattern: `{prefix}-{layer}`

| Layer | Bucket name | Purpose |
|-------|------------|---------|
| Bronze | `myco-lakehouse-bronze` | Raw landing zone |
| Silver | `myco-lakehouse-silver` | Curated, deduplicated |
| Gold | `myco-lakehouse-gold` | Aggregated, BI-ready |
| Rejected | `myco-lakehouse-rejected` | Records failing validation |
| Scripts | `myco-lakehouse-scripts` | Glue scripts, Spark jars |

**Rules:**
- Lowercase, hyphens only (`-`). No underscores (S3 DNS compatibility).
- Prefix must be globally unique — use `{org}-{project}` form.
- Max 63 characters total.

---

## 2. S3 Prefix Structure (Partition Scheme)

```
s3://{bucket}/{source_system}/{entity}/{year}/{month}/{day}/{filename}
```

| Segment | Format | Example |
|---------|--------|---------|
| `source_system` | `snake_case` | `orders_system` |
| `entity` | `snake_case` | `orders` |
| `year` | `YYYY` (4 digits) | `2024` |
| `month` | `MM` (zero-padded) | `03` |
| `day` | `DD` (zero-padded) | `07` |
| `filename` | `{entity}_{YYYYMMDDTHHMMSS}.parquet` | `orders_20240307T143200.parquet` |

**Example:**
```
s3://myco-lakehouse-bronze/orders_system/orders/2024/03/07/orders_20240307T143200.parquet
```

---

## 3. Glue Databases

Pattern: `{prefix_underscored}_{layer}_db`

| Layer | Database name |
|-------|--------------|
| Bronze | `myco_lakehouse_bronze_db` |
| Silver | `myco_lakehouse_silver_db` |
| Gold | `myco_lakehouse_gold_db` |

**Rules:**
- Underscores only (hyphens are invalid in Glue database names).
- Hyphens in the prefix are replaced with underscores automatically by Terraform.

---

## 4. Glue Crawlers

Pattern: `{prefix}-{layer}-crawler-{environment}`

Examples:
- `myco-lakehouse-bronze-crawler-dev`
- `myco-lakehouse-silver-crawler-prod`

---

## 5. IAM Roles

Pattern: `{prefix}-{role_type}-role-{environment}`

| Role | Name example |
|------|-------------|
| Glue | `myco-lakehouse-glue-role-dev` |
| Redshift | `myco-lakehouse-redshift-role-dev` |
| Pipeline | `myco-lakehouse-pipeline-role-dev` |

---

## 6. Redshift Serverless

| Resource | Pattern | Example |
|----------|---------|---------|
| Namespace | `{prefix}-ns-{env}` | `myco-lakehouse-ns-dev` |
| Workgroup | `{prefix}-wg-{env}` | `myco-lakehouse-wg-dev` |
| External schema | `{layer}_spectrum` | `bronze_spectrum` |

---

## 7. dbt Models

| Layer | Pattern | Example |
|-------|---------|---------|
| Silver | `silver_{entity}` | `silver_orders` |
| Gold | `gold_{entity}_{grain}` | `gold_orders_daily` |

**Column naming:**
- `snake_case` everywhere — no abbreviations, no Hungarian notation.
- No layer prefix on columns (`order_id` not `slv_order_id`).
- Metadata columns: underscore-prefixed (`_ingested_at`, `_row_hash`).
- Gold models: business-friendly names, no metadata columns exposed.

---

## 8. Python Modules

| Module | File |
|--------|------|
| S3 upload | `ingestion/s3_loader.py` |
| Crawler trigger | `ingestion/glue_crawler_trigger.py` |

Class names: `PascalCase`.
Function/method names: `snake_case`.
Constants: `UPPER_SNAKE_CASE`.
