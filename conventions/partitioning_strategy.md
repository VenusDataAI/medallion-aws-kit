# Partitioning Strategy

---

## 1. Standard Partition Scheme

All data in the lakehouse uses the following Hive-style partition structure:

```
{source_system}/{entity}/{year}/{month}/{day}/
```

This scheme is optimized for:
- **Date-range queries** (most common in analytics workloads)
- **Source isolation** (easy to reprocess a single system)
- **Glue crawler discovery** (predictable, auto-detectable columns)

---

## 2. When to Partition by Date Only

Use `year/month/day` as the **only** partition when:
- A single source system feeds an entity (no ambiguity).
- You access data primarily by date (e.g. daily batch jobs).

```sql
-- Redshift Spectrum — partition pruning on date
SELECT order_id, total_amount
FROM bronze_spectrum.orders
WHERE year = '2024'
  AND month = '03'
  AND day BETWEEN '01' AND '07';
-- Scans only 7 partitions instead of the entire dataset.
```

---

## 3. When to Partition by Source System + Date

Use `{source_system}/{entity}/{year}/{month}/{day}` when:
- Multiple source systems feed the same entity (e.g. two order systems).
- You need to reprocess or quarantine one system independently.
- SLAs differ between systems.

```sql
-- Filter by source_system AND date — two-level pruning
SELECT order_id, total_amount
FROM bronze_spectrum.orders
WHERE source_system = 'orders_system'
  AND year = '2024'
  AND month = '03';
-- Scans only the March 2024 partitions of orders_system.
```

---

## 4. Partition Column Types in Redshift Spectrum

Declare partition columns as `VARCHAR` in the external schema DDL:

```sql
CREATE EXTERNAL TABLE bronze_spectrum.orders (
    order_id     VARCHAR(64),
    customer_id  VARCHAR(64),
    order_date   DATE,
    total_amount NUMERIC(18, 4),
    is_paid      BOOLEAN
)
PARTITIONED BY (
    source_system VARCHAR(64),
    year          VARCHAR(4),
    month         VARCHAR(2),
    day           VARCHAR(2)
)
STORED AS PARQUET
LOCATION 's3://myco-lakehouse-bronze/orders_system/orders/'
TABLE PROPERTIES ('parquet.compress'='SNAPPY');
```

> **Why VARCHAR for year/month/day?** Glue infers partition values from the
> S3 path as strings. Using VARCHAR avoids type cast errors on discovery.

---

## 5. Partition Pruning Examples

### Filter on single day

```sql
SELECT COUNT(*) FROM bronze_spectrum.orders
WHERE year = '2024' AND month = '03' AND day = '15';
```

Spectrum scans: `s3://…/orders_system/orders/2024/03/15/` only.

### Filter on month range (cross-month)

```sql
SELECT * FROM silver_spectrum.orders
WHERE (year = '2024' AND month = '03')
   OR (year = '2024' AND month = '04');
```

> Avoid range predicates on `year` alone (e.g. `year >= '2023'`) — Spectrum
> cannot prune partitions on open-ended range comparisons over VARCHAR.
> Use `year = '2023' OR year = '2024'` instead.

### Incremental dbt gold model

```sql
-- In gold_orders_daily.sql incremental block:
WHERE _updated_at > (SELECT MAX(_refreshed_at) FROM {{ this }})
```

This relies on silver's `_updated_at` timestamp, not partition columns —
partition pruning is handled by the upstream silver query.

---

## 6. Layer-specific Guidance

| Layer | Partition columns | Retention | Storage class |
|-------|------------------|-----------|---------------|
| Bronze | source_system / entity / year / month / day | 90 days active, then IA | STANDARD → STANDARD_IA after 30d |
| Silver | source_system / entity / year / month / day | 1 year | STANDARD → STANDARD_IA after 60d |
| Gold | year / month / day (no source_system) | Indefinite | STANDARD |

---

## 7. Anti-patterns to Avoid

| Anti-pattern | Problem | Fix |
|-------------|---------|-----|
| Partitioning by high-cardinality column (e.g. `order_id`) | Creates millions of tiny files | Partition by date only |
| Too-fine granularity (hour/minute) | File proliferation, Glue crawler slowdown | Use daily partitions; use file name for sub-day ordering |
| Mixing raw JSON with Parquet in the same partition | Glue cannot infer schema cleanly | Keep formats consistent per partition path |
| Skipping source_system in the path when multi-source | Cannot isolate reprocessing | Always include source_system in the path |
