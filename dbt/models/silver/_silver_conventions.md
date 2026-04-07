# Silver Layer Conventions

> **Purpose**: The silver layer is the _curated_ layer of the lakehouse.
> Raw data from bronze is deduplicated, typed, validated, and enriched before
> being written here. Silver is the single source of truth for operational
> reporting and the input for gold aggregations.

---

## 1. Naming

| Object | Convention | Example |
|--------|-----------|---------|
| Model file | `silver_<entity>.sql` | `silver_orders.sql` |
| Schema | `silver` | `lakehouse.silver` |
| Column names | `snake_case` | `order_date`, `total_amount` |
| No table prefixes | ✗ | ~~`slv_orders`~~ |
| No layer suffixes | ✗ | ~~`orders_silver`~~ |

---

## 2. Grain

Every silver model **must** document its grain in the model header comment:

```sql
-- Grain: one row per (order_id, source_system) — latest record wins.
```

---

## 3. Deduplication

All silver models use the `{{ deduplicate() }}` macro:

```sql
{{ deduplicate(
    source('bronze_orders', 'orders'),
    ['order_id', 'source_system'],   -- composite natural key
    '_raw_timestamp DESC'            -- latest record wins
) }}
```

**Rules for choosing the dedup key:**

1. Prefer the business natural key — never a surrogate/auto-increment key.
2. Always include `source_system` if data arrives from multiple systems.
3. If no natural key exists, document it and use all columns as the key.
4. The `order_by` expression must be deterministic — tie-breaking by a
   stable field (timestamp, sequence ID) is required.

---

## 4. Data Types

| Source type | Silver type |
|------------|------------|
| Raw string dates | `DATE` or `TIMESTAMP` |
| Raw string numbers | `NUMERIC(p, s)` |
| Boolean-like strings | `BOOLEAN` via `{{ standardize_boolean() }}` |
| Large text / JSON blobs | Decompose into typed columns; **no raw JSON blobs** |
| Enumeration strings | `VARCHAR(n)` with an accepted_values test |

---

## 5. Column Standards

- All columns must be **`snake_case`**.
- No column prefixes (no `ord_`, `slv_`, etc.).
- Every column must have a `description` in `schema.yml`.
- **Raw provenance columns** (`_raw_file`, `_raw_timestamp`) may be kept in
  silver for lineage but must **not** be exposed in gold.

---

## 6. Mandatory Metadata Columns

Every silver model must end its SELECT with:

```sql
{{ add_metadata_columns(
    source_system_col='source_system',
    ingested_at_col='_raw_timestamp',
    business_cols=['col_a', 'col_b', ...]
) }}
```

This appends:

| Column | Type | Description |
|--------|------|-------------|
| `_ingested_at` | TIMESTAMP | When the record arrived in bronze |
| `_updated_at` | TIMESTAMP | When dbt last processed this row |
| `_source_system` | VARCHAR | Source system identifier |
| `_row_hash` | VARCHAR | MD5 of all business columns |

The `_row_hash` enables efficient CDC: compare hashes between runs instead
of diffing every column.

---

## 7. Tests

Every silver model must include at minimum:

```yaml
tests:
  - not_null       # on every NOT NULL business column
  - unique         # on the grain / natural key
  - not_null_proportion:
      min_proportion: 0.99   # for high-cardinality optional FK columns
```

---

## 8. Materialization

Silver models are **always materialized as `table`**. Never use `view` in silver
(views re-execute the dedup logic on every query — expensive and fragile).

---

## 9. What Does NOT Belong in Silver

| ❌ Do not include | ✅ Use gold instead |
|------------------|---------------------|
| Pre-aggregated metrics | `gold_orders_daily.sql` |
| Business logic calculations | Gold models |
| Dashboard-ready column names | Gold models |
| Exposed metadata (`_ingested_at`) | Gold models strip these |
| Raw JSON blobs | Decompose in silver |

---

## 10. Adding a New Entity

1. Add the source table to `models/bronze/sources.yml`.
2. Create `models/silver/silver_<entity>.sql` using the template:
   ```sql
   with deduped as (
       {{ deduplicate(source('...', '...'), ['<key>'], '<order_by>') }}
   ),
   typed as (
       select <typed columns> from deduped
   ),
   final as (
       select <business cols>,
              {{ add_metadata_columns(...) }}
       from typed
   )
   select * from final
   ```
3. Add `schema.yml` entry with column descriptions and tests.
4. Run `dbt run --select silver_<entity>` and `dbt test --select silver_<entity>`.
