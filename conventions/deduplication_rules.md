# Deduplication Rules

---

## 1. Why Deduplicate in Silver?

Bronze is an **append-only** landing zone — retries, replays, and late-arriving
records all create duplicates. Silver must present a clean, deduplicated view
before any business logic or aggregation is applied.

---

## 2. Choosing the Deduplication Key

The dedup key is the **natural/business key** — the set of columns that
uniquely identify an entity in the real world.

| Entity | Dedup key | Rationale |
|--------|-----------|-----------|
| orders | `(order_id, source_system)` | Same order ID can appear in two systems |
| customers | `(customer_id, source_system)` | |
| products | `(sku, source_system)` | |
| events | `(event_id,)` | UUID from source — globally unique |

**Rules for picking the key:**

1. **Prefer the business key**, not a surrogate auto-increment ID.
2. **Always include `source_system`** when data flows from multiple systems to
   the same entity table — identical IDs across systems are common.
3. If the source has no natural key, document it and hash a stable set of
   business columns to create a synthetic key — never use row number alone.
4. **Test your key** with `SELECT <key_cols>, COUNT(*) … HAVING COUNT(*) > 1`
   against a sample of bronze data before writing the silver model.

---

## 3. Choosing the Order-by (Tie-breaking) Column

The `order_by` determines which duplicate survives.

| Scenario | Recommended `order_by` | Reason |
|----------|----------------------|--------|
| Source sends updated events | `updated_at DESC` | Latest update wins |
| Source sends only inserts (no updates) | `_raw_timestamp DESC` | Latest arrival wins |
| CDC stream with sequence numbers | `sequence_id DESC` | Source-defined ordering |
| Late-arriving data matters | `event_timestamp DESC` | Business time wins |

> **Warning:** Do not use `_raw_timestamp ASC` (oldest-wins) unless the source
> guarantees that earlier arrivals are always more accurate — this is rare.

---

## 4. Handling Late-arriving Data

Late-arriving records appear in today's bronze partition but carry an
`event_timestamp` from a previous day.

**Strategy in silver:**

- Silver models are **full refreshes** (materialized as `table`).
  Each run reprocesses the entire history, so late arrivals are automatically
  incorporated on the next silver run.

**Strategy in gold:**

- Gold uses **incremental** materialization with `delete+insert`.
  The incremental predicate filters on `silver._updated_at`, not on
  `order_date`. This ensures that gold rows are refreshed for any date whose
  silver data changed, even if the `order_date` is in the past.

```sql
-- In gold_orders_daily.sql:
WHERE _updated_at > (SELECT MAX(_refreshed_at) FROM {{ this }})
```

---

## 5. How the `deduplicate` Macro Works

```sql
{{ deduplicate(
    source('bronze_orders', 'orders'),   -- relation to deduplicate
    ['order_id', 'source_system'],        -- unique_key (list)
    '_raw_timestamp DESC'                 -- order_by expression
) }}
```

The macro generates:

```sql
with source_data as (
    select * from bronze_spectrum.orders
),

ranked as (
    select
        source_data.*,
        row_number() over (
            partition by order_id, source_system
            order by _raw_timestamp DESC
        ) as _dedup_rn
    from source_data
)

select
    order_id, customer_id, order_date, -- ... all original columns
from ranked
where _dedup_rn = 1
```

Key properties:
- **Deterministic** — tie-breaking is done by a stable column, not by
  arbitrary execution order.
- **Non-destructive** — bronze is never modified; dedup exists only in silver.
- **Auditable** — the `_row_hash` in silver lets you detect which rows changed
  between runs without re-reading every column.

---

## 6. Testing Deduplication

Every silver model's `schema.yml` must include:

```yaml
- name: order_id
  tests:
    - unique          # validates the dedup worked
    - not_null
```

For composite unique keys use the `dbt_utils.unique_combination_of_columns` test:

```yaml
tests:
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns: [order_id, source_system]
```

---

## 7. Incremental Deduplication (Advanced)

When the full-refresh silver pattern is too slow (very large tables), switch to
an incremental approach:

1. Partition filter: process only new bronze partitions since the last silver run.
2. Union with existing silver data: bring in the existing silver row for the
   same key.
3. Apply `ROW_NUMBER()` across the union to pick the winner.
4. `delete+insert` the affected rows in silver.

This pattern is more complex — only adopt it when the full-refresh runtime
exceeds SLA. Document the change in the model header comment.
