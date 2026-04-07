{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'orders'],
        post_hook=[
            "GRANT SELECT ON {{ this }} TO GROUP analysts"
        ]
    )
}}

-- ────────────────────────────────────────────────────────────────────────────
-- silver_orders
-- Deduplicated, typed, and standardised orders from the bronze layer.
-- Grain: one row per (order_id, source_system) — latest record wins.
-- ────────────────────────────────────────────────────────────────────────────

with deduped as (

    {{ deduplicate(
        source('bronze_orders', 'orders'),
        ['order_id', 'source_system'],
        '_raw_timestamp DESC'
    ) }}

),

typed as (

    select
        -- ── Business columns ──────────────────────────────────────────────
        order_id::varchar(64)                                  as order_id,
        customer_id::varchar(64)                               as customer_id,
        order_date::date                                       as order_date,
        total_amount::numeric(18, 4)                           as total_amount,
        {{ standardize_boolean('is_paid') }}                   as is_paid,
        source_system::varchar(64)                             as source_system,

        -- ── Raw provenance (kept for lineage; dropped in gold) ────────────
        _raw_file,
        _raw_timestamp,

        -- ── Partition columns (passed through for Spectrum) ───────────────
        year,
        month,
        day

    from deduped

),

final as (

    select
        -- ── All typed business + provenance columns ───────────────────────
        order_id,
        customer_id,
        order_date,
        total_amount,
        is_paid,
        source_system,
        year,
        month,
        day,

        -- ── Mandatory silver metadata columns ─────────────────────────────
        {{ add_metadata_columns(
            source_system_col='source_system',
            ingested_at_col='_raw_timestamp',
            business_cols=['order_id', 'customer_id', 'order_date', 'total_amount', 'is_paid']
        ) }}

    from typed

)

select * from final
