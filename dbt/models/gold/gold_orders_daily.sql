{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key=['order_date', 'source_system'],
        incremental_strategy='delete+insert',
        tags=['gold', 'orders', 'daily'],
        post_hook=[
            "GRANT SELECT ON {{ this }} TO GROUP analysts",
            "GRANT SELECT ON {{ this }} TO GROUP bi_tools"
        ]
    )
}}

-- ────────────────────────────────────────────────────────────────────────────
-- gold_orders_daily
-- Daily aggregation of orders per source system.
-- Grain: one row per (order_date, source_system).
-- ────────────────────────────────────────────────────────────────────────────

with silver as (

    select
        order_date,
        source_system,
        total_amount,
        is_paid,
        _updated_at
    from {{ ref('silver_orders') }}

    {% if is_incremental() %}
    -- Only reprocess dates updated since the last run
    where _updated_at > (
        select coalesce(max(_refreshed_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

),

aggregated as (

    select
        order_date,
        source_system,
        count(*)                                               as total_orders,
        sum(total_amount)                                      as total_revenue,
        avg(total_amount)                                      as avg_order_value,
        sum(case when is_paid then 1 else 0 end)               as paid_orders,
        sum(case when not is_paid or is_paid is null then 1 else 0 end)
                                                               as unpaid_orders,
        sum(case when is_paid then total_amount else 0 end)    as paid_revenue,
        current_timestamp                                      as _refreshed_at

    from silver
    group by order_date, source_system

)

select * from aggregated
