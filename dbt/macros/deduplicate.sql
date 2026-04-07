{#
  deduplicate(relation, unique_key, order_by)
  ============================================
  Generates a ROW_NUMBER()-based deduplication CTE.

  Args:
    relation   : a dbt ref() or source() — the relation to deduplicate.
    unique_key  : list of column names that form the natural/business key.
                  Accepts a list: ['order_id', 'source_system']
    order_by    : SQL expression used to pick the "winning" row.
                  Typically a timestamp DESC, e.g. '_raw_timestamp DESC'

  Returns:
    A SELECT … FROM (subquery) WHERE rn = 1 expression that can be used
    directly as a model body or inside a CTE.

  Usage example:
    {{ deduplicate(
        source('bronze_orders', 'orders'),
        ['order_id', 'source_system'],
        '_raw_timestamp DESC'
    ) }}
#}

{% macro deduplicate(relation, unique_key, order_by) %}

{%- if unique_key is string %}
  {%- set unique_key = [unique_key] %}
{%- endif %}

with source_data as (
    select * from {{ relation }}
),

ranked as (
    select
        source_data.*,
        row_number() over (
            partition by {{ unique_key | join(', ') }}
            order by {{ order_by }}
        ) as _dedup_rn
    from source_data
)

select
    {%- for column in adapter.get_columns_in_relation(relation) %}
    {{ column.name }}{{ "," if not loop.last }}
    {%- endfor %}
from ranked
where _dedup_rn = 1

{% endmacro %}
