{#
  Generic test: not_null_proportion
  ==================================
  Fails if the proportion of non-null values in `column` falls below
  `min_proportion` (default: 0.95 → 95%).

  Configuration in schema.yml:
    columns:
      - name: customer_id
        tests:
          - not_null_proportion:
              min_proportion: 0.95

  Returns the count of failures (rows where the proportion is too low).
  A non-zero return means the test fails.
#}

{% test not_null_proportion(model, column_name, min_proportion=0.95) %}

with stats as (
    select
        count(*)                                             as total_rows,
        count({{ column_name }})                             as non_null_rows,
        count({{ column_name }}) * 1.0 / nullif(count(*), 0) as actual_proportion
    from {{ model }}
)

select
    total_rows,
    non_null_rows,
    actual_proportion,
    {{ min_proportion }}                                     as required_proportion
from stats
where actual_proportion < {{ min_proportion }}
   or actual_proportion is null

{% endtest %}
