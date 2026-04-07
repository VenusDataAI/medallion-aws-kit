{#
  standardize_boolean(column)
  ============================
  Converts messy boolean-like string values to a proper SQL BOOLEAN.

  Handles:
    'true'  / 'True'  / 'TRUE'  → TRUE
    'false' / 'False' / 'FALSE' → FALSE
    '1'                          → TRUE
    '0'                          → FALSE
    NULL or any other value      → NULL

  Args:
    column : column name or SQL expression to convert.

  Usage example:
    {{ standardize_boolean('is_paid') }} as is_paid
#}

{% macro standardize_boolean(column) %}
    case
        when lower(cast({{ column }} as varchar)) in ('true', '1', 'yes', 'y')
            then true
        when lower(cast({{ column }} as varchar)) in ('false', '0', 'no', 'n')
            then false
        else null
    end
{% endmacro %}
