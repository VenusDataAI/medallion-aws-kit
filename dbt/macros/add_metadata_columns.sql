{#
  add_metadata_columns()
  =======================
  Appends standard metadata columns to any silver model.

  Columns added:
    _ingested_at    TIMESTAMP  — when the raw record arrived in bronze (from source)
    _updated_at     TIMESTAMP  — current_timestamp at model run time
    _source_system  VARCHAR    — originating system identifier
    _row_hash       VARCHAR    — MD5 of all business columns for change detection

  Args (called via caller context — used inside a SELECT):
    source_system_col : name of the column carrying the source system value
                        (default: 'source_system')
    ingested_at_col   : name of the column carrying the raw ingest timestamp
                        (default: '_raw_timestamp')
    business_cols     : list of column names to include in the row hash.
                        If omitted the macro uses all columns via '*' — less
                        deterministic; prefer passing an explicit list.

  Usage example (at the end of your SELECT list):
    {{ add_metadata_columns(
        source_system_col='source_system',
        ingested_at_col='_raw_timestamp',
        business_cols=['order_id','customer_id','order_date','total_amount','is_paid']
    ) }}
#}

{% macro add_metadata_columns(
    source_system_col='source_system',
    ingested_at_col='_raw_timestamp',
    business_cols=[]
) %}
    {{ ingested_at_col }}::timestamp                           as _ingested_at,
    current_timestamp                                          as _updated_at,
    {{ source_system_col }}                                    as _source_system,
    {%- if business_cols %}
    md5(
        {%- for col in business_cols %}
        coalesce(cast({{ col }} as varchar), '')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
    {%- else %}
    md5(cast(row_to_json(t.*) as varchar))
    {%- endif %}
                                                               as _row_hash
{% endmacro %}
