{%- macro insert_metadata_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}
{{ log('Test ob Snap V0 Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.snap_v0_metadata (
        Snap_name,
        start_date,
        daily_snapshot_time,
        sdts_alias,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Snap_name,
        CAST('{{ start_date }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS start_date,
        CAST('{{ daily_snapshot_time }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS daily_snapshot_time,
        CAST('{{ sdts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS sdts_alias,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}